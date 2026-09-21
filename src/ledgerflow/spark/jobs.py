"""The three Spark jobs: streaming, backfill, and training-set assembly.

    python -m ledgerflow.spark.jobs streaming --checkpoint ./_checkpoints
    python -m ledgerflow.spark.jobs backfill  --source ./data/bronze
    python -m ledgerflow.spark.jobs training  --labels ./data/labels
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import features, session

GOLD_ACCOUNT_FEATURES = "gold_account_features"


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------


def upsert_features(batch_df: DataFrame, batch_id: int, *, path: str) -> None:
    """Idempotent write.

    ``foreachBatch`` gives a deterministic ``batch_id`` on replay, and the MERGE
    is keyed on (account_id, window_end), so re-running a micro-batch is a
    no-op. Same replay-safety idea as the API's idempotency keys and the
    consumers' dedupe table, one layer up: every write path in this system
    tolerates being run twice.
    """
    if session.delta_available():
        from delta.tables import DeltaTable

        spark = batch_df.sparkSession
        if DeltaTable.isDeltaTable(spark, path):
            (
                DeltaTable.forPath(spark, path).alias("t")
                .merge(
                    batch_df.alias("s"),
                    "t.account_id = s.account_id AND t.window_end = s.window_end",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            batch_df.write.format("delta").mode("overwrite").save(path)
        return

    # Parquet has no MERGE. Partitioning by the window and overwriting only the
    # touched partitions gets the same idempotence for this key shape.
    (
        batch_df.write.format("parquet")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("window_end")
        .save(path)
    )


def run_streaming(
    *,
    brokers: str,
    topic: str,
    output_path: str,
    checkpoint: str,
    trigger_seconds: int = 30,
) -> None:
    spark = session.build("ledgerflow-features-streaming", streaming=True)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        # bound each micro-batch so one slow trigger cannot pull a day of
        # backlog into a single batch and blow up executor memory
        .option("maxOffsetsPerTrigger", 50_000)
        .load()
    )

    computed = features.account_features(features.parse_events(raw), watermark=True)

    query = (
        computed.writeStream
        .foreachBatch(lambda df, bid: upsert_features(df, bid, path=output_path))
        # the checkpoint holds Kafka offsets AND the aggregation state, so a
        # killed job resumes mid-window instead of recomputing. the _v1 suffix
        # is deliberate: a checkpoint is coupled to the query plan, and
        # versioning the path makes a breaking change an explicit decision
        # rather than a failed restart at 3am.
        .option("checkpointLocation", os.path.join(checkpoint, f"{GOLD_ACCOUNT_FEATURES}_v1"))
        .outputMode("update")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )
    query.awaitTermination()


# ---------------------------------------------------------------------------
# Backfill -- the same feature function, a different reader
# ---------------------------------------------------------------------------


def run_backfill(
    *, source_path: str, output_path: str, source_format: str | None = None
) -> DataFrame:
    spark = session.build("ledgerflow-features-backfill")
    fmt = source_format or session.table_format()
    reader = spark.read.format(fmt)
    if fmt == "json":
        # declared schema, never inferred: inference samples the file to guess
        # types, so a day where every merchant_id is null changes the column type
        reader = reader.schema(features.NORMALIZED_SCHEMA)
    bronze = reader.load(source_path)
    if "effective_at" not in bronze.columns:
        bronze = bronze.withColumn("effective_at", F.to_timestamp("occurred_at"))

    # identical call to the streaming path. if this line and the streaming one
    # ever diverge, offline and online features diverge with them.
    computed = features.account_features(bronze, watermark=False)

    computed.write.format(session.table_format()).mode("overwrite").save(output_path)
    return computed


# ---------------------------------------------------------------------------
# Training sets
# ---------------------------------------------------------------------------


def point_in_time_join(labels: DataFrame, feature_rows: DataFrame) -> DataFrame:
    """Attach, to each label, the newest feature window that closed BEFORE it.

    The ``<=`` predicate is the whole ballgame. Join without it and the model
    reads the future: it trains on aggregates that include the very transaction
    being labelled, scores beautifully in backtest, and fails in production.
    Spark has no native as-of join, so this is a windowed row pick.
    """
    joined = (
        labels.alias("l")
        .join(feature_rows.alias("f"), "account_id")
        .where(F.col("f.window_end") <= F.col("l.decision_at"))
    )

    newest = Window.partitionBy("l.label_id").orderBy(F.col("f.window_end").desc())

    return (
        joined.withColumn("rn", F.row_number().over(newest))
        .where(F.col("rn") == 1)
        .drop("rn")
    )


def run_training(*, labels_path: str, features_path: str, output_path: str) -> DataFrame:
    spark = session.build("ledgerflow-training-set")
    fmt = session.table_format()
    labels = spark.read.format(fmt).load(labels_path)
    feature_rows = spark.read.format(fmt).load(features_path)
    training = point_in_time_join(labels, feature_rows)
    training.write.format(fmt).mode("overwrite").save(output_path)
    return training


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="ledgerflow-spark")
    sub = parser.add_subparsers(dest="job", required=True)

    p = sub.add_parser("streaming")
    p.add_argument("--brokers",
                   default=os.environ.get("LEDGERFLOW_KAFKA_BROKERS", "localhost:9092"))
    p.add_argument("--topic", default="transactions.normalized.v1")
    p.add_argument("--output", default="./data/gold/account_features")
    p.add_argument("--checkpoint", default="./_checkpoints")
    p.add_argument("--trigger-seconds", type=int, default=30)

    p = sub.add_parser("backfill")
    p.add_argument("--source", default="./data/bronze/normalized_transactions")
    p.add_argument("--output", default="./data/gold/account_features")
    p.add_argument("--source-format", default=None)

    p = sub.add_parser("training")
    p.add_argument("--labels", default="./data/labels")
    p.add_argument("--features", default="./data/gold/account_features")
    p.add_argument("--output", default="./data/gold/training_set")

    args = parser.parse_args(argv)
    if args.job == "streaming":
        run_streaming(
            brokers=args.brokers, topic=args.topic, output_path=args.output,
            checkpoint=args.checkpoint, trigger_seconds=args.trigger_seconds,
        )
    elif args.job == "backfill":
        run_backfill(
            source_path=args.source, output_path=args.output,
            source_format=args.source_format,
        )
    else:
        run_training(
            labels_path=args.labels, features_path=args.features, output_path=args.output
        )


if __name__ == "__main__":
    main()
