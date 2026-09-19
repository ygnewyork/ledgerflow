"""The feature transformation.

The point of this module: ``account_features`` is a pure DataFrame -> DataFrame
function, called by BOTH the streaming job and the backfill job, unchanged.

Structured Streaming and batch share the DataFrame API, which is what makes
that possible, and it is the strongest argument for Spark in this system --
stronger than throughput, which a single node would handle at this volume.
Two implementations of the same window logic drift, and the drift shows up as a
model that backtests well and fails in production. One function cannot drift.

The window sizes and the watermark are imported from
``ledgerflow.features.windows``, the same module the online risk worker uses,
so there is exactly one definition of "1h spend" in the entire system.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..features import windows

#: Schema of `transactions.normalized.v1`. Declared, never inferred: schema
#: inference on a stream reads a sample to guess types, so a day when every
#: merchant_id happens to be null silently changes the column's type.
NORMALIZED_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("raw_transaction_id", T.StringType()),
    T.StructField("tenant_id", T.StringType()),
    T.StructField("account_id", T.StringType()),
    T.StructField("amount_minor", T.LongType()),
    T.StructField("currency", T.StringType()),
    T.StructField("occurred_at", T.StringType()),
    T.StructField("descriptor", T.StringType()),
    T.StructField("cleaned", T.StringType()),
    T.StructField("merchant_id", T.StringType()),
    T.StructField("merchant_name", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("confidence", T.DoubleType()),
    T.StructField("normalizer_version", T.IntegerType()),
])


def parse_events(raw: DataFrame) -> DataFrame:
    """Kafka value bytes -> typed columns with a real event-time column."""
    return (
        raw.select(F.from_json(F.col("value").cast("string"), NORMALIZED_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("effective_at", F.to_timestamp("occurred_at"))
        .drop("occurred_at")
    )


def account_features(txns: DataFrame, *, watermark: bool = True) -> DataFrame:
    """Windowed per-account features.

    ``watermark=False`` is for batch: a bounded historical DataFrame has no
    late data by definition, and ``withWatermark`` on a batch query is a no-op
    that only makes the plan harder to read.
    """
    duration, slide = windows.SPEND_1H.spark_window

    framed = txns
    if watermark:
        # bounds the aggregation state (closed windows are evicted, so memory
        # does not grow with account count) and defines "late". events older
        # than the watermark are dropped from the aggregate -- correct, and
        # routed to late_arrivals so the drop is observable.
        framed = framed.withWatermark("effective_at", _interval(windows.WATERMARK))

    return (
        framed
        .groupBy(
            F.col("account_id"),
            F.window(F.col("effective_at"), duration, slide),
        )
        .agg(
            F.sum("amount_minor").cast("long").alias("spend_minor"),
            F.count("*").cast("int").alias("txn_count"),
            F.max("amount_minor").cast("long").alias("max_amount_minor"),
            # approx_count_distinct keeps state bounded; exact distinct counts
            # hold every value seen in the window, which is unbounded per account
            F.approx_count_distinct("merchant_id").cast("int").alias("distinct_merchants"),
            F.avg("amount_minor").cast("double").alias("avg_amount_minor"),
            F.stddev_pop("amount_minor").cast("double").alias("stddev_amount"),
        )
        .select(
            "account_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "spend_minor", "txn_count", "max_amount_minor",
            "distinct_merchants", "avg_amount_minor", "stddev_amount",
        )
    )


def merchant_features(txns: DataFrame) -> DataFrame:
    """Per-merchant rollups, for the category views in the dashboard."""
    return (
        txns.filter(F.col("merchant_id").isNotNull())
        .groupBy("tenant_id", "merchant_id", "merchant_name", "category")
        .agg(
            F.sum("amount_minor").cast("long").alias("spend_minor"),
            F.count("*").cast("int").alias("txn_count"),
            F.avg("confidence").cast("double").alias("avg_confidence"),
        )
    )


def normalization_coverage(txns: DataFrame) -> DataFrame:
    """What fraction of descriptors resolved, by normalizer version.

    The number to watch when shipping a new normalizer: a version that resolves
    more but resolves them wrongly looks identical here, which is why the
    version diff in Postgres exists too.
    """
    return (
        txns.groupBy("normalizer_version")
        .agg(
            F.count("*").cast("int").alias("total"),
            F.sum(F.when(F.col("merchant_id").isNotNull(), 1).otherwise(0))
                .cast("int").alias("resolved"),
            F.avg(F.when(F.col("merchant_id").isNotNull(), F.col("confidence")))
                .cast("double").alias("avg_confidence"),
        )
        .withColumn(
            "coverage",
            F.round(F.col("resolved") / F.col("total"), 4),
        )
    )


def _interval(delta) -> str:  # type: ignore[no-untyped-def]
    seconds = int(delta.total_seconds())
    if seconds % 3600 == 0:
        return f"{seconds // 3600} hours"
    if seconds % 60 == 0:
        return f"{seconds // 60} minutes"
    return f"{seconds} seconds"
