"""SparkSession construction.

Delta is used when its jars are available and Parquet otherwise, so the jobs
run on a laptop with no Maven access and unchanged on a cluster. The table
layout and the merge semantics are the same either way; only the writer
differs.
"""

from __future__ import annotations

import os
from typing import Any

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"


def delta_available() -> bool:
    return os.environ.get("LEDGERFLOW_SPARK_FORMAT", "delta").lower() == "delta" and _jars_present()


def _jars_present() -> bool:
    try:
        import delta  # noqa: F401

        return True
    except ImportError:
        return False


def table_format() -> str:
    return "delta" if delta_available() else "parquet"


def build(app_name: str, *, streaming: bool = False, **extra: Any):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        # event-time semantics only make sense against a fixed zone. leaving
        # this to the JVM default means the same job produces different window
        # boundaries on a laptop and on a cluster.
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
    )
    if streaming:
        builder = builder.config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        )
    if delta_available():
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
    for key, value in extra.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
