"""Configuration, read once from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str = os.environ.get(
        "LEDGERFLOW_DATABASE_URL", "postgresql://localhost/ledgerflow"
    )
    redis_url: str | None = os.environ.get("LEDGERFLOW_REDIS_URL") or None

    # "pg" runs the whole platform on Postgres alone; "kafka" uses a broker.
    # Consumers do not know which is in use.
    stream_backend: str = os.environ.get("LEDGERFLOW_STREAM", "pg")
    kafka_brokers: str = os.environ.get("LEDGERFLOW_KAFKA_BROKERS", "localhost:9092")

    api_version: date = date(2026, 9, 17)
    idempotency_ttl_hours: int = 24
    idempotency_lease_seconds: int = 30
    # how long a duplicate request waits behind an in-flight one before 409
    idempotency_lock_timeout_ms: int = 3000

    rate_limit_per_minute: int = int(os.environ.get("LEDGERFLOW_RATE_LIMIT", "600"))

    webhook_timeout_seconds: float = 5.0
    # 1m, 5m, 30m, 2h, 6h, 24h -- then the delivery is exhausted
    webhook_backoff_seconds: tuple[int, ...] = (60, 300, 1800, 7200, 21600, 86400)

    consumer_max_attempts: int = 3
    consumer_batch_size: int = 200

    # How late an event may be and still count toward a feature window.
    # Mirrors the Spark watermark exactly; see src/ledgerflow/features/windows.py.
    watermark_seconds: int = 7200


settings = Settings()
