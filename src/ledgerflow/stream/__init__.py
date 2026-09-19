"""Stream selection. Nothing downstream knows which backend is in use."""

from __future__ import annotations

from ..config import settings
from .base import (
    FRAUD_SIGNALS,
    LEDGER_EVENTS,
    NORMALIZED_TRANSACTIONS,
    EventStream,
    Message,
    lag,
)

_stream: EventStream | None = None


def get_stream() -> EventStream:
    global _stream
    if _stream is None:
        if settings.stream_backend == "kafka":
            from .kafka import KafkaStream

            _stream = KafkaStream()
        else:
            from .pg import PostgresStream

            _stream = PostgresStream()
    return _stream


__all__ = [
    "EventStream", "Message", "get_stream", "lag",
    "LEDGER_EVENTS", "NORMALIZED_TRANSACTIONS", "FRAUD_SIGNALS",
]
