"""The event stream port.

Consumers depend on this interface, never on Kafka. That is what makes the
local-development Postgres transport and a production broker interchangeable,
and it is why the outbox was worth defining before any broker existed.

Topics carry a version suffix (``ledger.events.v1``). A breaking payload change
is a new topic and a parallel consumer, not a mutation of a topic other people
are already reading.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator, Protocol

LEDGER_EVENTS = "ledger.events.v1"
NORMALIZED_TRANSACTIONS = "transactions.normalized.v1"
FRAUD_SIGNALS = "fraud.signals.v1"


@dataclass(frozen=True, slots=True)
class Message:
    offset: int
    topic: str
    partition_key: str
    event_id: str
    event_type: str
    payload: dict[str, Any]
    published_at: datetime


class EventStream(Protocol):
    """Publish and consume. Deliberately small.

    There is no ``ack(message)``: offsets are committed by the consumer runner
    after its work transaction, in one call per batch. An ack-per-message API
    invites committing the offset before the work is durable, which converts
    at-least-once into silent at-most-once.
    """

    def publish(
        self,
        *,
        topic: str,
        partition_key: str,
        event_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        ...

    def poll(
        self, *, topic: str, consumer_group: str, limit: int
    ) -> list[Message]:
        ...

    def commit(self, *, topic: str, consumer_group: str, offset: int) -> None:
        ...

    def offset(self, *, topic: str, consumer_group: str) -> int:
        ...

    def seek(self, *, topic: str, consumer_group: str, offset: int) -> None:
        ...

    def head(self, topic: str) -> int:
        ...


def lag(stream: EventStream, topic: str, consumer_group: str) -> int:
    """How far behind a group is. The number the health panel shows."""
    return max(0, stream.head(topic) - stream.offset(topic=topic, consumer_group=consumer_group))
