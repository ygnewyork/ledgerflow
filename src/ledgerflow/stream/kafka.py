"""Kafka / Redpanda adapter.

Same port as the Postgres transport. Not exercised in the default local setup,
because requiring a broker to run the test suite is a good way to end up with a
test suite nobody runs.

Two choices worth naming:

``enable.auto.commit=False`` -- auto-commit advances the offset on poll, so a
crash mid-processing skips the event entirely. That is at-most-once, silently.
The runner commits after its work transaction instead.

``key=partition_key`` -- Kafka orders within a partition, and the key decides
the partition. Keying by account_id buys per-account ordering, which is the
only ordering that means anything here: two unrelated users' purchases have no
causal relationship, so global ordering would be a cost with no benefit.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from ..config import settings
from .base import Message


class KafkaStream:
    def __init__(self, brokers: str | None = None) -> None:
        from confluent_kafka import Consumer, Producer  # imported lazily

        self._brokers = brokers or settings.kafka_brokers
        self._producer = Producer({
            "bootstrap.servers": self._brokers,
            # wait for all in-sync replicas: a financial event that the leader
            # acknowledged and then lost on failover is exactly the loss the
            # outbox exists to prevent, so do not undo it here
            "acks": "all",
            "enable.idempotence": True,
        })
        self._consumers: dict[tuple[str, str], Consumer] = {}
        self._Consumer = Consumer

    def publish(
        self, *, topic: str, partition_key: str, event_id: str, event_type: str,
        payload: dict[str, Any],
    ) -> None:
        self._producer.produce(
            topic,
            key=partition_key.encode(),
            value=json.dumps({
                "event_id": event_id, "event_type": event_type, "payload": payload,
            }).encode(),
            headers={"event-id": event_id, "event-type": event_type},
        )
        self._producer.flush()

    def _consumer(self, topic: str, group: str):
        key = (topic, group)
        if key not in self._consumers:
            consumer = self._Consumer({
                "bootstrap.servers": self._brokers,
                "group.id": group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            consumer.subscribe([topic])
            self._consumers[key] = consumer
        return self._consumers[key]

    def poll(self, *, topic: str, consumer_group: str, limit: int) -> list[Message]:
        consumer = self._consumer(topic, consumer_group)
        records = consumer.consume(num_messages=limit, timeout=1.0)
        out: list[Message] = []
        for record in records:
            if record.error():
                continue
            body = json.loads(record.value())
            out.append(Message(
                offset=record.offset(),
                topic=record.topic(),
                partition_key=(record.key() or b"").decode(),
                event_id=body["event_id"],
                event_type=body["event_type"],
                payload=body["payload"],
                published_at=datetime.now(timezone.utc),
            ))
        return out

    def commit(self, *, topic: str, consumer_group: str, offset: int) -> None:
        self._consumer(topic, consumer_group).commit(asynchronous=False)

    def offset(self, *, topic: str, consumer_group: str) -> int:
        return 0  # reported by the broker; not tracked client-side

    def seek(self, *, topic: str, consumer_group: str, offset: int) -> None:
        from confluent_kafka import TopicPartition

        self._consumer(topic, consumer_group).seek(TopicPartition(topic, 0, offset))

    def head(self, topic: str) -> int:
        return 0
