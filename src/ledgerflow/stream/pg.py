"""Postgres-backed event stream.

Mimics the parts of Kafka the platform actually relies on: append-only ordered
messages, ordering within a partition key, and consumer groups with independent
offsets. Good enough that the whole system runs on one container, and close
enough in semantics that swapping in the Kafka adapter changes no consumer
code.

What it does NOT provide, and would not survive at scale: parallel consumers
within a group, partition rebalancing, or retention. Those are the reasons the
Kafka adapter exists -- not throughput.
"""

from __future__ import annotations

import json
from typing import Any

from ..adapters.db import pool
from .base import Message


class PostgresStream:
    def publish(
        self,
        *,
        topic: str,
        partition_key: str,
        event_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        with pool().connection() as conn:
            conn.execute(
                "INSERT INTO stream_messages "
                "(topic, partition_key, event_id, event_type, payload) "
                "VALUES (%s, %s, %s, %s, %s)",
                (topic, partition_key, event_id, event_type, json.dumps(payload)),
            )

    def publish_batch(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with pool().connection() as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO stream_messages "
                "(topic, partition_key, event_id, event_type, payload) "
                "VALUES (%s, %s, %s, %s, %s)",
                [
                    (
                        r["topic"], r["partition_key"], r["event_id"],
                        r["event_type"], json.dumps(r["payload"]),
                    )
                    for r in rows
                ],
            )

    def poll(self, *, topic: str, consumer_group: str, limit: int) -> list[Message]:
        with pool().connection() as conn:
            rows = conn.execute(
                """
                SELECT m.* FROM stream_messages m
                 WHERE m.topic = %s
                   AND m.offset_id > COALESCE(
                        (SELECT last_offset FROM consumer_offsets
                          WHERE consumer_group = %s AND topic = %s), 0)
                 ORDER BY m.offset_id
                 LIMIT %s
                """,
                (topic, consumer_group, topic, limit),
            ).fetchall()
        return [
            Message(
                offset=r["offset_id"],
                topic=r["topic"],
                partition_key=r["partition_key"],
                event_id=r["event_id"],
                event_type=r["event_type"],
                payload=r["payload"],
                published_at=r["published_at"],
            )
            for r in rows
        ]

    def commit(self, *, topic: str, consumer_group: str, offset: int) -> None:
        with pool().connection() as conn:
            conn.execute(
                """
                INSERT INTO consumer_offsets (consumer_group, topic, last_offset)
                VALUES (%s, %s, %s)
                ON CONFLICT (consumer_group, topic) DO UPDATE
                    SET last_offset = GREATEST(consumer_offsets.last_offset, EXCLUDED.last_offset),
                        updated_at = now()
                """,
                (consumer_group, topic, offset),
            )

    def offset(self, *, topic: str, consumer_group: str) -> int:
        with pool().connection() as conn:
            row = conn.execute(
                "SELECT last_offset FROM consumer_offsets "
                "WHERE consumer_group = %s AND topic = %s",
                (consumer_group, topic),
            ).fetchone()
        return int(row["last_offset"]) if row else 0

    def seek(self, *, topic: str, consumer_group: str, offset: int) -> None:
        """Rewind (or fast-forward) a group. The mechanism behind replay.

        Unlike ``commit`` this does not take a GREATEST, because moving
        backwards is the entire point.
        """
        with pool().connection() as conn:
            conn.execute(
                """
                INSERT INTO consumer_offsets (consumer_group, topic, last_offset)
                VALUES (%s, %s, %s)
                ON CONFLICT (consumer_group, topic) DO UPDATE
                    SET last_offset = EXCLUDED.last_offset, updated_at = now()
                """,
                (consumer_group, topic, offset),
            )

    def head(self, topic: str) -> int:
        with pool().connection() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(offset_id), 0) AS head FROM stream_messages WHERE topic = %s",
                (topic,),
            ).fetchone()
        return int(row["head"]) if row else 0
