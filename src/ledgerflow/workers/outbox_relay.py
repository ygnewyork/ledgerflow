"""The outbox relay: database rows out to the stream.

Not a stream consumer -- it is the thing that *feeds* the stream. It reads
committed outbox rows and publishes them.

The crash window is deliberate and worth understanding. The relay can die after
publishing and before marking the batch sent, which republishes those events on
restart. That is why delivery is at-least-once and every consumer dedupes. The
alternative -- marking sent before publishing -- loses events instead, and a
lost financial event is unrecoverable while a duplicate one is merely absorbed.

Upgrade path: replace this polling loop with Debezium CDC on the WAL. Same
topic, same consumers, no schema change. That interchangeability is the payoff
for having an outbox at all.
"""

from __future__ import annotations

import logging
import time

from ..adapters.db import unit_of_work
from ..stream import LEDGER_EVENTS, get_stream

log = logging.getLogger("ledgerflow.relay")


def relay_once(batch_size: int = 500) -> int:
    stream = get_stream()
    published = 0

    with unit_of_work() as uow:
        rows = uow.events.claim_unpublished(limit=batch_size)
        if not rows:
            return 0

        sent: list[int] = []
        for row in rows:
            try:
                stream.publish(
                    topic=LEDGER_EVENTS,
                    partition_key=row["partition_key"],
                    event_id=row["event_id"],
                    event_type=row["event_type"],
                    payload=row["payload"],
                )
                sent.append(row["id"])
                published += 1
            except Exception:
                # stop at the first failure rather than skipping ahead: the
                # outbox is ordered, and publishing past a gap would deliver
                # an account's events out of order
                log.exception("publish failed at outbox id=%s", row["id"])
                break

        uow.events.mark_published(sent)

    return published


def run(*, once: bool = False, poll_interval: float = 0.25) -> int:
    total = 0
    while True:
        count = relay_once()
        total += count
        if once and count == 0:
            break
        if count == 0:
            time.sleep(poll_interval)
    return total
