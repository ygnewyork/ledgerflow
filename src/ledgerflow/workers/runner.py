"""The consumer loop every worker shares.

Three properties, all of which are easy to get wrong individually and are
therefore implemented once here rather than in each worker:

1. **Claim and side effect commit together.** The dedupe row and the work share
   a transaction, so at-least-once delivery becomes effectively-once
   processing. Split them and a crash between leaves an event marked done that
   never happened.

2. **Offsets commit after the work transaction.** Never before, never with
   auto-commit. Auto-commit advances the offset on poll, so a crash mid-
   processing skips the event entirely -- at-most-once, silently.

3. **A poison message never stalls the topic.** Bounded retries with jitter,
   then the dead-letter table, then move on. Blocking on one bad message blocks
   every account behind it.
"""

from __future__ import annotations

import contextlib
import logging
import random
import signal
import time
import traceback
from abc import ABC, abstractmethod
from typing import Any

from .. import ids
from ..adapters.db import UnitOfWork, unit_of_work
from ..config import settings
from ..stream import Message, get_stream

log = logging.getLogger("ledgerflow.worker")


class PoisonMessage(Exception):
    """Retrying will not help: malformed payload, or a bug in this consumer.

    Distinguished from a transient failure because the response is different.
    A timeout deserves a retry; a payload missing a required field deserves the
    dead-letter table and an engineer.
    """


class Consumer(ABC):
    group: str
    topic: str

    @abstractmethod
    def handle(self, uow: UnitOfWork, message: Message) -> None:
        """Do the work. Runs inside the transaction that holds the claim."""

    def on_idle(self) -> None:  # noqa: B027
        """Hook for periodic maintenance when there is nothing to process.

        Deliberately concrete and empty, not abstract: most consumers have no
        idle work, and forcing every one of them to write ``pass`` would be a
        worse interface than letting them say nothing.
        """


_shutdown = False


def _install_signal_handlers() -> None:
    def stop(signum: int, frame: Any) -> None:
        global _shutdown
        _shutdown = True
        log.info("shutdown requested; finishing the current message")

    for sig in (signal.SIGINT, signal.SIGTERM):
        # not on the main thread (tests): there is no handler to install
        with contextlib.suppress(ValueError):
            signal.signal(sig, stop)


def process_one(consumer: Consumer, message: Message) -> bool:
    """Returns True when the offset may advance.

    Always True in practice: a dead-lettered message is *handled*, just not
    successfully, and leaving the offset parked on it would stall the topic.
    """
    attempt = 0
    last_error: Exception | None = None

    while attempt < settings.consumer_max_attempts:
        attempt += 1
        try:
            with unit_of_work() as uow:
                if not uow.events.claim_event(consumer.group, message.event_id):
                    log.debug("%s: duplicate %s, skipping", consumer.group, message.event_id)
                    return True
                consumer.handle(uow, message)
            return True
        except PoisonMessage as exc:
            last_error = exc
            break  # retrying a poison message is just burning cycles
        except Exception as exc:
            last_error = exc
            if attempt < settings.consumer_max_attempts:
                # jitter, so N workers hitting the same downed dependency do
                # not retry in lockstep and hammer it back down
                delay = (2 ** attempt) * 0.1 * (0.5 + random.random())
                log.warning(
                    "%s: attempt %d/%d failed for %s (%s); retrying in %.2fs",
                    consumer.group, attempt, settings.consumer_max_attempts,
                    message.event_id, exc, delay,
                )
                time.sleep(delay)

    with unit_of_work() as uow:
        uow.events.dead_letter(
            dlq_id=ids.new_id("dlq"),
            consumer_group=consumer.group,
            event_id=message.event_id,
            topic=message.topic,
            payload=message.payload,
            error_class=type(last_error).__name__ if last_error else "Unknown",
            error_detail="".join(
                traceback.format_exception_only(type(last_error), last_error)
            ) if last_error else "",
            attempts=attempt,
        )
    log.error("%s: dead-lettered %s after %d attempts", consumer.group, message.event_id, attempt)
    return True


def run(consumer: Consumer, *, once: bool = False, poll_interval: float = 0.5) -> int:
    """Poll, process, commit. ``once=True`` drains and exits, for tests."""
    _install_signal_handlers()
    stream = get_stream()
    processed = 0

    while not _shutdown:
        batch = stream.poll(
            topic=consumer.topic,
            consumer_group=consumer.group,
            limit=settings.consumer_batch_size,
        )
        if not batch:
            consumer.on_idle()
            if once:
                break
            time.sleep(poll_interval)
            continue

        for message in batch:
            if process_one(consumer, message):
                # after the work transaction, one message at a time: a crash
                # here replays only the last message, and dedupe absorbs it
                stream.commit(
                    topic=consumer.topic,
                    consumer_group=consumer.group,
                    offset=message.offset,
                )
                processed += 1
            if _shutdown:
                break

    return processed
