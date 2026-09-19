"""Webhook fan-out and delivery.

Two jobs in one module because they are two halves of the same thing: the
consumer turns events into queued deliveries, and the dispatcher attempts them.

Splitting the queueing from the sending is what keeps a slow customer endpoint
off the ledger's write path. Nothing in the transaction that moves money waits
on someone else's HTTP server.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from typing import Any

import httpx

from .. import ids
from ..adapters.db import UnitOfWork, unit_of_work
from ..config import settings
from ..stream import LEDGER_EVENTS, Message
from .runner import Consumer

log = logging.getLogger("ledgerflow.webhooks")


class WebhookFanout(Consumer):
    """Event -> a pending delivery row per subscribed endpoint."""

    group = "webhooks"
    topic = LEDGER_EVENTS

    def handle(self, uow: UnitOfWork, message: Message) -> None:
        row = uow.one(
            "SELECT tenant_id, mode FROM outbox WHERE event_id = %s", (message.event_id,)
        )
        if row is None:
            return
        for endpoint in uow.webhooks.endpoints_for(
            row["tenant_id"], row["mode"], message.event_type
        ):
            uow.webhooks.enqueue(
                delivery_id=ids.new_id("whd"),
                endpoint_id=endpoint["id"],
                event_id=message.event_id,
            )


def sign(secret: bytes, timestamp: int, body: bytes) -> str:
    """HMAC-SHA256 over ``{timestamp}.{raw body}``.

    The timestamp is inside the signed payload on purpose. Sign only the body
    and an attacker who captures one delivery can replay it forever; with the
    timestamp signed, a receiver rejecting anything outside a few minutes makes
    replay useless.

    Signed over the RAW bytes, before any JSON parsing. Re-serializing changes
    key order and whitespace and breaks every signature.
    """
    return hmac.new(secret, f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()


def _backoff(attempt: int) -> int | None:
    schedule = settings.webhook_backoff_seconds
    return schedule[attempt] if attempt < len(schedule) else None


def dispatch_once(limit: int = 50, client: httpx.Client | None = None) -> dict[str, int]:
    """Attempt every due delivery. Returns a tally for the health panel."""
    owned = client is None
    client = client or httpx.Client(timeout=settings.webhook_timeout_seconds)
    tally = {"succeeded": 0, "retrying": 0, "exhausted": 0}

    try:
        with unit_of_work() as uow:
            due = uow.webhooks.claim_due(limit=limit)
            for delivery in due:
                event = uow.one(
                    "SELECT * FROM outbox WHERE event_id = %s", (delivery["event_id"],)
                )
                if event is None:
                    continue

                body = json.dumps({
                    "id": event["event_id"],
                    "object": "event",
                    "type": event["event_type"],
                    "created": int(event["created_at"].timestamp()),
                    "api_version": str(settings.api_version),
                    "data": event["payload"].get("data", {}),
                }, separators=(",", ":")).encode()

                timestamp = int(time.time())
                # the endpoint secret is stored hashed; the hash is what we sign
                # with, so a database read cannot reveal a reusable secret
                signature = sign(bytes(delivery["secret_hash"]), timestamp, body)
                attempt = delivery["attempt"] + 1

                try:
                    response = client.post(
                        delivery["url"],
                        content=body,
                        headers={
                            "Content-Type": "application/json",
                            "LedgerFlow-Signature": f"t={timestamp},v1={signature}",
                            "LedgerFlow-Event-Id": event["event_id"],
                            "User-Agent": "LedgerFlow/1.0",
                        },
                    )
                    ok = 200 <= response.status_code < 300
                    body_text = response.text[:2000]
                    error = None if ok else f"HTTP {response.status_code}"
                except Exception as exc:  # noqa: BLE001
                    ok, response, body_text, error = False, None, None, str(exc)

                if ok:
                    uow.webhooks.record_attempt(
                        delivery_id=delivery["id"], attempt=attempt, status="succeeded",
                        response_code=response.status_code if response else None,
                        response_body=body_text, error=None, next_attempt_seconds=None,
                    )
                    tally["succeeded"] += 1
                    continue

                delay = _backoff(attempt)
                if delay is None:
                    uow.webhooks.record_attempt(
                        delivery_id=delivery["id"], attempt=attempt, status="exhausted",
                        response_code=response.status_code if response else None,
                        response_body=body_text, error=error, next_attempt_seconds=None,
                    )
                    # an endpoint that failed every attempt over 24h is gone.
                    # keep retrying forever and the queue fills with corpses.
                    uow.webhooks.disable_endpoint(delivery["endpoint_id"])
                    tally["exhausted"] += 1
                else:
                    uow.webhooks.record_attempt(
                        delivery_id=delivery["id"], attempt=attempt, status="pending",
                        response_code=response.status_code if response else None,
                        response_body=body_text, error=error, next_attempt_seconds=delay,
                    )
                    tally["retrying"] += 1
    finally:
        if owned:
            client.close()

    return tally
