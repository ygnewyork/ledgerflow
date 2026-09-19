"""Normalizer: raw descriptors to canonical merchants."""

from __future__ import annotations

import logging

from .. import ids
from ..adapters.db import UnitOfWork
from ..normalization.clean import VERSION
from ..normalization.resolve import resolve
from ..stream import LEDGER_EVENTS, NORMALIZED_TRANSACTIONS, Message, get_stream
from .runner import Consumer, PoisonMessage

log = logging.getLogger("ledgerflow.normalizer")


class Normalizer(Consumer):
    group = "normalizer"
    topic = LEDGER_EVENTS

    def handle(self, uow: UnitOfWork, message: Message) -> None:
        if message.event_type not in ("transaction.created", "transaction.reversed"):
            return

        try:
            txn = message.payload["data"]["object"]
            txn_id = txn["id"]
        except (KeyError, TypeError) as exc:
            # a malformed payload will be malformed forever; retrying is just
            # burning cycles before the inevitable dead letter
            raise PoisonMessage(f"event {message.event_id} has no transaction object") from exc

        row = uow.normalization.raw_for_transaction(txn_id)
        if row is None:
            return  # no descriptor on this transaction; nothing to normalize

        resolution = resolve(uow, row["tenant_id"], row["descriptor"])

        uow.normalization.upsert_normalized(
            normalized_id=ids.new_id("ntx"),
            raw_transaction_id=row["id"],
            tenant_id=row["tenant_id"],
            account_id=row["account_id"],
            merchant_id=resolution.merchant_id,
            merchant_name=resolution.merchant_name,
            category=resolution.category,
            confidence=resolution.confidence,
            normalizer_version=VERSION,
        )

        # downstream consumers read the normalized topic, not the raw ledger,
        # so risk scoring sees a merchant id rather than a descriptor
        get_stream().publish(
            topic=NORMALIZED_TRANSACTIONS,
            partition_key=row["account_id"],
            event_id=f"{message.event_id}:norm",
            event_type="transaction.normalized",
            payload={
                "transaction_id": txn_id,
                "raw_transaction_id": row["id"],
                "tenant_id": row["tenant_id"],
                "account_id": row["account_id"],
                "amount_minor": row["amount_minor"],
                "currency": row["currency"],
                "occurred_at": row["occurred_at"].isoformat(),
                "descriptor": row["descriptor"],
                "cleaned": resolution.cleaned,
                "merchant_id": resolution.merchant_id,
                "merchant_name": resolution.merchant_name,
                "category": resolution.category,
                "confidence": resolution.confidence,
                "normalizer_version": VERSION,
            },
        )
