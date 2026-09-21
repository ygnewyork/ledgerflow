"""Risk worker: windowed features and rule evaluation."""

from __future__ import annotations

import logging
from datetime import datetime

from .. import ids
from ..adapters.db import UnitOfWork
from ..features import rules
from ..features.compute import compute
from ..features.windows import WATERMARK
from ..stream import FRAUD_SIGNALS, NORMALIZED_TRANSACTIONS, Message, get_stream
from .runner import Consumer, PoisonMessage

log = logging.getLogger("ledgerflow.risk")


class RiskWorker(Consumer):
    group = "risk"
    topic = NORMALIZED_TRANSACTIONS

    def __init__(self) -> None:
        # the watermark: the newest event time seen so far, minus the allowed
        # lateness. tracked per worker, exactly as Spark tracks it per query.
        self._max_event_time: datetime | None = None

    def handle(self, uow: UnitOfWork, message: Message) -> None:
        payload = message.payload
        try:
            account_id = payload["account_id"]
            occurred_at = datetime.fromisoformat(payload["occurred_at"])
            amount_minor = int(payload["amount_minor"])
        except (KeyError, TypeError, ValueError) as exc:
            raise PoisonMessage(
                f"event {message.event_id} is not a normalized transaction") from exc

        if self._max_event_time is None or occurred_at > self._max_event_time:
            self._max_event_time = occurred_at
        watermark = self._max_event_time - WATERMARK

        if occurred_at < watermark:
            # too late for its window. dropping it from the aggregate is
            # correct; dropping it silently is not -- a rising late-arrival
            # rate means the watermark is too tight or a producer is lagging.
            uow.risk.record_late_arrival(
                event_id=message.event_id,
                account_id=account_id,
                effective_at=occurred_at,
                watermark_at=watermark,
            )
            return

        # as_of is the EVENT's time, never now(). see features/compute.py.
        features = compute(
            uow,
            account_id=account_id,
            as_of=occurred_at,
            amount_minor=amount_minor,
            merchant_id=payload.get("merchant_id"),
        )

        uow.risk.upsert_features(account_id, occurred_at, {
            "spend_1h": features.spend_1h,
            "spend_24h": features.spend_24h,
            "txn_count_1h": features.txn_count_1h,
            "max_1h": features.max_amount_1h,
            "distinct_7d": features.distinct_merchants_7d,
            "avg_90d": int(features.avg_amount_90d),
            "stddev_90d": features.stddev_amount_90d,
        })

        # advisory only. A blocking rule evaluated here would be theatre: the
        # money moved before this worker ever saw the event, so recording a
        # "block" after the fact claims a refusal that never happened.
        fired = rules.advisory(features)
        for rule in fired:
            signal = uow.risk.insert_signal(
                signal_id=ids.new_id("sig"),
                tenant_id=payload["tenant_id"],
                account_id=account_id,
                transaction_id=payload.get("transaction_id"),
                rule=rule.name,
                score=rule.score,
                # the feature values AS OF evaluation. without these the
                # decision cannot be explained or reproduced later.
                features=features.to_dict(),
            )
            get_stream().publish(
                topic=FRAUD_SIGNALS,
                partition_key=account_id,
                event_id=f"{message.event_id}:{rule.name}",
                event_type="fraud.signal",
                payload={
                    "signal_id": signal["id"],
                    "tenant_id": payload["tenant_id"],
                    "account_id": account_id,
                    "transaction_id": payload.get("transaction_id"),
                    "rule": rule.name,
                    "score": rule.score,
                    "description": rule.description,
                },
            )
        if fired:
            log.info("account %s fired %s", account_id, [r.name for r in fired])
