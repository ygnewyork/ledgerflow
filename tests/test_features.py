"""Point-in-time correctness, watermarks, and the online/offline contract."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ledgerflow import ids
from ledgerflow.adapters.db import read_only, unit_of_work
from ledgerflow.application.services import post_transaction
from ledgerflow.domain.money import Money
from ledgerflow.features import rules, windows
from ledgerflow.features.compute import compute
from ledgerflow.stream import LEDGER_EVENTS, NORMALIZED_TRANSACTIONS, get_stream
from ledgerflow.workers import drain_all

NOW = datetime(2026, 9, 17, 16, 0, tzinfo=timezone.utc)


def _spend(ctx, when: datetime, amount: int, descriptor: str = "STARBUCKS #04212") -> None:
    with unit_of_work() as uow:
        post_transaction(
            uow, ctx, kind="card_purchase",
            accounts={"expense": "groceries", "funding": "checking"},
            amount=Money(amount, "usd"), effective_at=when, descriptor=descriptor,
        )


def test_features_use_event_time_not_wall_clock(funded):
    """The rule that makes this a feature pipeline rather than a dashboard.

    A feature computed for a September 17 event must not see September 18 data.
    Online that is merely wrong; offline it trains a model on information it
    will never have at inference time.
    """
    ctx = funded["ctx"]
    _spend(ctx, NOW - timedelta(minutes=30), 5_000)   # inside the 1h window
    _spend(ctx, NOW - timedelta(hours=5), 90_000)     # outside it
    _spend(ctx, NOW + timedelta(days=1), 70_000)      # the FUTURE, relative to NOW

    with read_only() as uow:
        features = compute(
            uow, account_id=funded["accounts"]["checking"], as_of=NOW,
            amount_minor=1_000, merchant_id=None,
        )

    assert features.spend_1h == 5_000, "the 1h window picked up events outside it"
    assert features.spend_24h == 95_000, "the future leaked into a past window"


def test_the_same_history_produces_the_same_features(funded):
    """Recomputation must be deterministic.

    This is the test that catches a stray ``now()`` anywhere in the path: with
    a fixed as_of, two runs over identical history must agree exactly.
    """
    ctx = funded["ctx"]
    for minutes in (10, 25, 40):
        _spend(ctx, NOW - timedelta(minutes=minutes), 3_000)

    account_id = funded["accounts"]["checking"]
    with read_only() as uow:
        first = compute(uow, account_id=account_id, as_of=NOW,
                        amount_minor=1_000, merchant_id=None)
    with read_only() as uow:
        second = compute(uow, account_id=account_id, as_of=NOW,
                         amount_minor=1_000, merchant_id=None)

    assert first.to_dict() == second.to_dict()


def test_replaying_the_stream_reproduces_identical_features(funded, client):
    """End-to-end determinism: reprocess the whole stream, get the same numbers."""
    for amount in (1_200, 4_500, 9_900):
        client.post("/v1/transactions", headers={"Idempotency-Key": ids.new_id("k")}, json={
            "kind": "card_purchase", "amount": amount,
            "accounts": {"expense": "groceries", "funding": "checking"},
            "descriptor": "WHOLEFDS MKT #10234 AUSTIN TX",
        })
    drain_all()

    account_id = funded["accounts"]["checking"]
    with read_only() as uow:
        before = uow.execute(
            "SELECT window_end, spend_1h_minor, spend_24h_minor, txn_count_1h "
            "FROM account_features WHERE account_id = %s ORDER BY window_end",
            (account_id,),
        )

    stream = get_stream()
    stream.seek(topic=NORMALIZED_TRANSACTIONS, consumer_group="risk", offset=0)
    with unit_of_work() as uow:
        uow.execute("DELETE FROM processed_events WHERE consumer_group = 'risk'")
    drain_all()

    with read_only() as uow:
        after = uow.execute(
            "SELECT window_end, spend_1h_minor, spend_24h_minor, txn_count_1h "
            "FROM account_features WHERE account_id = %s ORDER BY window_end",
            (account_id,),
        )

    assert before == after, "replay produced different features -- something reads now()"


def test_a_zscore_needs_a_baseline_before_it_means_anything(funded):
    """With no spread, the z-score is 0 -- 'no evidence', not a huge number."""
    ctx = funded["ctx"]
    _spend(ctx, NOW - timedelta(minutes=5), 5_000)

    with read_only() as uow:
        features = compute(uow, account_id=funded["accounts"]["checking"], as_of=NOW,
                           amount_minor=500_000, merchant_id=None)

    assert features.stddev_amount_90d == 0
    assert features.amount_zscore == 0.0, "a single sample must not manufacture an outlier"


def test_card_testing_fires_on_many_tiny_charges(funded):
    ctx = funded["ctx"]
    for i in range(12):
        _spend(ctx, NOW - timedelta(minutes=50 - i * 2), 150)

    with read_only() as uow:
        features = compute(uow, account_id=funded["accounts"]["checking"], as_of=NOW,
                           amount_minor=150, merchant_id=None)

    fired = {rule.name for rule in rules.evaluate(features)}
    assert "card_testing" in fired, f"txn_count_1h={features.txn_count_1h}"


def test_signals_are_emitted_never_enforced(funded, client):
    """A flagged purchase still posts. Declining groceries is the worse error."""
    ctx = funded["ctx"]
    for i in range(12):
        _spend(ctx, NOW - timedelta(minutes=50 - i * 2), 150)

    with read_only() as uow:
        posted = uow.execute(
            "SELECT count(*) n FROM transactions WHERE tenant_id = %s", (ctx.tenant_id,)
        )[0]["n"]
    assert posted >= 12, "a risk signal blocked a posting"


def test_online_and_offline_share_one_window_definition():
    """The parity guarantee, asserted rather than asserted-in-a-comment."""
    duration, slide = windows.SPEND_1H.spark_window
    assert duration == "1 hours"
    assert slide == "5 minutes"
    assert windows.SPEND_1H.duration == timedelta(hours=1)

    start, end = windows.SPEND_1H.bounds(NOW)
    assert end - start == windows.SPEND_1H.duration


def test_the_watermark_marks_late_events_late():
    late = NOW - windows.WATERMARK - timedelta(minutes=1)
    on_time = NOW - windows.WATERMARK + timedelta(minutes=1)
    watermark = windows.watermark_for(NOW)

    assert late < watermark, "an event past the allowed lateness should be late"
    assert on_time >= watermark


def test_late_arrivals_are_recorded_not_silently_dropped(funded):
    """Dropping a late event from an aggregate is correct. Hiding it is not."""
    from ledgerflow.stream import Message
    from ledgerflow.workers.risk import RiskWorker

    worker = RiskWorker()
    account_id = funded["accounts"]["checking"]
    base = {
        "tenant_id": funded["tenant_id"], "account_id": account_id,
        "amount_minor": 1_000, "merchant_id": None, "transaction_id": None,
    }

    def message(event_id: str, occurred_at: datetime) -> Message:
        return Message(
            offset=1, topic=NORMALIZED_TRANSACTIONS, partition_key=account_id,
            event_id=event_id, event_type="transaction.normalized",
            payload={**base, "occurred_at": occurred_at.isoformat()},
            published_at=NOW,
        )

    with unit_of_work() as uow:
        worker.handle(uow, message(ids.event_id(), NOW))            # sets the watermark
    with unit_of_work() as uow:
        worker.handle(uow, message(ids.event_id(), NOW - timedelta(hours=6)))  # very late

    with read_only() as uow:
        late = uow.execute(
            "SELECT * FROM late_arrivals WHERE account_id = %s", (account_id,)
        )
    assert len(late) == 1, "a late event was dropped without a trace"
    assert late[0]["lateness"] > timedelta(0)
