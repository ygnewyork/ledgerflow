"""Blocking risk decisions: the checks that are allowed to say no.

Everything else in the risk pipeline runs after COMMIT and can only annotate.
One rule runs *inside* the write transaction and refuses the posting, and these
tests pin the three properties that make that safe to ship: it fires at a
higher bar than the flag that shadows it, a refusal leaves no entry behind, and
the refusal is still written down even though the transaction rolled back.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ledgerflow import ids
from ledgerflow.adapters.db import read_only, unit_of_work
from ledgerflow.application.errors import TransactionDeclined
from ledgerflow.application.services import post_transaction
from ledgerflow.domain.ledger import AccountType
from ledgerflow.domain.money import Money
from ledgerflow.features import rules
from ledgerflow.features.compute import Features

NOW = datetime(2026, 9, 17, 16, 0, tzinfo=timezone.utc)

# The advisory rule flags at >10 tiny charges; the blocking rule waits for >12.
# The probe counts what is already on the ledger, so 13 charges have to land
# before the 14th is the one refused.
PRIOR = 13


def _probe(**overrides) -> Features:
    base = {
        "account_id": "acct_x", "as_of": NOW, "amount_minor": 180,
        "spend_1h": 0, "spend_24h": 0, "txn_count_1h": 0, "max_amount_1h": 0,
        "distinct_merchants_7d": 0, "merchant_frequency": 0,
        "avg_amount_90d": 0.0, "stddev_amount_90d": 0.0,
    }
    return Features(**{**base, **overrides})


def _charge(ctx, funding: str, when: datetime, amount: int = 180) -> None:
    with unit_of_work() as uow:
        post_transaction(
            uow, ctx, kind="card_purchase",
            accounts={"expense": "groceries", "funding": funding},
            amount=Money(amount, "usd"), effective_at=when,
            descriptor="SQ *TESTMERCH",
        )


def _burst(ctx, funding: str, count: int = PRIOR) -> None:
    """Charges spaced across the hour, all inside the 1h velocity window."""
    for i in range(count):
        _charge(ctx, funding, NOW - timedelta(minutes=55 - i * 2))


# ---------------------------------------------------------------------------
# The advisory / blocking split
# ---------------------------------------------------------------------------


def test_the_worker_can_never_claim_a_refusal_it_did_not_make():
    """``advisory()`` must never return a blocking rule.

    The async worker runs after COMMIT. If it recorded a blocking rule there,
    the dashboard would show a decline for a transaction whose money had
    already moved -- a refusal that never happened.
    """
    features = _probe(txn_count_1h=50, max_amount_1h=180)

    assert rules.BLOCKING, "there is no blocking rule left to test"
    assert any(r.blocking for r in rules.evaluate(features)), "fixture too weak"
    assert not any(r.blocking for r in rules.advisory(features))


def test_blocking_waits_longer_than_flagging():
    """A human reading a flag is cheap; a decline at a till is not.

    The blocking rule must not fire on any input where its advisory twin has
    not already fired -- otherwise we decline before anyone was even warned.
    """
    for count in range(30):
        features = _probe(txn_count_1h=count, max_amount_1h=180)
        fired = {r.name for r in rules.evaluate(features)}
        if "card_testing_block" in fired:
            assert "card_testing" in fired, (
                f"at {count} txns the block fires without the flag"
            )


# ---------------------------------------------------------------------------
# What a decline does to the ledger
# ---------------------------------------------------------------------------


def test_a_declined_posting_writes_no_entry(funded):
    """The point of blocking: there is nothing to reverse afterwards."""
    ctx, checking = funded["ctx"], funded["accounts"]["checking"]
    _burst(ctx, "checking")

    with read_only() as uow:
        before = uow.execute(
            "SELECT COUNT(*)::int AS n FROM entries WHERE account_id = %s", (checking,)
        )[0]["n"]
        balance_before = uow.accounts.balance(checking)

    with pytest.raises(TransactionDeclined) as declined:
        _charge(ctx, "checking", NOW)

    with read_only() as uow:
        after = uow.execute(
            "SELECT COUNT(*)::int AS n FROM entries WHERE account_id = %s", (checking,)
        )[0]["n"]
        balance_after = uow.accounts.balance(checking)

    assert declined.value.extra["rule"] == "card_testing_block"
    assert after == before, "a declined posting left entries behind"
    assert balance_after == balance_before, "a declined posting moved money"


def test_the_decline_survives_the_rollback(funded):
    """Recorded in its own transaction, or it vanishes with the posting.

    A decline is the event a customer is most likely to call about. The ledger
    correctly has no entry -- no money moved -- so the only trace is this row.
    """
    ctx = funded["ctx"]
    _burst(ctx, "checking")

    with pytest.raises(TransactionDeclined) as declined:
        _charge(ctx, "checking", NOW)

    from ledgerflow.application.services import record_decline

    record_decline(ctx, declined.value)

    with read_only() as uow:
        rows = uow.execute(
            "SELECT rule, action, score, attempted_amount_minor FROM fraud_signals "
            "WHERE tenant_id = %s AND action = 'block'",
            (funded["tenant_id"],),
        )
    assert len(rows) == 1, "the decline left no trace"
    assert rows[0]["rule"] == "card_testing_block"
    assert rows[0]["attempted_amount_minor"] == 180

    # the score has to come from the rule that fired, not a constant, or a
    # retuned threshold ships with the old number attached to it
    rule = next(r for r in rules.BLOCKING if r.name == rows[0]["rule"])
    assert rows[0]["score"] == pytest.approx(rule.score)


def test_the_api_declines_with_402(funded, client):
    """402, not 400: the request was well-formed and the caller did nothing wrong."""
    ctx = funded["ctx"]
    _burst(ctx, "checking")

    response = client.post(
        "/v1/transactions",
        headers={"Idempotency-Key": ids.new_id("blk")},
        json={
            "kind": "card_purchase", "amount": 180, "effective_at": NOW.isoformat(),
            "accounts": {"expense": "groceries", "funding": "checking"},
        },
    )

    assert response.status_code == 402, response.text
    body = response.json()["error"]
    assert body["code"] == "transaction_declined"
    assert body["rule"] == "card_testing_block"
    assert body["features"]["txn_count_1h"] == PRIOR


# ---------------------------------------------------------------------------
# The credit-side regression
# ---------------------------------------------------------------------------


def test_velocity_sees_spending_on_a_credit_card(funded):
    """The bug this test exists for.

    The velocity probe used to ask for ``direction <> normal_balance``, which
    reads as "spending" only if you assume the funding account is an asset. On
    a credit card -- a liability, whose normal balance IS credit -- that test
    matched nothing, so every velocity feature scored a card-testing burst on
    a stolen credit card as zero transactions. Spending always CREDITS the
    funding account, whatever its type, and that is what the query asks now.
    """
    ctx = funded["ctx"]
    with unit_of_work() as uow:
        card = uow.accounts.create(
            account_id=ids.account_id(), tenant_id=funded["tenant_id"], mode="test",
            name="Liabilities:Credit Card", type=AccountType.LIABILITY,
            currency="usd", external_id="card", minimum_balance=None,
        )

    _burst(ctx, "card")

    with read_only() as uow:
        probe = uow.risk.velocity_probe(card["id"], NOW)

    assert probe["txn_count"] == PRIOR, (
        "velocity is blind to credit-card spending: "
        f"counted {probe['txn_count']} of {PRIOR} charges"
    )

    with pytest.raises(TransactionDeclined):
        _charge(ctx, "card", NOW)
