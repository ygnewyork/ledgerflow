"""The generator must produce a history the ledger will actually accept.

This exists because it did not. Every account started at zero, so the first
paycheck had to cover rent, the savings transfer and two weeks of groceries
before the second one arrived -- and a 90-day run died partway through on an
overdraft the ledger was right to reject.

The fix was an opening balance plus a pass that replays the timeline against a
running checking balance. These tests pin both.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ledgerflow import loadgen
from ledgerflow.adapters.db import read_only
from ledgerflow.loadgen import Event, _fit_to_checking

NOW = datetime(2026, 9, 17, 12, 0, tzinfo=timezone.utc)


def test_a_discretionary_transfer_shrinks_to_what_is_there():
    events = [
        Event(NOW, "deposit", {"destination": "checking", "income": "income"}, 50_000),
        Event(NOW + timedelta(hours=1), "transfer",
              {"source": "checking", "destination": "savings"}, 45_000),
    ]
    fitted, adapted = _fit_to_checking(events, None)

    transfer = fitted[1]
    assert transfer.amount < 45_000, "the transfer would have overdrawn checking"
    assert adapted["transfers_reduced"] == 1


def test_a_transfer_with_nothing_behind_it_is_dropped():
    events = [
        Event(NOW, "deposit", {"destination": "checking", "income": "income"}, 5_000),
        Event(NOW + timedelta(hours=1), "transfer",
              {"source": "checking", "destination": "investments"}, 90_000),
    ]
    fitted, adapted = _fit_to_checking(events, None)

    assert len(fitted) == 1, "an unaffordable transfer should not be emitted at all"
    assert adapted["transfers_skipped"] == 1


def test_a_purchase_falls_back_to_the_credit_card():
    """What a person does when checking is low, and why the card balance grows."""
    events = [
        Event(NOW, "deposit", {"destination": "checking", "income": "income"}, 20_000),
        Event(NOW + timedelta(hours=1), "card_purchase",
              {"expense": "groceries", "funding": "checking"}, 40_000),
    ]
    fitted, adapted = _fit_to_checking(events, None)

    assert fitted[1].accounts["funding"] == "card"
    assert adapted["moved_to_card"] == 1


def test_rent_is_never_moved_to_a_credit_card():
    """You cannot pay a landlord with a Visa, and pretending otherwise would
    quietly understate the one expense the demo most needs to be right."""
    events = [
        Event(NOW, "deposit", {"destination": "checking", "income": "income"}, 20_000),
        Event(NOW + timedelta(hours=1), "card_purchase",
              {"expense": "rent", "funding": "checking"}, 145_000),
    ]
    fitted, _ = _fit_to_checking(events, None)
    assert fitted[1].accounts["funding"] == "checking"


def test_an_account_opening_at_zero_emits_no_entry(monkeypatch):
    """The domain rejects a zero-amount entry, correctly. "Nothing happened" is
    the absence of a posting, not a posting of nothing."""
    monkeypatch.setattr(loadgen, "OPENING", {"checking": 10_000, "savings": 0})
    import random

    events = loadgen._timeline(NOW - timedelta(days=30), 30, random.Random(1))
    openings = [e for e in events if e.kind == "opening_balance"]
    assert len(openings) == 1
    assert all(e.amount > 0 for e in openings)


@pytest.mark.parametrize("days,seed", [(30, 5), (90, 1), (90, 404), (180, 17)])
def test_a_generated_history_posts_without_overdrawing(tenant, days, seed):
    """The end-to-end guarantee: every event the generator emits is one the
    ledger accepts, across day counts and seeds."""
    from ledgerflow.adapters.db import unit_of_work
    from ledgerflow.domain.ledger import AccountType
    from ledgerflow import ids

    # the fixture's chart of accounts is minimal; loadgen needs the full one
    with unit_of_work() as uow:
        existing = {
            a["external_id"] for a in uow.accounts.list(tenant["tenant_id"], "test", limit=200)
        }
        for name, type_, external, floor in loadgen_accounts():
            if external in existing:
                continue
            uow.accounts.create(
                account_id=ids.account_id(), tenant_id=tenant["tenant_id"], mode="test",
                name=name, type=AccountType(type_), currency="usd",
                external_id=external, minimum_balance=floor,
            )

    result = loadgen.generate(
        tenant_id=tenant["tenant_id"], days=days, count=5000, seed=seed
    )
    assert result["transactions"] > 0

    with read_only() as uow:
        assert uow.accounts.global_drift() == [], "debits and credits disagree"
        negative = uow.execute(
            """
            SELECT a.name, MIN(s.running)::bigint AS lowest
              FROM (SELECT e.account_id,
                           SUM(CASE WHEN e.direction = a.normal_balance
                                    THEN e.amount_minor ELSE -e.amount_minor END)
                           OVER (PARTITION BY e.account_id ORDER BY e.id) AS running
                      FROM entries e JOIN accounts a ON a.id = e.account_id) s
              JOIN accounts a ON a.id = s.account_id
             WHERE a.tenant_id = %s AND a.minimum_balance IS NOT NULL
             GROUP BY a.name HAVING MIN(s.running) < 0
            """,
            (tenant["tenant_id"],),
        )
    assert negative == [], f"an account with a floor went negative: {negative}"


def loadgen_accounts():
    from ledgerflow.cli import DEFAULT_ACCOUNTS

    return DEFAULT_ACCOUNTS
