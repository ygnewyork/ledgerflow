"""Idempotency, including the failure that actually matters."""

from __future__ import annotations

import pytest

from ledgerflow import ids
from ledgerflow.adapters.db import read_only
from ledgerflow.api.idempotency import fingerprint, idempotent
from ledgerflow.api.schemas import CreateTransfer
from ledgerflow.application import services
from ledgerflow.domain.money import Money


def _count_transactions(tenant_id: str) -> int:
    with read_only() as uow:
        return len(uow.transactions.list(tenant_id=tenant_id, mode="test", limit=100))


class SimulatedCrash(Exception):
    """The process dying between COMMIT and the response reaching the client."""


def test_crash_between_commit_and_response(funded, client):
    """The whole reason the idempotency row lives in the work's transaction.

        request -> work COMMITs -> process dies -> client retries

    The write is durable and the client never learned that. The retry must
    return the original result and must NOT move the money a second time.

    Driven through ``idempotent()`` directly rather than through HTTP, because
    the precise instant being simulated -- after COMMIT, before the response is
    produced -- is the boundary of that context manager. Going through the
    framework would test where the framework puts its own error handling, not
    where the idempotency record is written.
    """
    ctx = funded["ctx"]
    key = "transfer_9384abc"
    body = {"source": "checking", "destination": "savings", "amount": 84_37}
    # The handler fingerprints the VALIDATED model, not the raw body, so that a
    # retry omitting `currency` matches one that sends "usd" explicitly. The
    # test has to hash the same thing the handler will.
    fp = fingerprint("POST", "/v1/transfers", CreateTransfer(**body).model_dump(mode="json"))

    before = _count_transactions(ctx.tenant_id)

    with pytest.raises(SimulatedCrash):
        with idempotent(ctx, key, fp) as slot:
            result = services.transfer(
                slot.uow, ctx, source="checking", destination="savings",
                amount=Money(8437, "usd"),
            )
            slot.complete(201, result, result["id"])
        # <-- COMMIT happened on the line above. the process dies here.
        raise SimulatedCrash("killed before the response was written")

    # the work is durable: the money moved exactly once
    assert _count_transactions(ctx.tenant_id) == before + 1

    # the client, having seen nothing, retries with the same key
    retry = client.post("/v1/transfers", headers={"Idempotency-Key": key}, json=body)

    assert retry.status_code == 201
    assert retry.json()["id"] == result["id"], "retry returned a different transaction"
    assert retry.headers.get("Idempotent-Replay") == "true"
    assert _count_transactions(ctx.tenant_id) == before + 1, "the retry moved the money again"

    balance = client.get("/v1/accounts/savings/balance").json()
    assert balance["balance"] == 8437, "savings should have received exactly one transfer"


def test_retry_replays_the_stored_response(funded, client):
    body = {"kind": "card_purchase", "amount": 1299,
            "accounts": {"expense": "groceries", "funding": "checking"}}
    first = client.post("/v1/transactions", headers={"Idempotency-Key": "p1"}, json=body)
    second = client.post("/v1/transactions", headers={"Idempotency-Key": "p1"}, json=body)

    assert first.status_code == second.status_code == 201
    assert first.json() == second.json()
    assert second.headers.get("Idempotent-Replay") == "true"
    assert first.headers.get("Idempotent-Replay") is None


def test_same_key_different_body_is_a_conflict(funded, client):
    client.post("/v1/transactions", headers={"Idempotency-Key": "p2"}, json={
        "kind": "card_purchase", "amount": 1299,
        "accounts": {"expense": "groceries", "funding": "checking"}})
    clash = client.post("/v1/transactions", headers={"Idempotency-Key": "p2"}, json={
        "kind": "card_purchase", "amount": 999_99,
        "accounts": {"expense": "groceries", "funding": "checking"}})

    # replaying the first response here would hide a real client bug
    assert clash.status_code == 409
    assert clash.json()["error"]["code"] == "idempotency_key_reuse"


def test_key_reordering_is_still_the_same_request(funded, client):
    """The fingerprint covers meaning, not byte order."""
    a = client.post("/v1/transfers", headers={"Idempotency-Key": "p3"},
                    json={"source": "checking", "destination": "savings", "amount": 100})
    b = client.post("/v1/transfers", headers={"Idempotency-Key": "p3"},
                    json={"amount": 100, "destination": "savings", "source": "checking"})
    assert b.status_code == 201
    assert a.json()["id"] == b.json()["id"]


def test_concurrent_duplicate_waits_then_replays(funded):
    """A duplicate arriving mid-flight blocks, then replays -- it never re-executes.

    Because the claim commits with the work, the second request cannot see an
    'in_progress' row; it blocks on the uncommitted INSERT. When the first
    transaction commits, the waiter conflicts, reads a completed row, and
    replays the stored response.

    Run in a thread so the block is real rather than simulated.
    """
    import threading

    ctx = funded["ctx"]
    key = "concurrent_1"
    fp = fingerprint("POST", "/v1/transfers", {"amount": 100})
    outcome: dict = {}
    second_started = threading.Event()

    def duplicate() -> None:
        second_started.set()
        try:
            with idempotent(ctx, key, fp) as slot:
                outcome["replayed"] = slot.replayed
                outcome["response"] = slot.response
                if not slot.replayed:
                    outcome["executed"] = True
        except Exception as exc:  # noqa: BLE001
            outcome["error"] = type(exc).__name__

    with idempotent(ctx, key, fp) as slot:
        result = services.transfer(
            slot.uow, ctx, source="checking", destination="savings",
            amount=Money(100, "usd"),
        )
        slot.complete(201, result, result["id"])

        thread = threading.Thread(target=duplicate)
        thread.start()
        second_started.wait(timeout=2)
        # the duplicate is now blocked on our uncommitted row
        thread.join(timeout=0.5)
        assert thread.is_alive(), "the duplicate should be blocked, not executing"

    thread.join(timeout=10)
    assert not thread.is_alive(), "the duplicate never unblocked after COMMIT"
    assert outcome.get("replayed") is True, f"duplicate did not replay: {outcome}"
    assert "executed" not in outcome, "the duplicate executed the transfer a second time"
    assert outcome["response"]["id"] == result["id"]


def test_lock_timeout_turns_an_unbounded_wait_into_a_409(funded, monkeypatch):
    """A duplicate behind a slow request gets 409 rather than hanging forever."""
    import threading

    import dataclasses

    from ledgerflow.application.errors import RequestInFlight
    from ledgerflow.config import settings

    # Settings is frozen on purpose -- configuration that mutates at runtime is
    # a debugging nightmare -- so swap the module's reference, not the field.
    monkeypatch.setattr(
        "ledgerflow.api.idempotency.settings",
        dataclasses.replace(settings, idempotency_lock_timeout_ms=250),
    )

    ctx = funded["ctx"]
    key = "slow_1"
    fp = fingerprint("POST", "/v1/transfers", {"amount": 1})
    outcome: dict = {}

    def duplicate() -> None:
        try:
            with idempotent(ctx, key, fp):
                outcome["granted"] = True
        except RequestInFlight:
            outcome["rejected"] = True
        except Exception as exc:  # noqa: BLE001
            outcome["error"] = f"{type(exc).__name__}: {exc}"

    with idempotent(ctx, key, fp) as slot:
        result = services.transfer(
            slot.uow, ctx, source="checking", destination="savings",
            amount=Money(100, "usd"),
        )
        slot.complete(201, result, result["id"])

        thread = threading.Thread(target=duplicate)
        thread.start()
        thread.join(timeout=8)

    assert outcome.get("rejected") is True, f"expected a bounded 409, got {outcome}"


def test_keys_are_scoped_per_api_key(funded, client):
    """Two tenants using 'transfer_1' must not collide."""
    from ledgerflow.adapters.db import unit_of_work
    from ledgerflow.api.auth import create_key

    with unit_of_work() as uow:
        other_tenant = ids.new_id("ten")
        uow.execute("INSERT INTO tenants (id, name) VALUES (%s, %s)", (other_tenant, "Other"))
        other_key, _ = create_key(uow, tenant_id=other_tenant, mode="test")
        uow.accounts.create(
            account_id=ids.account_id(), tenant_id=other_tenant, mode="test",
            name="Assets:Checking", type=__import__(
                "ledgerflow.domain.ledger", fromlist=["AccountType"]
            ).AccountType("asset"),
            currency="usd", external_id="checking", minimum_balance=None,
        )
        uow.accounts.create(
            account_id=ids.account_id(), tenant_id=other_tenant, mode="test",
            name="Revenue:Income", type=__import__(
                "ledgerflow.domain.ledger", fromlist=["AccountType"]
            ).AccountType("revenue"),
            currency="usd", external_id="income", minimum_balance=None,
        )

    body = {"kind": "deposit", "amount": 5000,
            "accounts": {"destination": "checking", "income": "income"}}
    mine = client.post("/v1/transactions", headers={"Idempotency-Key": "shared"}, json=body)
    theirs = client.post(
        "/v1/transactions",
        headers={"Idempotency-Key": "shared", "Authorization": f"Bearer {other_key}"},
        json=body,
    )

    assert mine.status_code == theirs.status_code == 201
    assert mine.json()["id"] != theirs.json()["id"], "keys leaked across tenants"
