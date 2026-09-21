"""Integration test fixtures.

These tests talk to a real PostgreSQL. That is deliberate: the deferred balance
trigger, ``ON CONFLICT DO NOTHING``, ``FOR UPDATE SKIP LOCKED`` and the
idempotency race are the things most worth testing, and none of them exist in a
mock or in SQLite.

    export LEDGERFLOW_DATABASE_URL=postgresql://localhost/ledgerflow_test
    python -m ledgerflow.cli migrate
    pytest
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

pytest.importorskip("psycopg", reason="integration tests need psycopg")

from ledgerflow import ids
from ledgerflow.adapters.db import migrate, read_only, unit_of_work
from ledgerflow.api.auth import create_key
from ledgerflow.application.services import TenantContext
from ledgerflow.domain.ledger import AccountType


def _database_reachable() -> bool:
    try:
        with read_only() as uow:
            uow.execute("SELECT 1")
        return True
    except Exception:
        return False


@pytest.fixture(scope="session", autouse=True)
def database() -> None:
    if not _database_reachable():
        where = (
            "no database at LEDGERFLOW_DATABASE_URL="
            f"{os.environ.get('LEDGERFLOW_DATABASE_URL', '(unset)')}"
        )
        # Skipping is right on a laptop with no Postgres running -- the domain
        # tests still say something useful. In CI it is a trap: every test
        # skips, pytest exits 0, and the badge goes green over a suite that
        # never ran. A misconfigured database has to be a failure there.
        if os.environ.get("CI"):
            raise RuntimeError(f"{where} (refusing to skip the suite in CI)")
        pytest.skip(where)
    migrate(str(Path(__file__).resolve().parents[1] / "migrations"))


@pytest.fixture
def tenant() -> dict:
    """A fresh tenant per test.

    Isolation by tenant rather than by truncating tables: it is closer to how
    the system really runs, and it means the tests do not quietly depend on
    starting from an empty ledger.
    """
    tenant_id = ids.new_id("ten")
    accounts: dict[str, str] = {}

    with unit_of_work() as uow:
        uow.execute("INSERT INTO tenants (id, name) VALUES (%s, %s)", (tenant_id, "Test"))
        key, key_row = create_key(uow, tenant_id=tenant_id, mode="test")
        for name, type_, external, floor in [
            ("Assets:Checking", "asset", "checking", 0),
            ("Assets:Savings", "asset", "savings", 0),
            ("Expenses:Groceries", "expense", "groceries", None),
            ("Expenses:General", "expense", "general", None),
            ("Revenue:Income", "revenue", "income", None),
        ]:
            row = uow.accounts.create(
                account_id=ids.account_id(), tenant_id=tenant_id, mode="test",
                name=name, type=AccountType(type_), currency="usd",
                external_id=external, minimum_balance=floor,
            )
            accounts[external] = row["id"]

    return {
        "tenant_id": tenant_id,
        "api_key": key,
        "api_key_id": key_row["id"],
        "accounts": accounts,
        "ctx": TenantContext(tenant_id=tenant_id, api_key_id=key_row["id"], mode="test"),
    }


@pytest.fixture
def client(tenant: dict):
    from fastapi.testclient import TestClient

    from ledgerflow.api.main import app

    test_client = TestClient(app, raise_server_exceptions=False)
    test_client.headers.update({"Authorization": f"Bearer {tenant['api_key']}"})
    return test_client


@pytest.fixture
def funded(client, tenant):
    """A checking account with money in it."""
    response = client.post(
        "/v1/transactions",
        headers={"Idempotency-Key": ids.new_id("seed")},
        json={
            "kind": "deposit", "amount": 500_000,
            "accounts": {"destination": "checking", "income": "income"},
        },
    )
    assert response.status_code == 201, response.text
    return tenant
