"""Operator entry points.

    python -m ledgerflow.cli migrate
    python -m ledgerflow.cli bootstrap          # tenant + api keys + chart of accounts
    python -m ledgerflow.cli serve
    python -m ledgerflow.cli worker <name>      # relay | normalizer | risk | webhooks | all
    python -m ledgerflow.cli loadgen --count 5000
    python -m ledgerflow.cli reconcile
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from . import ids
from .adapters.db import migrate, read_only, unit_of_work
from .application.services import TenantContext


DEFAULT_ACCOUNTS = [
    ("Assets:Checking", "asset", "checking", 0),
    ("Assets:Savings", "asset", "savings", 0),
    ("Liabilities:Card", "liability", "card", None),
    ("Expenses:Groceries", "expense", "groceries", None),
    ("Expenses:Food and Drink", "expense", "food", None),
    ("Expenses:Shopping", "expense", "shopping", None),
    ("Expenses:Transport", "expense", "transport", None),
    ("Expenses:Entertainment", "expense", "entertainment", None),
    ("Expenses:Travel", "expense", "travel", None),
    ("Expenses:Health", "expense", "health", None),
    ("Expenses:Bills", "expense", "bills", None),
    ("Expenses:General", "expense", "general", None),
    ("Expenses:Fees", "expense", "fees", None),
    ("Revenue:Income", "revenue", "income", None),
]


def cmd_migrate(args: argparse.Namespace) -> None:
    applied = migrate(args.directory)
    print(f"applied: {applied}" if applied else "already up to date")


def cmd_bootstrap(args: argparse.Namespace) -> dict[str, Any]:
    from .api.auth import create_key
    from .application import services
    from .domain.ledger import AccountType

    tenant_id = args.tenant or ids.new_id("ten")
    out: dict[str, Any] = {"tenant_id": tenant_id, "keys": {}, "accounts": {}}

    with unit_of_work() as uow:
        uow.execute(
            "INSERT INTO tenants (id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (tenant_id, args.name),
        )
        for mode in ("test", "live"):
            plaintext, _ = create_key(uow, tenant_id=tenant_id, mode=mode)
            out["keys"][mode] = plaintext

        # the chart of accounts exists in both modes; test and live never share rows
        for mode in ("test", "live"):
            ctx = TenantContext(tenant_id=tenant_id, api_key_id="bootstrap", mode=mode)
            for name, type_, external, floor in DEFAULT_ACCOUNTS:
                row = uow.accounts.create(
                    account_id=ids.account_id(),
                    tenant_id=tenant_id,
                    mode=mode,
                    name=name,
                    type=AccountType(type_),
                    currency="usd",
                    external_id=external,
                    minimum_balance=floor,
                )
                if mode == "test":
                    out["accounts"][external] = row["id"]

    print(json.dumps(out, indent=2))
    return out


def cmd_serve(args: argparse.Namespace) -> None:
    import uvicorn

    uvicorn.run(
        "ledgerflow.api.main:app", host=args.host, port=args.port, reload=args.reload,
        log_level="info",
    )


def cmd_worker(args: argparse.Namespace) -> None:
    from .workers import run_worker

    run_worker(args.name, once=args.once)


def cmd_loadgen(args: argparse.Namespace) -> None:
    from .loadgen import generate

    generate(tenant_id=args.tenant, count=args.count, days=args.days, seed=args.seed)


def cmd_reconcile(args: argparse.Namespace) -> None:
    with read_only() as uow:
        mismatches = uow.accounts.reconcile()
        drift = uow.accounts.global_drift()
    print(json.dumps({"mismatches": mismatches, "ledger_drift": drift,
                      "ok": not mismatches and not drift}, indent=2, default=str))
    if mismatches or drift:
        sys.exit(1)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="ledgerflow")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("migrate"); p.add_argument("--directory", default="migrations")
    p.set_defaults(func=cmd_migrate)

    p = sub.add_parser("bootstrap")
    p.add_argument("--tenant"); p.add_argument("--name", default="Demo Tenant")
    p.set_defaults(func=cmd_bootstrap)

    p = sub.add_parser("serve")
    p.add_argument("--host", default="127.0.0.1"); p.add_argument("--port", type=int, default=8000)
    p.add_argument("--reload", action="store_true")
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("worker")
    p.add_argument("name", choices=["relay", "normalizer", "risk", "webhooks", "all"])
    p.add_argument("--once", action="store_true", help="drain and exit, for tests and demos")
    p.set_defaults(func=cmd_worker)

    p = sub.add_parser("loadgen")
    p.add_argument("--tenant", required=True); p.add_argument("--count", type=int, default=1000)
    p.add_argument("--days", type=int, default=90); p.add_argument("--seed", type=int, default=17)
    p.set_defaults(func=cmd_loadgen)

    p = sub.add_parser("reconcile"); p.set_defaults(func=cmd_reconcile)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
