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

# A personal chart of accounts, in the five categories double-entry defines.
# The shape is the teaching tool: money arrives as revenue, sits in assets,
# leaves as expenses, and a credit card is a liability that grows when you
# spend and shrinks when you pay it -- which is why a card purchase CREDITS
# the card while a checking purchase CREDITS checking, and both are correct.
#
#   (display name, type, external id, minimum balance)
#
# A minimum balance of 0 means the account may not go negative, which is what
# makes it take a row lock on the write path. Expense and revenue accounts have
# no floor -- spending is unbounded by definition -- so they never contend.
DEFAULT_ACCOUNTS = [
    # --- assets: what you have -------------------------------------------
    ("Assets:Checking",         "asset",     "checking",      0),
    ("Assets:Savings",          "asset",     "savings",       0),
    ("Assets:Investments",      "asset",     "investments",   0),
    ("Assets:Cash",             "asset",     "cash",          0),

    # --- liabilities: what you owe ---------------------------------------
    # No floor: a credit card balance is *supposed* to go up when you spend.
    ("Liabilities:Credit Card", "liability", "card",          None),

    # --- equity: what the accounts were worth on day one -----------------
    # Opening balances are not income; they are the starting position. Booking
    # them to equity is what keeps "revenue" meaning money actually earned
    # during the period, which is what makes an income statement legible.
    ("Equity:Opening Balances", "equity",    "opening",       None),

    # --- revenue: where money comes from ---------------------------------
    ("Revenue:Income",          "revenue",   "income",        None),
    ("Revenue:Interest",        "revenue",   "interest",      None),
    ("Revenue:Refunds",         "revenue",   "refunds",       None),
    # Mark-to-market gains. An investment account that only ever equals what
    # you put into it is a savings account with extra steps -- the whole point
    # is that its value moves independently of your contributions.
    ("Revenue:Investment Gains","revenue",   "gains",         None),

    # --- expenses: where it goes -----------------------------------------
    ("Expenses:Rent",           "expense",   "rent",          None),
    ("Expenses:Groceries",      "expense",   "groceries",     None),
    ("Expenses:Food and Drink", "expense",   "food",          None),
    ("Expenses:Transport",      "expense",   "transport",     None),
    ("Expenses:Shopping",       "expense",   "shopping",      None),
    ("Expenses:Entertainment",  "expense",   "entertainment", None),
    ("Expenses:Subscriptions",  "expense",   "subscriptions", None),
    ("Expenses:Bills",          "expense",   "bills",         None),
    ("Expenses:Health",         "expense",   "health",        None),
    ("Expenses:Travel",         "expense",   "travel",        None),
    ("Expenses:Education",      "expense",   "education",     None),
    ("Expenses:General",        "expense",   "general",       None),
    ("Expenses:Fees",           "expense",   "fees",          None),
    ("Expenses:Investment Losses", "expense", "losses",        None),
]


def cmd_migrate(args: argparse.Namespace) -> None:
    applied = migrate(args.directory)
    print(f"applied: {applied}" if applied else "already up to date")


def cmd_bootstrap(args: argparse.Namespace) -> dict[str, Any]:
    """Create the demo tenant, or bring an existing one up to date.

    Idempotent on purpose. Minting a brand-new tenant on every run is what made
    the dashboard silently show stale data: the browser caches an API key, keys
    are scoped to a tenant, and a new tenant means the cached key still works
    and still points at last week's ledger. Nothing errors -- you just quietly
    read the wrong books.

    Reusing the tenant means a key issued months ago keeps working and sees
    current data, and accounts added since (investments, the credit card,
    equity) are backfilled rather than missing.
    """
    from .api.auth import create_key
    from .domain.ledger import AccountType

    out: dict[str, Any] = {"keys": {}, "accounts": {}}

    with unit_of_work() as uow:
        existing = None
        if args.tenant:
            existing = uow.one("SELECT * FROM tenants WHERE id = %s", (args.tenant,))
        else:
            existing = uow.one(
                "SELECT * FROM tenants WHERE name = %s ORDER BY created_at DESC LIMIT 1",
                (args.name,),
            )

        if existing:
            tenant_id = existing["id"]
            out["reused"] = True
        else:
            tenant_id = args.tenant or ids.new_id("ten")
            uow.execute(
                "INSERT INTO tenants (id, name) VALUES (%s, %s)", (tenant_id, args.name)
            )
            out["reused"] = False
        out["tenant_id"] = tenant_id

        for mode in ("test", "live"):
            plaintext, _ = create_key(uow, tenant_id=tenant_id, mode=mode)
            out["keys"][mode] = plaintext

        # add only what is missing, in both modes; test and live never share rows
        created = 0
        for mode in ("test", "live"):
            have = {
                a["external_id"]
                for a in uow.accounts.list(tenant_id, mode, limit=500)
                if a["external_id"]
            }
            for name, type_, external, floor in DEFAULT_ACCOUNTS:
                if external in have:
                    if mode == "test":
                        row = uow.accounts.get_by_external(external, tenant_id, mode)
                        out["accounts"][external] = row["id"]
                    continue
                row = uow.accounts.create(
                    account_id=ids.account_id(), tenant_id=tenant_id, mode=mode,
                    name=name, type=AccountType(type_), currency="usd",
                    external_id=external, minimum_balance=floor,
                )
                created += 1
                if mode == "test":
                    out["accounts"][external] = row["id"]
        out["accounts_added"] = created

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


def cmd_reset(args: argparse.Namespace) -> None:
    """Delete a tenant's ledger data so a fresh history can be generated.

    This has to switch off the append-only trigger on `entries`, which exists
    precisely to stop anyone doing this. That is the right protection for a
    ledger and the wrong one for a demo you want to regenerate, so the bypass
    is here, in a command that names what it is, rather than weakened in the
    schema where it guards real money.

    session_replication_role is transaction-scoped: the trigger is live again
    the moment this commits, whether or not it commits cleanly.
    """
    tenant_id = args.tenant or _default_tenant()
    with unit_of_work() as uow:
        uow.execute("SET LOCAL session_replication_role = replica")
        counts = {}
        for table, sql in [
            ("fraud_signals", "DELETE FROM fraud_signals WHERE tenant_id = %s"),
            ("normalized_transactions",
             "DELETE FROM normalized_transactions WHERE tenant_id = %s"),
            ("entries",
             "DELETE FROM entries WHERE account_id IN "
             "(SELECT id FROM accounts WHERE tenant_id = %s)"),
            ("raw_transactions", "DELETE FROM raw_transactions WHERE tenant_id = %s"),
            ("balance_snapshots",
             "DELETE FROM balance_snapshots WHERE account_id IN "
             "(SELECT id FROM accounts WHERE tenant_id = %s)"),
            ("transactions", "DELETE FROM transactions WHERE tenant_id = %s"),
            ("outbox", "DELETE FROM outbox WHERE tenant_id = %s"),
            ("idempotency_keys", "DELETE FROM idempotency_keys WHERE tenant_id = %s"),
        ]:
            rows = uow.execute(sql + " RETURNING 1", (tenant_id,))
            counts[table] = len(rows)
        # consumers must forget what they have seen, or a replayed event is
        # skipped as a duplicate and the derived tables never rebuild
        uow.execute("TRUNCATE processed_events")
        uow.execute("DELETE FROM stream_messages")
        uow.execute("DELETE FROM consumer_offsets")

    print(f"reset {tenant_id}: " + ", ".join(f"{k}={v}" for k, v in counts.items() if v))


def _default_tenant() -> str:
    """The newest tenant with a complete chart of accounts.

    Newest-overall is the obvious rule and the wrong one: a test run leaves
    tenants holding a handful of accounts, and loadgen then fails deep inside a
    posting with "no account 'food'". Requiring the full chart picks a
    bootstrapped tenant, and names it so the choice is visible.
    """
    required = [external for _, _, external, _ in DEFAULT_ACCOUNTS]
    with read_only() as uow:
        row = uow.one(
            """
            SELECT t.id, t.name
              FROM tenants t
             WHERE (SELECT count(*) FROM accounts a
                     WHERE a.tenant_id = t.id AND a.mode = 'test'
                       AND a.external_id = ANY(%s)) = %s
             ORDER BY t.created_at DESC
             LIMIT 1
            """,
            (required, len(required)),
        )
    if row is None:
        raise SystemExit(
            "no bootstrapped tenant found -- run `python -m ledgerflow.cli bootstrap` "
            "first, or pass --tenant explicitly"
        )
    print(f"using tenant {row['id']} ({row['name']})")
    return row["id"]


def cmd_loadgen(args: argparse.Namespace) -> None:
    from .loadgen import generate

    generate(
        tenant_id=args.tenant or _default_tenant(),
        count=args.count, days=args.days, seed=args.seed,
    )


def cmd_snapshot(args: argparse.Namespace) -> None:
    """Fold each account's entry tail into a balance snapshot.

    Without this the snapshot table stays empty and every balance read scans
    the account's whole history -- measurably linear: ~0.5 ms at 600 entries,
    ~15 ms at 35,000. The design always called for a background job; this is it.
    """
    written = 0
    with unit_of_work() as uow:
        stale = uow.execute(
            """
            SELECT a.id, count(e.id)::int AS tail
              FROM accounts a
              JOIN entries e ON e.account_id = a.id
             WHERE e.id > COALESCE((
                     SELECT max(up_to_entry_id) FROM balance_snapshots s
                      WHERE s.account_id = a.id), 0)
             GROUP BY a.id
            HAVING count(e.id) >= %s
            """,
            (args.threshold,),
        )
        for row in stale:
            if uow.accounts.write_snapshot(row["id"]):
                written += 1
    print(f"wrote {written} snapshot(s) (threshold {args.threshold} entries)")


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

    p = sub.add_parser("migrate")
    p.add_argument("--directory", default="migrations")
    p.set_defaults(func=cmd_migrate)

    p = sub.add_parser("bootstrap")
    p.add_argument("--tenant")
    p.add_argument("--name", default="Demo Tenant")
    p.set_defaults(func=cmd_bootstrap)

    p = sub.add_parser("serve")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--reload", action="store_true")
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("worker")
    p.add_argument("name", choices=["relay", "normalizer", "risk", "webhooks", "all"])
    p.add_argument("--once", action="store_true", help="drain and exit, for tests and demos")
    p.set_defaults(func=cmd_worker)

    p = sub.add_parser("loadgen")
    p.add_argument("--tenant", help="defaults to the most recent bootstrapped tenant")
    p.add_argument("--count", type=int, default=1000)
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--seed", type=int, default=17)
    p.set_defaults(func=cmd_loadgen)

    p = sub.add_parser("reset")
    p.add_argument("--tenant", help="defaults to the most recent bootstrapped tenant")
    p.set_defaults(func=cmd_reset)

    p = sub.add_parser("snapshot")
    p.add_argument("--threshold", type=int, default=500,
                   help="snapshot accounts with at least this many uncached entries")
    p.set_defaults(func=cmd_snapshot)

    p = sub.add_parser("reconcile")
    p.set_defaults(func=cmd_reconcile)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
