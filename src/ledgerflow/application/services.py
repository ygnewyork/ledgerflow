"""Use cases: the operations the API and the workers both call.

Every write here runs inside a caller-supplied ``UnitOfWork``, so the caller
decides the transaction boundary and can see it. That matters more than usual
in this system: "the ledger entries, the idempotency record and the outbox row
commit together" is the correctness argument, and it is only true if nothing
opens a second transaction behind your back.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from .. import ids
from ..adapters.db import UnitOfWork
from ..domain.ledger import Direction, JournalTransaction
from ..domain.money import Money
from ..domain.posting import PostingRequest, build_transaction
from ..stream import LEDGER_EVENTS, get_stream
from . import serializers
from .errors import (
    AccountNotFound,
    AlreadyReversed,
    CurrencyMismatchError,
    InsufficientFunds,
    NotFound,
)


@dataclass(frozen=True, slots=True)
class TenantContext:
    tenant_id: str
    api_key_id: str
    mode: str  # 'test' | 'live'


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Account resolution
# ---------------------------------------------------------------------------


def resolve_account(uow: UnitOfWork, ctx: TenantContext, ref: str) -> dict[str, Any]:
    """Accept either our id or the caller's own external id.

    Developers should not have to keep a mapping table just to use your API.
    """
    row = uow.accounts.resolve(ref, ctx.tenant_id, ctx.mode)
    if row is None:
        raise AccountNotFound(f"no account {ref!r} in {ctx.mode} mode", param="account")
    return row


def create_account(
    uow: UnitOfWork,
    ctx: TenantContext,
    *,
    name: str,
    type: str,
    currency: str,
    external_id: str | None = None,
    minimum_balance: int | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    from ..domain.ledger import AccountType

    row = uow.accounts.create(
        account_id=ids.account_id(),
        tenant_id=ctx.tenant_id,
        mode=ctx.mode,
        name=name,
        type=AccountType(type),
        currency=currency,
        external_id=external_id,
        minimum_balance=minimum_balance,
        metadata=dict(metadata or {}),
    )
    return serializers.account(row)


# ---------------------------------------------------------------------------
# Posting
# ---------------------------------------------------------------------------


def _check_floors(uow: UnitOfWork, txn: JournalTransaction, account_rows: dict[str, dict[str, Any]]) -> None:
    """Enforce minimum balances for the accounts this posting decreases.

    Only accounts that declare a floor take the row lock. That keeps expense
    and revenue postings -- the overwhelming majority -- off the contended
    path entirely, while still closing the write-skew hole for the accounts
    where it matters: without ``FOR UPDATE``, two concurrent transfers can each
    read a sufficient balance and both commit, which READ COMMITTED permits.
    """
    deltas: dict[str, int] = {}
    for entry in txn.entries:
        row = account_rows[entry.account_id]
        sign = 1 if entry.direction.value == row["normal_balance"] else -1
        deltas[entry.account_id] = deltas.get(entry.account_id, 0) + sign * entry.amount.minor

    for account_id, delta in deltas.items():
        row = account_rows[account_id]
        floor = row.get("minimum_balance")
        if floor is None or delta >= 0:
            continue
        uow.accounts.lock(account_id)          # serialize this account only
        current = uow.accounts.balance(account_id)["balance_minor"]
        if current + delta < floor:
            raise InsufficientFunds(
                f"{row['name']} has {current} {row['currency']} minor units available; "
                f"this posting requires {abs(delta)}",
                param="amount",
                account=account_id,
                available=current,
                required=abs(delta),
            )


def post_transaction(
    uow: UnitOfWork,
    ctx: TenantContext,
    *,
    kind: str,
    accounts: Mapping[str, str],
    amount: Money,
    effective_at: datetime | None = None,
    descriptor: str | None = None,
    metadata: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """The write path. Everything below commits together or not at all.

      1. resolve and validate accounts
      2. build the posting (domain refuses to construct an unbalanced one)
      3. enforce balance floors
      4. insert transaction + entries (DB trigger re-checks the balance at COMMIT)
      5. keep the raw descriptor for the normalizer
      6. append the outbox row -- the event, written as data, not published

    Step 6 is the one that cannot move. Publishing after COMMIT leaves a window
    where the money moved and the event was lost, and no amount of retry logic
    in application code can close it.
    """
    effective_at = effective_at or _now()
    resolved = {role: resolve_account(uow, ctx, ref) for role, ref in accounts.items()}

    for role, row in resolved.items():
        if row["currency"] != amount.currency:
            raise CurrencyMismatchError(
                f"account {row['id']} is denominated in {row['currency']}, "
                f"but the posting is in {amount.currency}",
                param=role,
            )
        if row["status"] != "open":
            raise CurrencyMismatchError(
                f"account {row['id']} is {row['status']}", param=role
            )

    txn = build_transaction(
        ids.transaction_id(),
        kind,
        PostingRequest(
            amount=amount,
            effective_at=effective_at,
            accounts={role: row["id"] for role, row in resolved.items()},
            metadata=dict(metadata or {}),
        ),
    )

    by_id = {row["id"]: row for row in resolved.values()}
    _check_floors(uow, txn, by_id)

    txn_row = uow.transactions.insert(txn, tenant_id=ctx.tenant_id, mode=ctx.mode)
    entry_rows = uow.entries.insert_many(txn)

    if descriptor:
        # raw is kept forever and never modified; every normalized row is
        # derived from it, and re-derivable when the normalizer changes
        # reuse the row we already resolved above; looking it up again is two
        # more round trips for an answer we are holding
        role = (
            "funding" if "funding" in resolved
            else "source" if "source" in resolved
            else next(iter(resolved))
        )
        uow.normalization.insert_raw(
            raw_id=ids.new_id("raw"),
            tenant_id=ctx.tenant_id,
            mode=ctx.mode,
            account_id=resolved[role]["id"],
            transaction_id=txn.id,
            payload={"kind": kind, "descriptor": descriptor, "metadata": dict(metadata or {})},
            descriptor=descriptor,
            amount_minor=amount.minor,
            currency=amount.currency,
            occurred_at=effective_at,
        )

    resource = serializers.transaction(txn_row, entry_rows)
    emit(uow, ctx, event_type="transaction.created", partition_key=_primary_account(txn),
         payload=resource)
    return resource


def _primary_account(txn: JournalTransaction) -> str:
    """The partition key.

    Credit side first: for a purchase that is the funding account, which is the
    account whose ordering and balance anyone actually cares about.
    """
    for entry in txn.entries:
        if entry.direction is Direction.CREDIT:
            return entry.account_id
    return txn.entries[0].account_id


def emit(
    uow: UnitOfWork,
    ctx: TenantContext,
    *,
    event_type: str,
    partition_key: str,
    payload: dict[str, Any],
) -> str:
    """Append an event to the outbox, inside the caller's transaction."""
    event_id = ids.event_id()
    uow.events.append_outbox(
        event_id=event_id,
        tenant_id=ctx.tenant_id,
        mode=ctx.mode,
        partition_key=partition_key,
        event_type=event_type,
        payload={"id": event_id, "type": event_type, "data": {"object": payload}},
    )
    return event_id


def transfer(
    uow: UnitOfWork,
    ctx: TenantContext,
    *,
    source: str,
    destination: str,
    amount: Money,
    effective_at: datetime | None = None,
    metadata: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return post_transaction(
        uow, ctx,
        kind="transfer",
        accounts={"source": source, "destination": destination},
        amount=amount,
        effective_at=effective_at,
        metadata=metadata,
    )


def reverse_transaction(
    uow: UnitOfWork,
    ctx: TenantContext,
    *,
    transaction_id: str,
    effective_at: datetime | None = None,
) -> dict[str, Any]:
    """Correct a posting with a mirrored one.

    The original is never touched. A unique index on ``reverses_id`` means a
    second reversal of the same transaction is rejected by the database, not
    just by this check -- which matters, because two concurrent reversal
    requests would both pass an application-level check.
    """
    original = uow.transactions.get(transaction_id, ctx.tenant_id, ctx.mode)
    if original is None:
        raise NotFound(f"no transaction {transaction_id!r}", param="transaction")
    if original["status"] == "reversed":
        raise AlreadyReversed(f"{transaction_id} has already been reversed")

    entry_rows = uow.entries.for_transaction(transaction_id)
    domain = JournalTransaction(
        id=original["id"],
        kind=original["kind"],
        effective_at=original["effective_at"],
        entries=tuple(uow.entries.to_domain(r) for r in entry_rows),
        metadata={k: str(v) for k, v in (original.get("metadata") or {}).items()},
    )
    reversal = domain.reverse(ids.transaction_id(), effective_at=effective_at or _now())

    txn_row = uow.transactions.insert(reversal, tenant_id=ctx.tenant_id, mode=ctx.mode)
    reversal_entries = uow.entries.insert_many(reversal)
    uow.transactions.mark_reversed(transaction_id)

    resource = serializers.transaction(txn_row, reversal_entries)
    emit(uow, ctx, event_type="transaction.reversed",
         partition_key=_primary_account(reversal), payload=resource)
    return resource


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def balance(
    uow: UnitOfWork,
    ctx: TenantContext,
    *,
    account_ref: str,
    as_of: datetime | None = None,
    as_known_at: datetime | None = None,
) -> dict[str, Any]:
    row = resolve_account(uow, ctx, account_ref)
    if as_of or as_known_at:
        result = uow.accounts.balance_at(row["id"], as_of=as_of, as_known_at=as_known_at)
        basis = "historical"
    else:
        result = uow.accounts.balance(row["id"])
        basis = "settled"
    return {
        "object": "balance",
        "account": row["id"],
        "balance": result["balance_minor"],
        "currency": result["currency"],
        # say which question was answered. a balance with no stated basis is
        # the kind of number that starts an argument three months later.
        "basis": basis,
        "as_of": int(as_of.timestamp()) if as_of else None,
        "as_known_at": int(as_known_at.timestamp()) if as_known_at else None,
    }


def health(uow: UnitOfWork) -> dict[str, Any]:
    """What the dashboard's system panel shows."""
    from ..stream import LEDGER_EVENTS, NORMALIZED_TRANSACTIONS
    from ..stream import lag as stream_lag

    # each group against the topic it actually consumes; measuring risk
    # against the ledger topic reports permanent lag that is not real
    topics = {
        "normalizer": LEDGER_EVENTS,
        "webhooks": LEDGER_EVENTS,
        "risk": NORMALIZED_TRANSACTIONS,
    }

    stream = get_stream()
    outbox = uow.events.outbox_lag()
    drift = uow.accounts.global_drift()
    return {
        "object": "health",
        "outbox_pending": int(outbox["pending"]),
        "outbox_oldest_seconds": float(outbox["oldest_seconds"] or 0),
        "stream_head": stream.head(LEDGER_EVENTS),
        "consumer_lag": {
            group: stream_lag(stream, topic, group) for group, topic in topics.items()
        },
        "dlq_depth": uow.events.dlq_depth(),
        "webhooks_24h": uow.webhooks.success_rate(),
        # empty unless money was created or destroyed
        "ledger_drift": drift,
        "reconciliation_mismatches": len(uow.accounts.reconcile()),
    }
