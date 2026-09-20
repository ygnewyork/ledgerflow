"""The v1 resource endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, Query, Request, Response

from ... import ids
from ...adapters.db import read_only
from ...application import serializers, services
from ...application.errors import NotFound
from ...application.services import TenantContext
from ...domain.money import Money, MoneyError
from ...stream import LEDGER_EVENTS, get_stream
from ..deps import context, parse_timestamp
from ..idempotency import fingerprint, idempotent
from ..schemas import (
    CreateAccount,
    CreateReplay,
    CreateReversal,
    CreateTransaction,
    CreateTransfer,
    CreateWebhookEndpoint,
)

router = APIRouter(prefix="/v1")

# These handlers are sync `def`, not `async def`, and that is deliberate.
#
# psycopg is a blocking driver. FastAPI runs an `async def` handler directly on
# the event loop, so a blocking query inside one stalls every other request in
# the process -- the server stops being concurrent at all, and throughput
# flattens no matter how many clients you point at it. A plain `def` handler is
# dispatched to the threadpool instead, where blocking is exactly what is
# expected.
#
# Measured honestly: on this workload the conversion did NOT change throughput,
# because commit durability -- not the event loop -- is what binds here. It is
# kept because the mixture is a latent hazard: the moment one query gets slow,
# an async handler holding a blocking driver stalls every other request in the
# process. The alternative is an async driver end to end; the mistake is mixing
# them and hoping.


def _money(amount: int, currency: str) -> Money:
    try:
        return Money(amount, currency)
    except MoneyError as exc:
        from ...application.errors import LedgerFlowError

        raise LedgerFlowError(str(exc), param="currency") from exc


# ---------------------------------------------------------------------------
# Accounts
# ---------------------------------------------------------------------------


@router.post("/accounts", status_code=201)
def create_account(
    body: CreateAccount,
    request: Request,
    ctx: TenantContext = Depends(context),
    idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    fp = fingerprint("POST", "/v1/accounts", body.model_dump(mode="json"))
    with idempotent(ctx, idempotency_key, fp) as slot:
        if slot.replayed:
            request.state.idempotent_replay = True
            return slot.response
        result = services.create_account(
            slot.uow, ctx,
            name=body.name, type=body.type, currency=body.currency,
            external_id=body.external_id, minimum_balance=body.minimum_balance,
            metadata=body.metadata,
        )
        slot.complete(201, result, result["id"])
    return result


@router.get("/accounts")
def list_accounts(
    ctx: TenantContext = Depends(context), limit: int = Query(25, ge=1, le=100)
) -> dict[str, Any]:
    with read_only() as uow:
        rows = uow.accounts.list(ctx.tenant_id, ctx.mode, limit=limit)
        return serializers.listing([
            serializers.account(r, uow.accounts.balance(r["id"])) for r in rows
        ])


@router.get("/accounts/{account_id}")
def get_account(account_id: str, ctx: TenantContext = Depends(context)) -> dict[str, Any]:
    with read_only() as uow:
        row = services.resolve_account(uow, ctx, account_id)
        return serializers.account(row, uow.accounts.balance(row["id"]))


@router.get("/accounts/{account_id}/balance")
def get_balance(
    account_id: str,
    ctx: TenantContext = Depends(context),
    as_of: str | None = Query(None, description="business time: when the money moved"),
    as_known_at: str | None = Query(None, description="system time: what we knew then"),
) -> dict[str, Any]:
    with read_only() as uow:
        return services.balance(
            uow, ctx,
            account_ref=account_id,
            as_of=parse_timestamp(as_of),
            as_known_at=parse_timestamp(as_known_at),
        )


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


@router.post("/transactions", status_code=201)
def create_transaction(
    body: CreateTransaction,
    request: Request,
    ctx: TenantContext = Depends(context),
    idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    fp = fingerprint("POST", "/v1/transactions", body.model_dump(mode="json"))
    with idempotent(ctx, idempotency_key, fp) as slot:
        if slot.replayed:
            request.state.idempotent_replay = True
            return slot.response
        result = services.post_transaction(
            slot.uow, ctx,
            kind=body.kind,
            accounts=body.accounts,
            amount=_money(body.amount, body.currency),
            effective_at=body.effective_at,
            descriptor=body.descriptor,
            metadata=body.metadata,
        )
        slot.complete(201, result, result["id"])
    return result


@router.get("/transactions")
def list_transactions(
    ctx: TenantContext = Depends(context),
    account: str | None = None,
    category: str | None = Query(None, description="resolved category, or 'Uncategorized'"),
    starting_after: str | None = None,
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    with read_only() as uow:
        account_id = services.resolve_account(uow, ctx, account)["id"] if account else None
        # fetch one extra to answer has_more without a second count query
        rows = uow.transactions.list(
            tenant_id=ctx.tenant_id, mode=ctx.mode, account_id=account_id,
            category=category, starting_after=starting_after, limit=limit + 1,
        )
        has_more = len(rows) > limit
        rows = rows[:limit]
        return serializers.listing(
            [serializers.transaction(r, uow.entries.for_transaction(r["id"])) for r in rows],
            has_more=has_more,
        )


@router.get("/transactions/{transaction_id}")
def get_transaction(
    transaction_id: str, ctx: TenantContext = Depends(context)
) -> dict[str, Any]:
    with read_only() as uow:
        row = uow.transactions.get(transaction_id, ctx.tenant_id, ctx.mode)
        if row is None:
            raise NotFound(f"no transaction {transaction_id!r}")
        return serializers.transaction(row, uow.entries.for_transaction(row["id"]))


@router.post("/transactions/{transaction_id}/reversals", status_code=201)
def reverse_transaction(
    transaction_id: str,
    body: CreateReversal,
    request: Request,
    ctx: TenantContext = Depends(context),
    idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    fp = fingerprint(
        "POST", f"/v1/transactions/{transaction_id}/reversals", body.model_dump(mode="json")
    )
    with idempotent(ctx, idempotency_key, fp) as slot:
        if slot.replayed:
            request.state.idempotent_replay = True
            return slot.response
        result = services.reverse_transaction(
            slot.uow, ctx, transaction_id=transaction_id, effective_at=body.effective_at
        )
        slot.complete(201, result, result["id"])
    return result


@router.post("/transfers", status_code=201)
def create_transfer(
    body: CreateTransfer,
    request: Request,
    ctx: TenantContext = Depends(context),
    idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    fp = fingerprint("POST", "/v1/transfers", body.model_dump(mode="json"))
    with idempotent(ctx, idempotency_key, fp) as slot:
        if slot.replayed:
            request.state.idempotent_replay = True
            return slot.response
        result = services.transfer(
            slot.uow, ctx,
            source=body.source, destination=body.destination,
            amount=_money(body.amount, body.currency),
            effective_at=body.effective_at, metadata=body.metadata,
        )
        slot.complete(201, result, result["id"])
    return result


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------


@router.get("/ledger/entries")
def list_entries(
    ctx: TenantContext = Depends(context),
    account: str | None = None,
    since_entry_id: int = 0,
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    with read_only() as uow:
        account_id = services.resolve_account(uow, ctx, account)["id"] if account else None
        rows = uow.entries.list(
            account_id=account_id, since_entry_id=since_entry_id, limit=limit
        )
        return serializers.listing([serializers.entry(r) for r in rows])


# ---------------------------------------------------------------------------
# Events, replays, dead letters
# ---------------------------------------------------------------------------


@router.get("/events")
def list_events(
    ctx: TenantContext = Depends(context),
    starting_after: str | None = None,
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    with read_only() as uow:
        rows = uow.events.list(
            tenant_id=ctx.tenant_id, mode=ctx.mode,
            starting_after=starting_after, limit=limit,
        )
        return serializers.listing([serializers.event(r) for r in rows])


@router.get("/events/{event_id}")
def get_event(event_id: str, ctx: TenantContext = Depends(context)) -> dict[str, Any]:
    with read_only() as uow:
        row = uow.events.get(event_id, ctx.tenant_id, ctx.mode)
        if row is None:
            raise NotFound(f"no event {event_id!r}")
        return serializers.event(row)


@router.post("/events/{event_id}/redeliver", status_code=202)
def redeliver_event(event_id: str, ctx: TenantContext = Depends(context)) -> dict[str, Any]:
    """Re-send a webhook. Receivers dedupe on event id, so this is safe."""
    from ...adapters.db import unit_of_work

    with unit_of_work() as uow:
        event = uow.events.get(event_id, ctx.tenant_id, ctx.mode)
        if event is None:
            raise NotFound(f"no event {event_id!r}")
        endpoints = uow.webhooks.endpoints_for(ctx.tenant_id, ctx.mode, event["event_type"])
        queued = [
            serializers.webhook_delivery(
                uow.webhooks.enqueue(
                    delivery_id=ids.new_id("whd"), endpoint_id=e["id"], event_id=event_id
                )
            )
            for e in endpoints
        ]
    return serializers.listing(queued)


@router.post("/replays", status_code=202)
def create_replay(body: CreateReplay, ctx: TenantContext = Depends(context)) -> dict[str, Any]:
    """Rewind a consumer group over a range of the stream.

    Distinct from redelivering a webhook: this reprocesses history and can
    rewrite a lot of derived state, which is why it is recorded as an auditable
    row rather than an ad-hoc offset UPDATE.
    """
    from ...adapters.db import unit_of_work

    stream = get_stream()
    replay_id = ids.new_id("rep")
    head = stream.head(body.topic)
    with unit_of_work() as uow:
        uow.execute(
            "INSERT INTO replays (id, consumer_group, topic, from_offset, to_offset, reason) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (replay_id, body.consumer_group, body.topic, body.from_offset, head, body.reason),
        )
        # clear the dedupe claims in the range, or the consumer will correctly
        # skip every event as already processed
        uow.execute(
            """
            DELETE FROM processed_events
             WHERE consumer_group = %s
               AND event_id IN (SELECT event_id FROM stream_messages
                                 WHERE topic = %s AND offset_id > %s AND offset_id <= %s)
            """,
            (body.consumer_group, body.topic, body.from_offset, head),
        )
    stream.seek(topic=body.topic, consumer_group=body.consumer_group, offset=body.from_offset)
    return {
        "id": replay_id, "object": "replay", "consumer_group": body.consumer_group,
        "topic": body.topic, "from_offset": body.from_offset, "to_offset": head,
        "reason": body.reason,
    }


@router.get("/dead_letters")
def list_dead_letters(
    ctx: TenantContext = Depends(context),
    consumer: str | None = None,
    unresolved: bool = True,
    limit: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    with read_only() as uow:
        rows = uow.events.list_dead_letters(
            consumer_group=consumer, unresolved=unresolved, limit=limit
        )
        return serializers.listing([serializers.dead_letter(r) for r in rows])


@router.post("/dead_letters/{dlq_id}/redrive", status_code=202)
def redrive(dlq_id: str, ctx: TenantContext = Depends(context)) -> dict[str, Any]:
    from ...adapters.db import unit_of_work

    with unit_of_work() as uow:
        row = uow.events.resolve_dead_letter(dlq_id)
        if row is None:
            raise NotFound(f"no unresolved dead letter {dlq_id!r}")
        # drop the claim so the consumer will pick it up again
        uow.events.release_claim(row["consumer_group"], row["event_id"])
        return serializers.dead_letter(row)


# ---------------------------------------------------------------------------
# Webhooks
# ---------------------------------------------------------------------------


@router.post("/webhook_endpoints", status_code=201)
def create_webhook_endpoint(
    body: CreateWebhookEndpoint, ctx: TenantContext = Depends(context)
) -> dict[str, Any]:
    import hashlib
    import secrets

    from ...adapters.db import unit_of_work

    secret = f"whsec_{secrets.token_urlsafe(24)}"
    with unit_of_work() as uow:
        row = uow.webhooks.create_endpoint(
            endpoint_id=ids.new_id("whe"),
            tenant_id=ctx.tenant_id,
            mode=ctx.mode,
            url=body.url,
            secret_hash=hashlib.sha256(secret.encode()).digest(),
            enabled_events=body.enabled_events,
        )
    out = serializers.webhook_endpoint(row)
    # shown exactly once, like the API key
    out["secret"] = secret
    return out


@router.get("/webhook_endpoints/{endpoint_id}/deliveries")
def list_deliveries(
    endpoint_id: str, ctx: TenantContext = Depends(context), limit: int = Query(50, ge=1, le=200)
) -> dict[str, Any]:
    with read_only() as uow:
        endpoint = uow.webhooks.get_endpoint(endpoint_id)
        if endpoint is None or endpoint["tenant_id"] != ctx.tenant_id:
            raise NotFound(f"no webhook endpoint {endpoint_id!r}")
        rows = uow.webhooks.deliveries_for(endpoint_id, limit=limit)
        return serializers.listing([serializers.webhook_delivery(r) for r in rows])


@router.post("/webhook_deliveries/{delivery_id}/retry", status_code=202)
def retry_delivery(delivery_id: str, ctx: TenantContext = Depends(context)) -> dict[str, Any]:
    from ...adapters.db import unit_of_work

    with unit_of_work() as uow:
        row = uow.webhooks.retry(delivery_id)
        if row is None:
            raise NotFound(f"no webhook delivery {delivery_id!r}")
        return serializers.webhook_delivery(row)


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


@router.get("/me")
def whoami(ctx: TenantContext = Depends(context)) -> dict[str, Any]:
    """Who this key is, and how much it can see.

    Exists because a cached API key pointing at a stale tenant looks exactly
    like a broken dashboard: every panel renders, every number is real, and
    all of it belongs to a different set of books. Naming the tenant and its
    size on screen turns that from a mystery into a glance.
    """
    with read_only() as uow:
        tenant = uow.one("SELECT * FROM tenants WHERE id = %s", (ctx.tenant_id,))
        key = uow.one("SELECT key_prefix, key_last4 FROM api_keys WHERE id = %s",
                      (ctx.api_key_id,))
        counts = uow.one(
            """
            SELECT (SELECT count(*) FROM accounts a
                     WHERE a.tenant_id = %(t)s AND a.mode = %(m)s)::int AS accounts,
                   (SELECT count(*) FROM transactions x
                     WHERE x.tenant_id = %(t)s AND x.mode = %(m)s)::int AS transactions,
                   (SELECT max(x.effective_at) FROM transactions x
                     WHERE x.tenant_id = %(t)s AND x.mode = %(m)s) AS newest
            """,
            {"t": ctx.tenant_id, "m": ctx.mode},
        )
    return {
        "object": "connection",
        "tenant": ctx.tenant_id,
        "tenant_name": tenant["name"] if tenant else None,
        "mode": ctx.mode,
        "key": f"{key['key_prefix']}...{key['key_last4']}" if key else None,
        "accounts": counts["accounts"],
        "transactions": counts["transactions"],
        "newest_transaction": int(counts["newest"].timestamp()) if counts["newest"] else None,
    }


@router.get("/health")
def health(ctx: TenantContext = Depends(context)) -> dict[str, Any]:
    with read_only() as uow:
        return services.health(uow)


@router.get("/fraud_signals")
def list_fraud_signals(
    ctx: TenantContext = Depends(context),
    account: str | None = None,
    limit: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    with read_only() as uow:
        account_id = services.resolve_account(uow, ctx, account)["id"] if account else None
        rows = uow.risk.list_signals(tenant_id=ctx.tenant_id, account_id=account_id, limit=limit)
        return serializers.listing([serializers.fraud_signal(r) for r in rows])


@router.get("/analytics/spend_by_category")
def spend_by_category(
    ctx: TenantContext = Depends(context),
    account: str | None = None,
    days: int = Query(90, ge=1, le=730),
) -> dict[str, Any]:
    with read_only() as uow:
        account_id = services.resolve_account(uow, ctx, account)["id"] if account else None
        rows = uow.normalization.spend_by_category(ctx.tenant_id, account_id, days, ctx.mode)
    return serializers.listing([
        {
            "object": "category_spend",
            "category": r["category"],
            "spend": r["spend_minor"],
            "txn_count": r["txn_count"],
            "unresolved": r["unresolved"],
        }
        for r in rows
    ])


@router.get("/analytics/balance_history")
def balance_history(
    ctx: TenantContext = Depends(context),
    days: int = Query(180, ge=1, le=1095),
    points: int = Query(60, ge=2, le=365),
) -> dict[str, Any]:
    """What you have, over time: every asset and liability account."""
    with read_only() as uow:
        rows = uow.accounts.balance_history(
            ctx.tenant_id, ctx.mode, days=days, points=points
        )

    series: dict[str, dict[str, Any]] = {}
    for row in rows:
        s = series.setdefault(row["id"], {
            "object": "balance_series", "account": row["id"],
            "name": row["name"], "type": row["type"], "points": [],
        })
        s["points"].append({
            "at": int(row["at"].timestamp()), "balance": int(row["balance_minor"]),
        })

    # an account that never moved is noise on a chart, not information
    live = [s for s in series.values() if any(p["balance"] for p in s["points"])]
    return serializers.listing(sorted(live, key=lambda s: s["name"]))


@router.get("/analytics/spending_history")
def spending_history(
    ctx: TenantContext = Depends(context),
    days: int = Query(180, ge=1, le=1095),
    points: int = Query(60, ge=2, le=365),
    top: int = Query(5, ge=1, le=8),
) -> dict[str, Any]:
    """What you spent, over time: cumulative by category.

    Everything past the top N folds into "Other". Past roughly seven colour
    classes adjacent categories stop being distinguishable, so a chart with
    eleven series is a chart nobody can read -- the tail belongs in one bucket
    or in the table, not in a ninth hue.
    """
    with read_only() as uow:
        rows = uow.normalization.spending_history(
            ctx.tenant_id, ctx.mode, days=days, points=points
        )

    series: dict[str, list[dict[str, int]]] = {}
    for row in rows:
        series.setdefault(row["category"], []).append({
            "at": int(row["at"].timestamp()), "spend": int(row["spend_minor"]),
        })

    ranked = sorted(series.items(), key=lambda kv: kv[1][-1]["spend"] if kv[1] else 0,
                    reverse=True)
    head, tail = ranked[:top], ranked[top:]

    out = [{"object": "spend_series", "category": name, "points": points_}
           for name, points_ in head if points_ and points_[-1]["spend"] > 0]

    if tail:
        merged: dict[int, int] = {}
        for _, points_ in tail:
            for point in points_:
                merged[point["at"]] = merged.get(point["at"], 0) + point["spend"]
        if any(merged.values()):
            out.append({
                "object": "spend_series", "category": "Other",
                "points": [{"at": at, "spend": v} for at, v in sorted(merged.items())],
            })
    return serializers.listing(out)


@router.post("/reconcile")
def reconcile(ctx: TenantContext = Depends(context)) -> dict[str, Any]:
    """Recompute every balance from entries and diff against the snapshot cache."""
    with read_only() as uow:
        mismatches = uow.accounts.reconcile()
        drift = uow.accounts.global_drift()
    return {
        "object": "reconciliation",
        "mismatches": mismatches,
        "ledger_drift": drift,
        "ok": not mismatches and not drift,
    }
