"""Resource shapes.

One place, used by the API, the webhook payloads, and the dashboard, so a field
cannot mean one thing in a response and another in an event.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


def _ts(value: Any) -> int | None:
    return int(value.timestamp()) if isinstance(value, datetime) else None


def account(row: dict[str, Any], balance: dict[str, Any] | None = None) -> dict[str, Any]:
    out = {
        "id": row["id"],
        "object": "account",
        "external_id": row.get("external_id"),
        "name": row["name"],
        "type": row["type"],
        "normal_balance": row["normal_balance"],
        "currency": row["currency"],
        "status": row["status"],
        "minimum_balance": row.get("minimum_balance"),
        "metadata": row.get("metadata") or {},
        "created": _ts(row.get("created_at")),
    }
    if balance is not None:
        out["balance"] = balance.get("balance_minor", 0)
    return out


def entry(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "object": "ledger_entry",
        "transaction": row["transaction_id"],
        "account": row["account_id"],
        "direction": row["direction"],
        "amount": row["amount_minor"],
        "currency": row["currency"],
        # both clocks, always. a consumer that only ever sees one of them will
        # eventually reimplement the other one badly.
        "effective_at": _ts(row.get("effective_at")),
        "recorded_at": _ts(row.get("recorded_at")),
    }


def transaction(row: dict[str, Any], entries: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    out = {
        "id": row["id"],
        "object": "transaction",
        "kind": row["kind"],
        "status": row["status"],
        "effective_at": _ts(row.get("effective_at")),
        "recorded_at": _ts(row.get("recorded_at")),
        "reverses": row.get("reverses_id"),
        "metadata": row.get("metadata") or {},
    }
    if entries is not None:
        out["entries"] = [entry(e) for e in entries]
        out["amount"] = sum(e["amount_minor"] for e in entries if e["direction"] == "debit")
        out["currency"] = entries[0]["currency"] if entries else None
    return out


def event(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["event_id"],
        "object": "event",
        "type": row["event_type"],
        "created": _ts(row.get("created_at")),
        "published": _ts(row.get("published_at")),
        "data": {"object": row["payload"]},
    }


def fraud_signal(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "object": "fraud_signal",
        "account": row["account_id"],
        "transaction": row.get("transaction_id"),
        "rule": row["rule"],
        "score": row["score"],
        # the feature values as of evaluation. without these the decision
        # cannot be explained or reproduced months later.
        "features": row.get("features") or {},
        "evaluated_at": _ts(row.get("evaluated_at")),
    }


def webhook_endpoint(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "object": "webhook_endpoint",
        "url": row["url"],
        "enabled_events": row["enabled_events"],
        "status": row["status"],
        "created": _ts(row.get("created_at")),
    }


def webhook_delivery(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "object": "webhook_delivery",
        "endpoint": row["endpoint_id"],
        "event": row["event_id"],
        "status": row["status"],
        "attempt": row["attempt"],
        "response_code": row.get("response_code"),
        "error": row.get("error"),
        "next_attempt_at": _ts(row.get("next_attempt_at")),
        "created": _ts(row.get("created_at")),
    }


def dead_letter(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "object": "dead_letter",
        "consumer_group": row["consumer_group"],
        "event": row["event_id"],
        "topic": row["topic"],
        "error_class": row["error_class"],
        "error_detail": row.get("error_detail"),
        "attempts": row["attempts"],
        "resolved": row.get("resolved_at") is not None,
        "last_failed_at": _ts(row.get("last_failed_at")),
    }


def listing(data: list[dict[str, Any]], has_more: bool = False) -> dict[str, Any]:
    return {"object": "list", "data": data, "has_more": has_more}
