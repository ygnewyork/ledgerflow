"""Request dependencies: identity, limits, and the version the caller speaks."""

from __future__ import annotations

from datetime import UTC, date, datetime

from fastapi import Header, Request

from ..adapters.db import read_only
from ..application.errors import LedgerFlowError, RateLimited
from ..application.services import TenantContext
from ..config import settings
from .auth import authenticate
from .ratelimit import read_limiter, write_limiter


def parse_timestamp(value: str | None) -> datetime | None:
    """Accept RFC 3339 or a unix timestamp; always return tz-aware UTC."""
    if value is None:
        return None
    try:
        if value.isdigit():
            return datetime.fromtimestamp(int(value), tz=UTC)
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise LedgerFlowError(f"cannot parse {value!r} as a timestamp") from None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def context(
    request: Request,
    authorization: str | None = Header(None),
    ledgerflow_version: str | None = Header(None, alias="LedgerFlow-Version"),
) -> TenantContext:
    with read_only() as uow:
        key_row = authenticate(uow, authorization)

    ctx = TenantContext(
        tenant_id=key_row["tenant_id"],
        api_key_id=key_row["id"],
        mode=key_row["mode"],
    )

    # header overrides the version pinned on the key at creation
    requested = key_row["api_version"]
    if ledgerflow_version:
        try:
            requested = date.fromisoformat(ledgerflow_version)
        except ValueError:
            raise LedgerFlowError(
                f"LedgerFlow-Version must be a date like {settings.api_version}",
            ) from None
    request.state.api_version = requested
    request.state.ctx = ctx

    limiter = write_limiter if request.method in ("POST", "DELETE", "PATCH") else read_limiter
    decision = limiter.check(f"{ctx.api_key_id}:{'w' if request.method == 'POST' else 'r'}")
    request.state.rate_limit = decision
    if not decision.allowed:
        raise RateLimited(
            "too many requests",
            retry_after=max(1, decision.reset_at - int(datetime.now(UTC).timestamp())),
        )
    return ctx
