"""The idempotency protocol.

The interesting failure is not the duplicate request. It is:

    request -> work commits -> process dies -> client retries

Which only replays correctly if the idempotency record committed *in the same
transaction as the effect*. Write it in its own transaction before the work and
a crash between them marks a key claimed for work that never happened; write it
after and a crash between them moves the money twice. There is one correct
placement, and it is the reason this is a context manager handing the caller a
``UnitOfWork`` rather than middleware wrapping the request.

Middleware cannot do this. Middleware runs outside the handler's transaction by
construction, so it can only write the idempotency row before or after -- both
of which are the broken variants above.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterator

import psycopg

from ..adapters.db import UnitOfWork, unit_of_work
from ..config import settings
from ..ids import new_id
from ..application.errors import IdempotencyKeyReuse, RequestInFlight
from ..application.services import TenantContext


def fingerprint(method: str, path: str, body: Any) -> bytes:
    """Hash of the request's meaning, not its bytes.

    Canonicalized -- sorted keys, no incidental whitespace -- so that a retry
    from a different JSON serializer still matches, while a genuinely different
    request does not.
    """
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(f"{method.upper()} {path} {canonical}".encode()).digest()


@dataclass
class Slot:
    uow: UnitOfWork
    record_id: str | None
    replayed: bool = False
    response: dict[str, Any] = field(default_factory=dict)
    status_code: int = 200

    def complete(self, status_code: int, body: dict[str, Any], resource_id: str | None = None) -> None:
        """Store the response we are about to return, in this transaction."""
        self.status_code = status_code
        self.response = body
        if self.record_id is not None:
            self.uow.idempotency.complete(
                record_id=self.record_id,
                response_code=status_code,
                response_body=body,
                resource_id=resource_id,
            )


@contextlib.contextmanager
def idempotent(
    ctx: TenantContext, key: str | None, request_hash: bytes
) -> Iterator[Slot]:
    """Open the transaction that will hold both the key and the work.

        with idempotent(ctx, key, fp) as slot:
            if slot.replayed:
                return slot.response
            result = do_work(slot.uow)
            slot.complete(201, result, result["id"])
        return result
    """
    with unit_of_work() as uow:
        if key is None:
            yield Slot(uow=uow, record_id=None)
            return

        # Bound the wait. Because the claim commits with the work, a concurrent
        # duplicate does not see an 'in_progress' row -- it sees nothing, tries
        # to INSERT the same key, and BLOCKS on the first transaction's
        # uncommitted row until that transaction ends.
        #
        # That blocking is correct, and better than failing fast: if the first
        # request commits, the waiter conflicts, reads a completed row, and
        # replays; if the first rolls back, the waiter proceeds and does the
        # work. Either way the caller gets the right answer.
        #
        # What is not acceptable is waiting forever behind a slow request and
        # holding a connection while doing it. lock_timeout turns an unbounded
        # wait into a 409 the client can retry.
        # set_config(..., is_local => true) rather than SET LOCAL: the latter
        # takes no bind parameters, and interpolating a value into DDL-ish SQL
        # is a habit worth not forming
        uow.execute(
            "SELECT set_config('lock_timeout', %s, true)",
            (f"{settings.idempotency_lock_timeout_ms}ms",),
        )

        try:
            claimed = uow.idempotency.claim(
            record_id=new_id("idem"),
            tenant_id=ctx.tenant_id,
            api_key_id=ctx.api_key_id,
            key=key,
            request_hash=request_hash,
                lease_seconds=settings.idempotency_lease_seconds,
                ttl_hours=settings.idempotency_ttl_hours,
            )
        except psycopg.errors.LockNotAvailable as exc:
            raise RequestInFlight(
                f"a request with idempotency key {key!r} is still in flight; retry shortly",
                param="Idempotency-Key",
                retry_after=1,
            ) from exc

        if claimed is not None:
            yield Slot(uow=uow, record_id=claimed["id"])
            return

        # someone else owns this key
        existing = uow.idempotency.get(ctx.api_key_id, key)
        if existing is None:  # raced with a purge; treat as unclaimed
            yield Slot(uow=uow, record_id=None)
            return

        if existing["status"] == "completed":
            if bytes(existing["request_hash"]) != request_hash:
                # same key, different body. replaying the first response here
                # would hide a real client bug, so surface it instead.
                raise IdempotencyKeyReuse(
                    f"idempotency key {key!r} was already used with a different request body",
                    param="Idempotency-Key",
                )
            yield Slot(
                uow=uow,
                record_id=existing["id"],
                replayed=True,
                response=existing["response_body"] or {},
                status_code=existing["response_code"] or 200,
            )
            return

        # An 'in_progress' row that is VISIBLE to us means it was committed
        # that way, which this design never does: the claim and the completion
        # are in one transaction, so a crash rolls back both and a commit
        # always carries 'completed'. This branch is therefore a guard against
        # a future two-transaction variant or a hand-edited row, not a path the
        # API reaches on its own.
        if not existing["lease_expired"]:
            raise RequestInFlight(
                f"a request with idempotency key {key!r} is still in flight; retry shortly",
                param="Idempotency-Key",
                retry_after=1,
            )

        # the previous owner died mid-request. the UPDATE re-checks the lease,
        # so two processes racing to reclaim cannot both win.
        taken = uow.idempotency.take_over_expired(
            api_key_id=ctx.api_key_id,
            key=key,
            request_hash=request_hash,
            lease_seconds=settings.idempotency_lease_seconds,
        )
        if taken is None:
            raise RequestInFlight(
                f"a request with idempotency key {key!r} is still in flight; retry shortly",
                param="Idempotency-Key",
                retry_after=1,
            )
        yield Slot(uow=uow, record_id=taken["id"])
