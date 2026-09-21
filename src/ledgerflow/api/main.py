"""The LedgerFlow API."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from .. import ids
from ..application.errors import LedgerFlowError
from ..config import settings
from ..domain.ledger import LedgerError
from ..domain.money import MoneyError
from . import versioning

log = logging.getLogger("ledgerflow.api")

app = FastAPI(
    title="LedgerFlow",
    version=str(settings.api_version),
    description=(
        "Real-time financial event platform. Amounts are integers in the "
        "currency's minor unit. Write endpoints accept an Idempotency-Key."
    ),
)


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


@app.middleware("http")
async def envelope(request: Request, call_next):  # type: ignore[no-untyped-def]
    """Attach a request id, rate-limit headers, and the caller's API version.

    The request id is generated before the handler runs and is echoed on every
    response, success or failure -- and it is the same id the structured log
    line carries, so "send me the request id" actually resolves something.
    """
    request_id = ids.request_id()
    request.state.request_id = request_id
    started = time.perf_counter()

    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - started) * 1000

    response.headers["Request-Id"] = request_id
    response.headers["LedgerFlow-Version"] = str(
        getattr(request.state, "api_version", settings.api_version)
    )
    if getattr(request.state, "idempotent_replay", False):
        # tells the caller this was not re-executed -- invaluable when
        # debugging a retry storm from the client side
        response.headers["Idempotent-Replay"] = "true"

    decision = getattr(request.state, "rate_limit", None)
    if decision is not None:
        response.headers["RateLimit-Limit"] = str(decision.limit)
        response.headers["RateLimit-Remaining"] = str(decision.remaining)
        response.headers["RateLimit-Reset"] = str(decision.reset_at)

    log.info(
        "%s",
        json.dumps({
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status": response.status_code,
            "duration_ms": round(elapsed_ms, 2),
        }),
    )
    return response


@app.middleware("http")
async def version_responses(request: Request, call_next):  # type: ignore[no-untyped-def]
    """Downgrade the response body to the version the caller pinned."""
    response = await call_next(request)
    requested = getattr(request.state, "api_version", None)
    if requested is None or requested >= settings.api_version:
        return response
    if response.headers.get("content-type", "").startswith("application/json"):
        body = b"".join([chunk async for chunk in response.body_iterator])
        try:
            payload = versioning.apply(json.loads(body), requested)
        except (json.JSONDecodeError, TypeError):
            return JSONResponse(
                status_code=response.status_code, content=None, headers=dict(response.headers)
            )
        return JSONResponse(
            status_code=response.status_code,
            content=payload,
            headers={
                k: v for k, v in response.headers.items() if k.lower() != "content-length"
            },
        )
    return response


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


def _error_body(
    request: Request, *, type_: str, code: str, message: str, param: str | None
) -> dict[str, Any]:
    return {
        "error": {
            "type": type_,
            "code": code,
            "message": message,
            "param": param,
            "request_id": getattr(request.state, "request_id", None),
            "doc_url": f"https://docs.ledgerflow.dev/errors/{code}",
        }
    }


@app.exception_handler(LedgerFlowError)
async def handle_app_error(request: Request, exc: LedgerFlowError) -> JSONResponse:
    # A decline is evidence. Its own transaction already rolled back with the
    # posting it refused, so the record is written here, on the way out.
    from ..application.errors import TransactionDeclined
    from ..application.services import record_decline

    if isinstance(exc, TransactionDeclined) and getattr(request.state, "ctx", None):
        try:
            record_decline(request.state.ctx, exc)
        # Deliberately broad: recording the decline is bookkeeping, and a
        # failure here must never mask the decline the caller came for.
        except Exception:
            log.exception("could not record a decline")

    headers = {}
    if retry_after := exc.extra.get("retry_after"):
        headers["Retry-After"] = str(retry_after)
    body = _error_body(
        request, type_=exc.error_type, code=exc.code, message=exc.message, param=exc.param
    )
    body["error"].update({k: v for k, v in exc.extra.items() if k != "retry_after"})
    return JSONResponse(status_code=exc.status_code, content=body, headers=headers)


@app.exception_handler(MoneyError)
async def handle_money_error(request: Request, exc: MoneyError) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content=_error_body(
            request, type_="invalid_request_error", code="invalid_amount",
            message=str(exc), param="amount",
        ),
    )


@app.exception_handler(LedgerError)
async def handle_ledger_error(request: Request, exc: LedgerError) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content=_error_body(
            request, type_="invalid_request_error", code="invalid_posting",
            message=str(exc), param=None,
        ),
    )


@app.exception_handler(RequestValidationError)
async def handle_validation(request: Request, exc: RequestValidationError) -> JSONResponse:
    first = exc.errors()[0] if exc.errors() else {}
    param = ".".join(str(p) for p in first.get("loc", []) if p not in ("body", "query"))
    message = first.get("msg", "invalid request")
    # pydantic prefixes custom validator messages; the prefix is noise to a
    # developer reading an API error
    for prefix in ("Value error, ", "Assertion failed, "):
        if message.startswith(prefix):
            message = message[len(prefix):]
    return JSONResponse(
        status_code=400,
        content=_error_body(
            request, type_="invalid_request_error",
            code="parameter_invalid", message=message, param=param or None,
        ),
    )


@app.exception_handler(Exception)
async def handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None)
    log.exception("unhandled error request_id=%s", request_id)
    return JSONResponse(
        status_code=500,
        content=_error_body(
            request, type_="api_error", code="internal_error",
            # never leak the exception text: it can contain SQL, table names,
            # or fragments of another tenant's data
            message="an unexpected error occurred; quote the request id to support",
            param=None,
        ),
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

from .routers.resources import router as v1_router  # noqa: E402

app.include_router(v1_router)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    return Response(status_code=204)


@app.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    """Liveness only -- no auth, no database. Deliberately boring."""
    return {"status": "ok"}


def mount_dashboard() -> None:
    import pathlib

    static = pathlib.Path(__file__).resolve().parents[1] / "dashboard" / "static"
    if static.is_dir():
        app.mount("/dashboard", StaticFiles(directory=str(static), html=True), name="dashboard")

        @app.get("/", include_in_schema=False)
        async def index() -> RedirectResponse:
            # redirect rather than serving index.html from "/": the page's
            # relative asset paths must resolve under /dashboard/, and serving
            # the same file at two prefixes silently breaks one of them
            return RedirectResponse("/dashboard/")


mount_dashboard()
