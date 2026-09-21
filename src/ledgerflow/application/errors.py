"""Application errors, each carrying the HTTP shape it should produce.

Defined here rather than in the API layer so that workers raise the same
exceptions as request handlers, and so a new endpoint cannot invent a new error
code for a condition that already has one.
"""

from __future__ import annotations

from typing import Any


class LedgerFlowError(Exception):
    status_code = 400
    error_type = "invalid_request_error"
    code = "invalid_request"

    def __init__(self, message: str, *, param: str | None = None, **extra: Any) -> None:
        super().__init__(message)
        self.message = message
        self.param = param
        self.extra = extra


class AuthenticationError(LedgerFlowError):
    status_code = 401
    error_type = "authentication_error"
    code = "invalid_api_key"


class PermissionError_(LedgerFlowError):
    status_code = 403
    error_type = "permission_error"
    code = "permission_denied"


class NotFound(LedgerFlowError):
    status_code = 404
    error_type = "not_found_error"
    code = "resource_missing"


class AccountNotFound(NotFound):
    code = "account_not_found"


class InsufficientFunds(LedgerFlowError):
    code = "insufficient_funds"


class CurrencyMismatchError(LedgerFlowError):
    code = "currency_mismatch"


class ConflictError(LedgerFlowError):
    status_code = 409
    error_type = "conflict_error"
    code = "conflict"


class IdempotencyKeyReuse(ConflictError):
    code = "idempotency_key_reuse"


class RequestInFlight(ConflictError):
    code = "request_in_flight"


class AlreadyReversed(ConflictError):
    code = "transaction_already_reversed"


class TransactionDeclined(LedgerFlowError):
    """A blocking risk rule refused the posting.

    402 rather than 400: the request was well-formed and the caller did
    nothing wrong. Something about the pattern was refused, which is a
    different conversation from a malformed body.
    """

    status_code = 402
    error_type = "card_error"
    code = "transaction_declined"


class RateLimited(LedgerFlowError):
    status_code = 429
    error_type = "rate_limit_error"
    code = "rate_limit_exceeded"
