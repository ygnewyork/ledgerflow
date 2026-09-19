"""API keys.

Keys are shown once, at creation, and stored only as a SHA-256 hash. A database
dump therefore does not hand over working credentials -- which is the whole
reason not to store the key itself, however convenient that would be for a
support tool.

The prefix and last four characters are stored in the clear so the dashboard
can show `lf_test_9fA2...zQ1x` and a developer can tell two keys apart.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from typing import Any

from ..adapters.db import UnitOfWork
from ..config import settings
from ..ids import new_id
from ..application.errors import AuthenticationError


def generate_key(mode: str) -> str:
    return f"lf_{mode}_{secrets.token_urlsafe(24)}"


def hash_key(key: str) -> bytes:
    return hashlib.sha256(key.encode()).digest()


def create_key(uow: UnitOfWork, *, tenant_id: str, mode: str) -> tuple[str, dict[str, Any]]:
    plaintext = generate_key(mode)
    row = uow.one(
        """
        INSERT INTO api_keys
            (id, tenant_id, mode, key_hash, key_prefix, key_last4, api_version)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING *
        """,
        (
            new_id("key"), tenant_id, mode, hash_key(plaintext),
            plaintext[:12], plaintext[-4:], settings.api_version,
        ),
    )
    return plaintext, row  # type: ignore[return-value]


def authenticate(uow: UnitOfWork, authorization: str | None) -> dict[str, Any]:
    if not authorization:
        raise AuthenticationError(
            "no API key provided; send 'Authorization: Bearer lf_test_...'"
        )
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise AuthenticationError("malformed Authorization header; expected 'Bearer <key>'")

    row = uow.one(
        "SELECT * FROM api_keys WHERE key_hash = %s AND revoked_at IS NULL",
        (hash_key(token),),
    )
    if row is None:
        # deliberately does not distinguish "unknown key" from "revoked key":
        # the difference is only useful to someone probing for valid keys
        raise AuthenticationError("invalid API key")

    expected_mode = "test" if token.startswith("lf_test_") else "live"
    if row["mode"] != expected_mode:
        raise AuthenticationError("invalid API key")
    return row


def verify_signature(secret: bytes, timestamp: str, body: bytes, signature: str) -> bool:
    """Constant-time comparison. ``==`` on an HMAC leaks length and prefix."""
    expected = hmac.new(secret, f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)
