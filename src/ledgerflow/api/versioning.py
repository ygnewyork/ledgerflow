"""Date-based API versioning.

Handlers always speak the newest schema. Responses pass through a chain of
small, pure downgrade transformers on the way out, one per breaking change,
each reverting exactly one thing.

The alternative -- a branch inside the handler, or a forked handler per version
-- means the old path stops being exercised and rots. A transformer is ten
lines with its own test, and a request pinned three versions back simply
composes three of them in order.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any

from ..config import settings

Transformer = Callable[[dict[str, Any]], dict[str, Any]]

# (version this change landed in, what to undo for callers pinned before it)
_CHANGES: list[tuple[date, Transformer]] = []


def downgrade(introduced: date) -> Callable[[Transformer], Transformer]:
    def register(fn: Transformer) -> Transformer:
        _CHANGES.append((introduced, fn))
        _CHANGES.sort(key=lambda pair: pair[0], reverse=True)
        return fn

    return register


def apply(body: dict[str, Any], requested: date | None) -> dict[str, Any]:
    """Walk backwards from current to the caller's pinned version."""
    if requested is None or requested >= settings.api_version:
        return body
    for introduced, transform in _CHANGES:
        if requested < introduced:
            body = transform(body)
    return body


# --- example of the pattern, kept live so the machinery stays tested ---------


@downgrade(date(2026, 9, 17))
def _balance_without_basis(body: dict[str, Any]) -> dict[str, Any]:
    """Before 2026-09-17, a balance had no ``basis`` field.

    Callers pinned earlier get the old shape; the handler never knows.
    """
    if body.get("object") == "balance":
        body = {k: v for k, v in body.items() if k != "basis"}
    return body
