"""Prefixed, time-sortable identifiers.

ULIDs rather than UUIDv4: the first 48 bits are a millisecond timestamp, so ids
sort by creation time. That means a B-tree index on the id stays dense instead
of fragmenting on random inserts, and a human reading a log can tell which of
two ids came first.

The type prefix (``txn_``, ``acct_``) is there for the same reason Stripe's is:
an id pasted into a bug report says what it refers to without any context.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Final

# Crockford base32: no I, L, O or U, so an id read aloud or copied by hand does
# not turn into a different valid id.
_ALPHABET: Final = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _encode(value: int, length: int) -> str:
    out = []
    for _ in range(length):
        out.append(_ALPHABET[value & 0x1F])
        value >>= 5
    return "".join(reversed(out))


_MAX_RANDOM: Final = (1 << 80) - 1

_lock = threading.Lock()
_last_ms = 0
_last_random = 0


def ulid() -> str:
    """A monotonic ULID.

    Two ULIDs minted in the same millisecond share a timestamp prefix, so plain
    random suffixes would sort arbitrarily against each other -- and the ledger
    leans on id ordering. The monotonic variant fixes that: within a
    millisecond the previous randomness is incremented rather than redrawn, so
    ids are strictly increasing even under a tight loop.
    """
    global _last_ms, _last_random

    with _lock:
        now_ms = int(time.time() * 1000)
        if now_ms > _last_ms:
            _last_ms = now_ms
            _last_random = int.from_bytes(os.urandom(10), "big")
        else:
            # same millisecond (or a clock that went backwards): keep the
            # previous timestamp and step the random component
            if _last_random >= _MAX_RANDOM:
                # 2^80 ids in one millisecond is not a real scenario, but
                # wrapping silently into a duplicate id would be a real bug
                _last_ms += 1
                _last_random = int.from_bytes(os.urandom(10), "big")
            else:
                _last_random += 1
        timestamp, randomness = _last_ms, _last_random

    return _encode(timestamp, 10) + _encode(randomness, 16)


def new_id(prefix: str) -> str:
    return f"{prefix}_{ulid()}"


def account_id() -> str:
    return new_id("acct")


def transaction_id() -> str:
    return new_id("txn")


def event_id() -> str:
    return new_id("evt")


def request_id() -> str:
    return new_id("req")
