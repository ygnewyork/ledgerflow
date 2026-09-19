"""Token bucket rate limiting.

The check and the decrement must be one atomic operation. A read-then-write
from application code is a race that lets a burst straight through under
exactly the load the limiter exists for -- so on Redis this is a Lua script,
which Redis runs atomically, and on Postgres it is a single UPDATE.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from ..config import settings

# Refill continuously rather than in fixed windows: a fixed window lets a
# caller spend a full quota at 11:59:59 and another at 12:00:00, which is twice
# the intended rate across a two-second span.
_LUA = """
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])   -- tokens per second
local now      = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'updated')
local tokens = tonumber(bucket[1])
local updated = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity
  updated = now
end

tokens = math.min(capacity, tokens + (now - updated) * refill)

local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'updated', now)
redis.call('EXPIRE', key, math.ceil(capacity / refill) * 2)

return {allowed, tokens}
"""


@dataclass(frozen=True, slots=True)
class Decision:
    allowed: bool
    limit: int
    remaining: int
    reset_at: int


class RateLimiter:
    def __init__(self, per_minute: int | None = None) -> None:
        self.capacity = per_minute or settings.rate_limit_per_minute
        self.refill = self.capacity / 60.0
        self._redis = None
        self._script = None
        if settings.redis_url:
            try:
                import redis

                self._redis = redis.Redis.from_url(settings.redis_url)
                self._script = self._redis.register_script(_LUA)
            except Exception:
                # a limiter that fails open on a missing dependency is worse
                # than one that falls back, so fall back rather than disable
                self._redis = None

    def check(self, bucket_key: str, cost: int = 1) -> Decision:
        now = time.time()
        if self._script is not None:
            try:
                allowed, tokens = self._script(
                    keys=[f"ratelimit:{bucket_key}"],
                    args=[self.capacity, self.refill, now, cost],
                )
                return self._decision(bool(allowed), float(tokens), now)
            except Exception:
                pass  # fall through to Postgres
        return self._check_pg(bucket_key, cost, now)

    def _check_pg(self, bucket_key: str, cost: int, now: float) -> Decision:
        from ..adapters.db import pool

        with pool().connection() as conn:
            row = conn.execute(
                """
                INSERT INTO rate_limit_buckets (bucket_key, tokens, updated_at)
                VALUES (%(key)s, %(capacity)s - %(cost)s, now())
                ON CONFLICT (bucket_key) DO UPDATE SET
                    tokens = LEAST(
                        %(capacity)s,
                        rate_limit_buckets.tokens
                        + EXTRACT(EPOCH FROM now() - rate_limit_buckets.updated_at) * %(refill)s
                    ) - CASE
                        WHEN LEAST(
                            %(capacity)s,
                            rate_limit_buckets.tokens
                            + EXTRACT(EPOCH FROM now() - rate_limit_buckets.updated_at) * %(refill)s
                        ) >= %(cost)s THEN %(cost)s ELSE 0 END,
                    updated_at = now()
                RETURNING tokens
                """,
                {"key": bucket_key, "capacity": self.capacity,
                 "refill": self.refill, "cost": cost},
            ).fetchone()
        tokens = float(row["tokens"]) if row else 0.0
        return self._decision(tokens >= 0, max(tokens, 0.0), now)

    def _decision(self, allowed: bool, tokens: float, now: float) -> Decision:
        deficit = max(0.0, 1.0 - tokens)
        return Decision(
            allowed=allowed,
            limit=self.capacity,
            remaining=int(max(0.0, tokens)),
            reset_at=int(now + (deficit / self.refill if self.refill else 0)),
        )


# Reads and writes get separate buckets: an analytics client polling
# GET /v1/transactions must not be able to throttle money movement.
read_limiter = RateLimiter()
write_limiter = RateLimiter()
