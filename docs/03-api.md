# API contract

Stripe-shaped, because Stripe's API shape is the one every backend engineer in
this space already has in their head.

```
Base URL   https://api.ledgerflow.dev
Auth       Authorization: Bearer lf_test_9fA2...
Version    LedgerFlow-Version: 2026-09-17     (optional; pinned per key otherwise)
```

---

## Money on the wire

```json
{ "amount": 8437, "currency": "usd" }
```

Integer minor units. A float amount is a `400`, not a rounding decision:

```json
{ "error": { "type": "invalid_request_error", "code": "amount_not_integer",
             "param": "amount",
             "message": "amount must be an integer in the currency's minor unit (8437 = $84.37)" } }
```

This is the first thing a developer hits and the first thing it teaches them
about the system.

---

## Endpoints

```
POST   /v1/accounts
GET    /v1/accounts/{id}
GET    /v1/accounts/{id}/balance          ?as_of= &as_known_at= &strict=
GET    /v1/accounts/{id}/transactions

POST   /v1/transactions                   Idempotency-Key required
GET    /v1/transactions/{id}
GET    /v1/transactions                   ?account= &created[gte]= &limit= &starting_after=
POST   /v1/transactions/{id}/reversals    Idempotency-Key required

POST   /v1/transfers                      Idempotency-Key required

GET    /v1/ledger/entries                 ?account= &since_entry_id= &limit=

GET    /v1/events                         the outbox, as a public log
GET    /v1/events/{id}
POST   /v1/events/{id}/redeliver          re-send to webhook endpoints
POST   /v1/replays                        reprocess a stream range (admin)

POST   /v1/webhook_endpoints
GET    /v1/webhook_endpoints/{id}/deliveries
POST   /v1/webhook_deliveries/{id}/retry

GET    /v1/dead_letters                   ?consumer= &unresolved=true
POST   /v1/dead_letters/{id}/redrive
```

Two different things are both called "replay", so they get different names:

- **`/v1/events/{id}/redeliver`** — resend a webhook. Customer-facing, cheap,
  idempotent from the receiver's perspective thanks to the signed `event_id`.
- **`/v1/replays`** — rewind a consumer group over a range of the stream, e.g.
  after shipping normalizer v4. Admin-only, because it can rewrite a lot of
  derived state at once.

---

## Idempotency

### The protocol

```
POST /v1/transfers
Idempotency-Key: transfer_9384abc
```

```
BEGIN;

  -- 1. try to claim the key
  INSERT INTO idempotency_keys
         (id, api_key_id, idempotency_key, request_hash, status, lease_expires_at, expires_at)
  VALUES (…, …, 'transfer_9384abc', sha256(canonical_request), 'in_progress',
          now() + interval '30 seconds', now() + interval '24 hours')
  ON CONFLICT (api_key_id, idempotency_key) DO NOTHING;

  -- claimed nothing? someone else owns this key. see "on conflict" below.

  -- 2. do the actual work
  INSERT INTO transactions …;
  INSERT INTO entries …;                  -- deferred balance trigger fires at COMMIT

  -- 3. emit the event
  INSERT INTO outbox …;

  -- 4. store the response we are about to return
  UPDATE idempotency_keys
     SET status = 'completed', response_code = 201, response_body = …, resource_id = 'txn_…'
   WHERE id = …;

COMMIT;                                   -- ← the only durability boundary
```

### Why all four steps are in one transaction

The failure that matters is not the duplicate request. It is:

```
request → work commits → process dies → client retries
```

Consider the placements:

| Where the key is written | What breaks |
|---|---|
| Own transaction, **before** the work | Crash between them ⇒ key marked claimed, money never moved. Retry gets a `409 request_in_flight` forever, or worse, a replayed success for work that never happened. |
| Own transaction, **after** the work | Crash between them ⇒ money moved, no record of the key. Retry executes the transfer **a second time**. |
| **Same transaction as the work** | Crash anywhere ⇒ either both exist or neither does. Retry either replays the stored response or starts clean. |

There is exactly one correct answer, and it is the only one that survives a
`kill -9` at an arbitrary instruction. That is the whole argument, and it is a
better interview answer than any amount of "we use idempotency keys".

### On conflict

The `INSERT` claimed nothing. Read the existing row:

| Existing row | Response |
|---|---|
| `completed`, `request_hash` matches | `200`/`201` with the stored `response_body`, plus `Idempotent-Replay: true` |
| `completed`, `request_hash` differs | `409 idempotency_key_reuse` — same key, different body is a client bug. Silently returning the first response would hide it. |

The request hash covers method, path, and a canonicalized body — keys sorted,
whitespace normalized, **computed from the validated model rather than the raw
bytes**, so a retry that omits `currency` matches one that sends `"usd"`
explicitly, while a genuinely different request does not.

### What happens to a *concurrent* duplicate

This is the consequence of putting the claim inside the work's transaction, and
it is worth stating precisely because it is not what you would guess.

A second request carrying the same key, arriving while the first is still
running, **does not see an `in_progress` row** — the first transaction has not
committed, so its row is invisible. The second request tries to `INSERT` the
same key and *blocks* on the first transaction's uncommitted row.

That blocking is correct, and better than failing fast:

- If the first request commits, the waiter's `INSERT` conflicts, it reads a
  `completed` row, and it replays the stored response.
- If the first request rolls back, the waiter's `INSERT` succeeds and it does
  the work itself.

Either way the client gets the right answer, and the money moves exactly once.
What is *not* acceptable is waiting forever behind a slow request while holding
a connection — so the claim runs under a `lock_timeout`:

```sql
SELECT set_config('lock_timeout', '3000ms', true);
```

A duplicate that waits longer than that gets `409 request_in_flight` with
`Retry-After: 1`, turning an unbounded wait into a bounded, retryable error.

The `status` and `lease_expires_at` columns remain on the table, and the code
still handles a visible `in_progress` row — but in this single-transaction
design that state is **unreachable by construction**: a crash rolls back both
the claim and the work, and a commit always carries `completed`. The branch is
a guard for a future two-transaction variant, not a path the API reaches on its
own. Both behaviours are pinned by tests
(`test_concurrent_duplicate_waits_then_replays`,
`test_lock_timeout_turns_an_unbounded_wait_into_a_409`).

### What is *not* idempotent

`GET`s, obviously. But also: the key is scoped to the **API key**, not globally.
Two tenants both using `transfer_1` must not collide, and a leaked idempotency
key must not let anyone probe another tenant's responses.

---

## Errors

```json
{
  "error": {
    "type": "invalid_request_error",
    "code": "insufficient_funds",
    "message": "acct_123 has an available balance of $12.00; the transfer requires $84.37",
    "param": "amount",
    "request_id": "req_01J8X…",
    "doc_url": "https://docs.ledgerflow.dev/errors/insufficient_funds"
  }
}
```

Types: `invalid_request_error` (400), `authentication_error` (401),
`permission_error` (403), `not_found_error` (404), `conflict_error` (409),
`rate_limit_error` (429), `api_error` (5xx).

`request_id` is on every response, success or failure, as the `Request-Id`
header — and it is the same id that appears in the structured logs and the
trace. "Send me the request id" has to actually resolve something.

---

## Pagination

Cursor, not offset:

```
GET /v1/transactions?limit=25&starting_after=txn_01J8X…
```

```json
{ "object": "list", "data": [...], "has_more": true }
```

Offset pagination on an append-only log is wrong in a specific, demonstrable
way: new rows arrive at the head between requests, so `?page=2` silently skips
records. The cursor is an `entries.id`, which is monotonic, so a client walking
the ledger cannot miss an entry no matter how fast it is being written.

---

## Rate limits

Token bucket in Redis, refilled continuously, evaluated in a single Lua script
so the check-and-decrement is atomic (`GET` then `SET` from the app is a race
that lets a burst through under exactly the load you built the limiter for).

```
RateLimit-Limit: 100
RateLimit-Remaining: 87
RateLimit-Reset: 1758131260
```

`429` carries `Retry-After`. Read and write buckets are separate — an analytics
client hammering `GET /v1/transactions` must not be able to throttle money
movement.

---

## Webhooks

```http
POST /your/endpoint
LedgerFlow-Signature: t=1758131260,v1=5257a869e7ecebeda32affa62cdca3fa…
Content-Type: application/json

{ "id": "evt_01J8X…", "type": "transaction.created", "created": 1758131260,
  "api_version": "2026-09-17", "data": { "object": { "id": "txn_01J8X…", … } } }
```

Signature = `HMAC-SHA256(secret, "{timestamp}.{raw_body}")`, compared in constant
time. Three details that are easy to skip and are the entire point:

- **The timestamp is inside the signed payload.** Signing only the body lets an
  attacker replay a captured webhook forever.
- **Receivers reject timestamps outside ±5 minutes.** That is what makes the
  above actually enforceable.
- **Signed over the *raw* bytes**, before JSON parsing. Re-serializing changes
  key order and whitespace and breaks every signature.

Delivery is at-least-once with exponential backoff and jitter
(1m, 5m, 30m, 2h, 6h, 24h — then `exhausted` and the endpoint is flagged).
Every attempt is recorded in `webhook_deliveries` and visible in the dashboard,
because "did you get my webhook?" is the single most common support question
any platform with webhooks ever receives.

Receivers are told, in the docs, to dedupe on `event.id` — since at-least-once
means they *will* eventually see a duplicate.

---

## API versioning

Date-based, pinned per API key at creation, overridable per request:

```
LedgerFlow-Version: 2026-09-17
```

Internally: handlers always speak the newest schema, and response objects pass
through a chain of small, pure **downgrade transformers** on the way out — one
per breaking change, each reverting exactly one field rename or shape change.

```
current → (2026-11-01: merchant object → string) → (2026-09-17: drop `confidence`) → response
```

This is the design that makes old versions maintainable: you never fork a
handler, you add one 10-line transformer and a test, and a request pinned three
versions back composes them in order. Without it, supporting two versions means
two code paths and the second one rots immediately.

---

## SDK surface

```python
import ledgerflow

client = ledgerflow.Client("lf_test_abc")

txn = client.transactions.create(
    account="acct_123",
    amount=12500,
    currency="usd",
    merchant="Apple",
    idempotency_key="order_93842",
)

# auto-paginating iterator — no manual cursor handling
for t in client.transactions.list(account="acct_123", created={"gte": 1757980800}):
    print(t.id, t.amount)
```

The SDK owns three things the caller should never have to:

1. **Automatic idempotency keys** on every write, if the caller doesn't supply
   one. A generated UUID is better than nothing, because the retry in point 2
   would otherwise be dangerous.
2. **Retries with backoff and jitter** on `429` and `5xx` — safe *precisely
   because* of point 1.
3. **Typed errors.** `InsufficientFunds` is a class you can `except`, not a
   string you have to match on.
