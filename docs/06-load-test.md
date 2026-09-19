# Load test

> Run it yourself:
> ```
> make serve
> python -m ledgerflow.bench --key lf_test_... --requests 1600 --sweep --funding-accounts 32
> ```

**Environment.** 4 vCPU container, 16 GB RAM, PostgreSQL 16 co-located on the
same box (`fsync=on`, `synchronous_commit=on`), single uvicorn worker, client
and server on the same machine. These are small numbers on small hardware; the
*shapes* below are the findings, not the absolute values.

Every request is a full `POST /v1/transactions`: idempotency claim, account
resolution, overdraft check, transaction, two ledger entries, the raw
descriptor row, and the outbox event — all in one database transaction.

---

## Results

**Writes sharded across 32 funding accounts**

| concurrency | txn/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|
| 1 | 137 | 7.2 | 8.8 | 9.8 |
| **2** | **192** | **10.3** | **11.9** | **12.9** |
| 4 | 148 | 26.6 | 32.6 | 40.8 |
| 8 | 128 | 61.5 | 75.0 | 85.2 |
| 16 | 127 | 125.1 | 147.9 | 160.4 |
| 32 | 127 | 250.9 | 279.1 | 294.0 |

**The same load against one funding account**

| concurrency | txn/s | p50 ms | p99 ms |
|---|---|---|---|
| 1 | 46 | 21.7 | 27.7 |
| 2 | 55 | 35.8 | 48.3 |
| 8 | 50 | 160.8 | 178.0 |
| 32 | 47 | 668.5 | 752.2 |

Zero errors in every run; the ledger reconciled clean afterwards.

---

## What the shapes say

**Throughput peaks at concurrency 2 and then flattens while latency grows
linearly.** That is the signature of a saturated resource, not a lock: past the
knee, extra clients queue rather than accomplish anything. On 4 vCPUs with the
database on the same box, the server and Postgres are competing for the same
cores. The honest conclusion is that this measures a laptop-class deployment,
and the next real gain is horizontal (more workers, database on its own host),
not another micro-optimization.

**One funding account costs ~4x.** 192 → 55 txn/s, with p99 rising from 13 ms
to 752 ms. Accounts with a balance floor take `SELECT ... FOR UPDATE` on the
write path, so every purchase funded from the same account serializes on that
row. This is correct — it is what stops two concurrent transfers from each
reading a sufficient balance and both committing — but it is the ceiling for
any single account, and it is why accounts without a floor never take the lock
at all.

---

## Four things the load test found

The numbers were the least valuable output. These were:

### 1. The balance cache was never written

`write_snapshot` existed and nothing called it, so every balance read summed
the account's entire history. Measured, and exactly linear:

| entries | balance() |
|---|---|
| 578 | 0.54 ms |
| 16,785 | 4.29 ms |
| 35,163 | 14.95 ms |

With snapshots written, the same reads: **0.12 ms and 0.32 ms** — a 45x
improvement at the top end, and now flat instead of growing. Every floored
write does a balance read, so this was compounding into the write path too.
Fixed by `ledgerflow snapshot`, which `drain_all` now calls.

### 2. The rate limiter was a no-op without Redis

The Postgres fallback subtracted a token only when tokens remained, then
reported `tokens >= 0`. At zero it declined to subtract, left the value at
zero, and zero passed the test — so it allowed everything, forever. It was
load-bearing precisely when Redis was missing, which is when it never worked.

Now it always subtracts and clamps at `-cost`, letting the sign carry the
answer. Verified: capacity 60, 100 rapid requests, exactly 60 allowed.

### 3. Thirteen database round trips per write

Counted by instrumenting the cursor. Three were pure waste:

- `resolve_account` tried our id, missed, then tried the external id — two
  queries for what one `WHERE id = %s OR external_id = %s` answers, on every
  account, on every request.
- The funding account was resolved a *second* time to attach the raw
  descriptor row, for an answer already in hand.
- Entries were inserted one statement per leg.

**13 → 8 round trips**; service-layer cost 3.5 ms → 2.6 ms per write.

### 4. Async handlers over a blocking driver

Every route was `async def` while psycopg blocks. FastAPI runs async handlers
on the event loop, so one blocking query stalls every other request in the
process. Converted to sync `def`, which dispatches to the threadpool.

Honestly: **this did not change throughput here**, because commit-level work,
not the event loop, is what binds on this hardware. It stays fixed because the
mixture is a latent hazard — the day one query gets slow, the whole process
stalls rather than that one request.

---

## What was ruled out

**Commit durability.** `synchronous_commit=off` gave 111 txn/s against 108 with
it on — within noise. fsync is not the constraint here, so there is no
temptation to trade durability for speed in a ledger. Worth measuring precisely
so the answer is "we checked", not "we assumed".

---

## What would actually move the number

In the order worth doing them, and none of them done yet:

1. **Database on its own host.** The server and Postgres share 4 cores; that is
   the ceiling this test is hitting.
2. **Multiple uvicorn workers.** One process on a 4-core box leaves cores idle.
3. **Pipelined round trips.** 8 is better than 13, and a single CTE could post
   the whole transaction in one.

Claiming any of these before measuring them is how a load test becomes
marketing. The numbers above are what this configuration does.
