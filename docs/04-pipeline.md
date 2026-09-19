# Stream, workers, and derived data

## Why a broker at all

The honest answer to *"why not just write to Postgres?"* — and you will be asked
this — is that for the ledger itself, you **should** just write to Postgres. The
ledger is a transactional workload with a hard consistency requirement and it
belongs in a database.

The broker exists for what happens *next*: normalization, feature computation,
fraud evaluation, webhook fan-out, analytics export. Those are slow, failure-
prone, independently scaled, and must not be able to fail the write path or hold
a database transaction open while they call someone's webhook endpoint over the
public internet.

So: **Postgres is the source of truth, the stream is the fan-out mechanism.**
Saying it that way is worth more than naming five technologies.

---

## The outbox relay

```
  outbox (Postgres)                    Redpanda
 ┌────────────────────┐              ┌──────────────────┐
 │ id  event_id  …    │   relay      │ ledger.events.v1 │
 │ 41  evt_a    ✓sent │ ──────────►  │                  │
 │ 42  evt_b    NULL  │              └──────────────────┘
 │ 43  evt_c    NULL  │
 └────────────────────┘
```

```sql
BEGIN;
SELECT * FROM outbox
 WHERE published_at IS NULL
 ORDER BY id
 LIMIT 500
 FOR UPDATE SKIP LOCKED;     -- lets N relay instances run without coordination
-- produce to the broker, wait for acks
UPDATE outbox SET published_at = now(), attempts = attempts + 1 WHERE id = ANY($1);
COMMIT;
```

`FOR UPDATE SKIP LOCKED` is what turns this into a work queue: multiple relay
processes each grab a disjoint batch, no leader election, no Zookeeper.

The relay can crash after producing and before the `UPDATE`, which republishes
the batch — that is *the* reason delivery is at-least-once and every consumer
dedupes. It is not a flaw to apologize for; it is the tradeoff, chosen
deliberately, because the alternative (marking sent before producing) loses
events, and losing a financial event is unrecoverable while seeing one twice is
not.

**Upgrade path:** swap polling for Debezium CDC on the WAL. Same topic, same
consumers, no schema change — which is the payoff for having the outbox at all.

---

## Topics

| Topic | Key | Produced by | Consumed by |
|---|---|---|---|
| `ledger.events.v1` | `account_id` | outbox relay | normalizer, risk, webhooks, analytics |
| `transactions.normalized.v1` | `account_id` | normalizer | risk, analytics |
| `fraud.signals.v1` | `account_id` | risk worker | webhooks, dashboard |
| `*.dlq.v1` | original key | any consumer | redrive tooling |

**Partition key = `account_id`.** Kafka orders within a partition only, so this
buys per-account ordering — which is the only ordering that means anything.
Global ordering across all accounts is neither achievable at scale nor useful:
two unrelated users' purchases have no causal relationship.

The consequence to be honest about: one enormous account is a hot partition.
The mitigation, if it ever matters, is a composite key (`account_id:bucket`)
plus a re-sequencing step — which you accept only when you actually have the
problem.

---

## Consumer contract

Every consumer does the same three things:

```python
def handle(event: Event) -> None:
    with db.transaction() as tx:
        # 1. claim the event; conflict means we already did this work
        claimed = tx.execute(
            "INSERT INTO processed_events (consumer_group, event_id) "
            "VALUES (%s, %s) ON CONFLICT DO NOTHING RETURNING 1",
            (GROUP, event.id),
        ).fetchone()
        if not claimed:
            return                      # duplicate delivery — drop it

        # 2. the actual side effect, in the SAME transaction as the claim
        do_the_work(tx, event)

    # 3. only now is the offset allowed to advance
    consumer.commit()
```

The claim and the side effect share a transaction for the same reason the
idempotency row shares one with the ledger write: any other arrangement has a
crash window where the event is marked done but the work did not happen.

Offsets are committed **after** the database transaction, never with
auto-commit. Auto-commit advances the offset on poll, so a crash mid-processing
skips the event entirely — at-most-once semantics, silently.

### Retries and the DLQ

```
attempt 1  ──fail──►  attempt 2 (2s)  ──fail──►  attempt 3 (8s)  ──fail──►  DLQ
                                                                             │
                                                    GET /v1/dead_letters ◄───┘
                                                    POST /v1/dead_letters/{id}/redrive
```

Retries happen in-process with jittered backoff, bounded at 3 attempts. Then the
event goes to the DLQ topic and a `dead_letters` row, and the consumer **moves
on** — a single poison message must never stall a partition, because stalling a
partition stalls every account hashed to it.

Distinguish the two failure classes:

- **Transient** (DB down, timeout) — retry, and if the DLQ fills, that is an
  alert about infrastructure.
- **Poison** (malformed payload, code bug) — retrying is pointless. Straight to
  the DLQ with the error class recorded, fix the bug, redrive.

---

## Normalization

```
'SQ *TST* STARBUCKS 800-782-7282 CA'
'STARBUCKS #04212'
'STARBUCKS MOBILE 04212'
                │
                ▼
  { "merchant": { "name": "Starbucks", "category": "Food and Drink",
                  "confidence": 0.96 } }
```

Four stages, deterministic before anything clever:

1. **Clean.** Uppercase, strip known processor prefixes (`SQ *`, `TST*`, `PAYPAL *`,
   `SP `), phone numbers, store numbers (`#04212`), trailing state codes, dates
   embedded in the descriptor, collapse whitespace.
2. **Candidates.** Trigram similarity against `merchant_aliases`, indexed with
   `pg_trgm`. Top 5 by score.
3. **Resolve.** Blend the string-similarity score with a prior: how often this
   tenant, and the corpus overall, resolved a similar descriptor to that
   merchant. Highest blended score wins.
4. **Confidence.** Below `0.80` → leave `merchant_id` NULL and keep the cleaned
   string. **An honest "I don't know" beats a confident wrong answer**, because
   a wrong merchant silently poisons every downstream category total and every
   fraud feature that keys on merchant novelty.

Two rules make this maintainable:

- **`raw_transactions` is never modified.** Normalized rows are derived, always
  regenerable, and safe to delete.
- **`normalizer_version` is stored on every row.** Ship v4, replay the stream,
  and diff v4 against v3 on the same inputs before trusting it. Without the
  version column you cannot even tell which rows came from which logic.

Correctness is measured against a hand-labeled golden set of ~500 real-shaped
descriptors, run in CI. Precision matters more than recall here, for the reason
in step 4.

---

## Real-time features

Sliding windows in Redis sorted sets, scored by event timestamp:

```
ZADD   spend:acct_123  1758131260  "txn_01J8X:8437"
ZREMRANGEBYSCORE spend:acct_123  -inf  (now - 3600)
ZRANGEBYSCORE    spend:acct_123  (now - 3600)  +inf
```

| Feature | Window | Used for |
|---|---|---|
| `spend_1h`, `spend_24h` | sliding | velocity rules |
| `txn_count_1h` | sliding | card-testing detection (many tiny charges, fast) |
| `distinct_merchants_7d` | sliding | novelty baseline |
| `amount_zscore` | 90d rolling mean/σ | "this is unlike you" |
| `merchant_frequency` | lifetime | novelty denominator |

### Point-in-time correctness

The rule that makes this a real feature pipeline instead of a dashboard:
**features are computed as of the event's timestamp, never as of now.**

If you evaluate a September 17 transaction using a window that includes
September 18 data, you have leaked the future into the decision. Online it is
merely wrong; offline, if you ever train on it, the model learns from
information it will never have at inference time, scores beautifully in
backtest, and fails in production. Same code path, same window semantics, both
online and offline — that is what "offline/online parity" means and why feature
stores exist at all.

Every emitted signal stores its `features` blob, so any decision can be
explained and reproduced months later.

### Rules

```python
RULES = [
    Rule("velocity_1h",    lambda f: f.spend_1h > 5 * f.avg_spend_1h_90d,     score=0.6),
    Rule("amount_zscore",  lambda f: f.amount_zscore > 4,                     score=0.5),
    Rule("novel_merchant", lambda f: f.merchant_frequency == 0
                                     and f.amount_minor > 20_000,             score=0.3),
    Rule("card_testing",   lambda f: f.txn_count_1h > 10
                                     and f.max_amount_1h < 500,               score=0.8),
]
```

Signals are emitted, never enforced. Nothing in this system blocks a posting —
a false positive that declines a real purchase is a far worse outcome than a
flag a human reviews, and the demo is more interesting when you can show the
signal stream next to the money stream.

---

## Analytics

`ledger.events.v1` is continuously written to Parquet, partitioned by date.
DuckDB queries it directly for the dashboard's aggregates and for ad-hoc
analysis.

**DuckDB over Spark, deliberately.** At this data volume Spark is ceremony:
a cluster to manage, a JVM to tune, minutes of startup to answer a question
DuckDB answers in 200ms on a laptop. Choosing the smaller tool and being able to
say *why* reads as judgment. Choosing Spark to have the word on a résumé reads
as the opposite — and an interviewer who works on Spark will find the bottom of
that claim in two questions.

The medallion layering is still worth keeping, because the layering is the idea,
not the engine:

```
raw_transactions         ← immutable, exactly as received
      ↓
normalized_transactions  ← merchant + category resolved, versioned
      ↓
account_features         ← windowed aggregates, point-in-time correct
      ↓
fraud_features           ← model-ready inputs
```

If throughput ever genuinely outgrows a single node, the same layering ports to
Spark Structured Streaming without a redesign. That is the argument for building
it this way now.
