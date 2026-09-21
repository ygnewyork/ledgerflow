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
backtest, and fails in production.

There are two parity claims here and they are not the same claim. The streaming
job and the backfill job call one function, so they cannot drift from each other
— that one holds, by construction. The online risk worker and the Spark jobs
are *separate implementations*, and sharing `features/windows.py` only makes
them agree about when a window starts and ends. It does not make them agree
about which rows go inside it. Measured on the demo dataset, they disagree on
1.6% of windows; `docs/07-spark-run.md` has the mechanism and the numbers, and
`python -m ledgerflow.spark.parity` is how you check it on any dataset.

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

## Analytics and the offline pipeline

Spark Structured Streaming into Delta Lake, laid out in medallion tiers:

```
  ledger.events.v1 ──┐
                     ├──► BRONZE   raw_transactions        append-only, exactly as received
  transactions.      │              ledger_entries
  normalized.v1 ─────┘
                            │
                            ▼
                     SILVER  normalized_transactions   merchant + category, versioned
                            │
                            ▼
                     GOLD    account_features          windowed, point-in-time correct
                             merchant_features
                             fraud_features            model-ready
```

### Why Spark here and not a single-node engine

The honest case has three parts, and none of them is throughput — at MVP volume
one machine would keep up.

**1. One code path for streaming and backfill.** Structured Streaming and batch
share the DataFrame API, so the aggregation that runs continuously and the
backfill that rebuilds two years of history are *the same function*, called with
a different reader:

```python
def account_features(txns: DataFrame) -> DataFrame:
    """Used by the streaming job and the backfill job, unchanged."""
    return (
        txns
        .withWatermark("effective_at", "2 hours")
        .groupBy(
            F.col("account_id"),
            F.window(F.col("effective_at"), "1 hour", "5 minutes"),
        )
        .agg(
            F.sum("amount_minor").alias("spend_minor"),
            F.count("*").alias("txn_count"),
            F.approx_count_distinct("merchant_id").alias("distinct_merchants"),
            F.max("amount_minor").alias("max_amount_minor"),
        )
    )

# streaming
account_features(spark.readStream.format("kafka")...load())
# backfill over all of history, same function
account_features(spark.read.format("delta").table("bronze.normalized_transactions"))
```

Two implementations of the same window logic will drift, and the drift shows up
as a model that scored well offline and fails online. One function cannot drift.

**2. Watermarks are real late-data handling, not a `WHERE` clause.** This is the
streaming counterpart of the `effective_at` / `recorded_at` split in the ledger:

```python
.withWatermark("effective_at", "2 hours")
```

Spark tracks the maximum event time it has seen, subtracts the delay threshold,
and uses that watermark to decide when a window is final. Two consequences worth
being able to state:

- Aggregation state for closed windows is **evicted**, so memory is bounded
  instead of growing forever with the number of accounts.
- Events arriving later than the watermark are **dropped from the aggregate**.
  They are not silently lost — route them to a `late_arrivals` Delta table and
  alert on the rate, because a rising late-arrival count means the watermark is
  too tight or an upstream producer is lagging.

Picking `2 hours` is a tradeoff you own: longer tolerates more lateness and
holds more state; shorter is cheaper and drops more. Say which you chose and why.

**3. Recoverable state.** The checkpoint directory holds both the Kafka offsets
*and* the serialized aggregation state, so a killed job resumes mid-window
rather than recomputing from scratch:

```python
.option("checkpointLocation", "s3://ledgerflow/_checkpoints/account_features_v1")
```

The `_v1` suffix matters. A checkpoint is coupled to the query plan; change the
aggregation shape and Spark will either refuse to start or resume with state it
cannot interpret. Versioning the path makes a breaking change an explicit
decision — new path, replay from the retention window — instead of a 3am
incident.

### Idempotent writes

At-least-once again, one layer up. `foreachBatch` plus a Delta `MERGE` keyed on
the window makes a re-run of the same micro-batch a no-op:

```python
def upsert_features(batch_df: DataFrame, batch_id: int) -> None:
    (
        DeltaTable.forName(spark, "gold.account_features").alias("t")
        .merge(
            batch_df.alias("s"),
            "t.account_id = s.account_id AND t.window_end = s.window_end",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

(
    account_features(stream)
    .select("account_id", F.col("window.end").alias("window_end"), "*")
    .writeStream
    .foreachBatch(upsert_features)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("update")
    .trigger(processingTime="30 seconds")
    .start()
)
```

This is the same idea as `processed_events` in the consumers and the idempotency
key in the API: the write path is replay-safe, so redelivery is boring. Notice
the pattern repeating at all three layers — that repetition is the system's
actual thesis.

### Point-in-time correct training sets

The rule from the online path applies with more force offline: **a feature must
be computed only from data that existed at decision time.** Spark has no native
as-of join, so it is a windowed row pick, and the `<=` is where leakage would
otherwise enter:

```python
joined = (
    labels.alias("l")
    .join(features.alias("f"), "account_id")
    .where(F.col("f.window_end") <= F.col("l.decision_at"))   # <- the whole ballgame
)

training = (
    joined
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("l.label_id").orderBy(F.col("f.window_end").desc())
        ),
    )
    .where("rn = 1")
    .drop("rn")
)
```

Replace `f.window_end <= l.decision_at` with an unconstrained join and you get a
model that reads the future, scores beautifully in backtest, and fails in
production. It is the single most common way a feature pipeline is silently
wrong, and being able to point at the line that prevents it is worth more than
any amount of pipeline diagramming.

### Delta gives the analytics tier the ledger's own property

```sql
SELECT * FROM gold.account_features VERSION AS OF 42;
SELECT * FROM gold.account_features TIMESTAMP AS OF '2026-09-01';
```

The ledger is bitemporal; Delta time travel gives the derived tables the same
ability. "Why did the dashboard show a different number on September 1" becomes
a query against both layers rather than a shrug.

It also makes a normalizer upgrade safe to evaluate: ship v4, replay bronze into
a new silver version, and diff v4 against v3 on identical inputs before any
consumer sees it.

### The costs, stated plainly

- A JVM, a cluster, and tens of seconds of job startup — so Spark is wrong for
  anything on the request path. Nothing user-facing waits on it.
- Checkpoints are coupled to query plans (hence `_v1`).
- Micro-batch output produces many small files; Delta `OPTIMIZE` and
  auto-compaction are maintenance you now own.
- At current volume a single node would suffice. Spark is here for the shared
  streaming/batch code path, watermarked recoverable state, and Delta's
  versioned tables — not because the data is big.

That last sentence is the answer to *"why Spark?"*, and it is a much better one
than naming the volume you do not have.

**Local development:** Spark runs in `docker-compose` with `delta-spark`; no
Databricks account is needed. The same jobs run on Databricks unchanged if you
want to demo them there, since Structured Streaming plus Delta is exactly what
that platform's streaming tables and pipelines are built on.

### Serving the dashboard

Gold tables are small and pre-aggregated, so the dashboard queries them
directly. Spark is not in the read path — it produces the tables, and the API
reads them. Keeping the interactive path off the cluster is what keeps the demo
responsive.
