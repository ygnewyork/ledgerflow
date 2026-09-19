# Roadmap

The risk with a project this shape is that it becomes eight half-built
subsystems. So every milestone below has a **done when** that is demonstrable,
and each one is independently worth showing even if you stop there.

---

## M1 — The ledger  (~1 week)

Pure Python domain + Postgres. No API, no broker.

- `Money`, `Entry`, `JournalTransaction`, posting-rule registry
- Schema, migrations, the deferred balance trigger, the immutability trigger
- Snapshot + delta balance reads
- Reconciliation job: recompute from entries, diff snapshots

**Done when:** a property test generates thousands of random posting sequences
and `Σ debits = Σ credits` holds for every currency at every point; and
`INSERT`ing a deliberately unbalanced pair is rejected by the database at
`COMMIT`, not by application code.

That second half is the one to record a terminal capture of.

---

## M2 — The API  (~1 week)

FastAPI. Auth, validation, idempotency, pagination, rate limits, errors.

- API keys: hashed at rest, `lf_test_` / `lf_live_`, test and live isolated
- The idempotency protocol from `03-api.md`, in one transaction with the write
- Cursor pagination, token-bucket limiter in Redis via Lua
- Structured logs and a `request_id` that actually joins logs to responses

**Done when:** an integration test kills the API process between `COMMIT` and
the HTTP response, retries with the same `Idempotency-Key`, and asserts exactly
one transaction exists and the retry returned the original response body.

That test is the most valuable artifact in the repository. Name the file
`test_crash_between_commit_and_response.py` so a reviewer skimming the tree
finds it.

---

## M3 — Events  (~1 week)

- `outbox` written in the same transaction as every ledger write
- Relay with `FOR UPDATE SKIP LOCKED`
- Redpanda via docker-compose, `ledger.events.v1`
- Consumer scaffolding: dedupe claim, bounded retry, DLQ, redrive endpoint
- Webhook dispatcher: HMAC signing, backoff, delivery log

**Note on sequencing:** define the `EventPublisher` interface in M1 and back it
with a Postgres-only implementation. Then M3 is swapping the adapter, not
retrofitting the write path. Retrofitting an outbox into code that publishes
inline is a genuinely unpleasant refactor.

**Done when:** you `docker kill` the relay mid-batch and show, from the
`processed_events` table, that the redelivered events changed nothing.

---

## M4 — Normalization  (~4 days)

- Seed merchant dictionary + aliases, `pg_trgm` candidate search
- Cleaning rules, blended scoring, confidence threshold, versioning
- A golden set of ~500 labeled descriptors, scored in CI

**Done when:** `make normalize-eval` prints precision/recall against the golden
set, and bumping `normalizer_version` + replaying produces a diff report of what
changed between versions.

---

## M5 — Features and risk  (~4 days)

- Redis sliding windows, point-in-time correct
- Rule engine, `fraud_signals` with the feature blob attached
- Spark Structured Streaming job: watermarked event-time windows into Delta
- The same window function reused, unchanged, for the historical backfill
- `foreachBatch` + Delta `MERGE` so a replayed micro-batch is a no-op

**Done when:** replaying the same event stream twice produces byte-identical
feature values — the actual test for point-in-time correctness, and one that
fails immediately if anything in the path calls `now()`. Plus: kill the Spark
job mid-window, restart it, and show from the checkpoint that it resumed rather
than recomputed.

---

## M6 — Demo  (~1 week)

This is what people actually look at. Budget real time for it.

- Live transaction feed (SSE or WebSocket)
- Ledger explorer: click a transaction, see its entries, watch them balance
- **Account time-travel slider** — drag through history, balance recomputes
- Health panel: outbox lag, consumer lag, DLQ depth, webhook success rate
- A **break-it button**: kill a consumer, show the lag climb, show recovery

**Done when:** a 90-second screen recording is embedded at the top of the
README. Most people who evaluate this project will watch that and read nothing
else.

---

## M7 — Load  (~3 days)

- Synthetic generator: millions of events, realistic descriptor mess
- Measure p50/p95/p99 write latency, throughput, consumer lag under load
- Publish the numbers *and the bottleneck you found*, with a flamegraph

Real measured numbers with an honest limitation beat round claims. "12k
transactions/sec; p99 write latency 38ms; the ceiling was the single-row
snapshot update on hot accounts" is a sentence that starts a good conversation.
"Scales to 100k TPS" invites someone to ask how you measured it.

---

## Sequencing notes

**Build M1 and M2 completely before anything else.** A rock-solid ledger with a
correct idempotent API and nothing else is a better project than seven
subsystems that each mostly work. If you have two weeks, that is the project.

Two adjustments to the obvious plan, both worth making:

- **Outbox from day one** (behind an interface), not when Kafka arrives.
- **Spark earns its place through the shared streaming/batch code path**, not
  through data volume. Write `account_features()` once and call it from both the
  streaming job and the backfill; that is the thing to demo. If you cannot point
  at what Spark does that a single node could not, an interviewer who works on
  Spark will find the bottom of the claim in two questions. See
  `04-pipeline.md`.

---

## On the résumé bullet

A draft of the bullet:

> **LedgerFlow — Real-Time Financial Infrastructure Platform**
> Built an event-driven financial platform processing simulated payment events
> through Kafka and FastAPI, implementing an ACID double-entry ledger with
> idempotent APIs and *exactly-once* financial operations…

Change **exactly-once**. It is not achievable across a network boundary, and
some interviewers treat the claim as a tell. What you actually built is better
and more specific:

> Built an event-driven financial platform on FastAPI, Postgres, and Redpanda:
> an append-only double-entry ledger with balance invariants enforced by
> deferred database constraints, crash-safe idempotent APIs, and a transactional
> outbox giving at-least-once delivery with consumer-side deduplication. Built
> the analytics tier on Spark Structured Streaming and Delta Lake, with
> watermarked event-time windows, checkpoint-recoverable state, and one feature
> definition shared by the streaming and backfill paths for point-in-time
> correctness. Added versioned transaction normalization, signed webhooks with
> backoff and dead-letter redrive, and bitemporal balance reconstruction. Load
> tested to N transactions/sec (p99 write latency Xms).

Every clause there is a question you can answer for twenty minutes, and none of
them is a claim that falls apart under one follow-up.

---

## Questions to be able to answer cold

- Why an outbox instead of publishing to Kafka after the commit?
- Where exactly does the idempotency record get written, and why does the
  placement matter?
- Why is the balance trigger `DEFERRABLE INITIALLY DEFERRED`?
- Why partition by `account_id`? What breaks when one account is huge?
- Two concurrent transfers from the same account — what stops an overdraft, and
  which isolation anomaly are you defending against?
- Why is `exactly-once` the wrong phrase, and what do you have instead?
- What happens when a consumer is down for six hours?
- How do you fix a transaction posted with the wrong amount last Tuesday?
- `effective_at` vs `recorded_at` — when do they diverge and who cares?
- Why not put the ledger in Kafka and make Postgres the projection?
- Why Spark, when one machine could handle this volume?
- What does `withWatermark("effective_at", "2 hours")` actually do to state, and
  what happens to an event that arrives three hours late?
- What is in a Structured Streaming checkpoint, and why is the path versioned?
- Your streaming features and your training features are computed by the same
  function — why does that matter, and what breaks if they are not?
