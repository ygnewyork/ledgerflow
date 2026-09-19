# LedgerFlow

A real-time financial event platform: an HTTP API that ingests transaction
events, posts them to an immutable double-entry ledger, fans them out through a
durable event stream, and exposes balances, analytics, and webhooks to
developers.

**Status: design phase.** The schema, domain model, API contract, and the pure
core objects are written and tested. The API, workers, and dashboard are not
built yet — see [`docs/05-roadmap.md`](docs/05-roadmap.md) for the milestones
and what "done" means for each.

---

## The one-sentence version

> Money movement is recorded exactly once, is never silently lost or
> double-counted, and every derived number (balance, aggregate, fraud signal)
> can be recomputed from the log.

Everything below exists to defend that sentence.

---

## Architecture

```
  ┌──────────┐   Idempotency-Key      ┌───────────────────────────────┐
  │ SDK /    │ ─────────────────────► │  API  (FastAPI)               │
  │ curl     │ ◄───────────────────── │  auth · validate · rate-limit │
  └──────────┘   replayed response    └───────────────┬───────────────┘
                                                      │
                          ┌───────────────────────────┴──────────────────┐
                          │  ONE Postgres transaction                    │
                          │   1. idempotency_keys row  (owns the request)│
                          │   2. transactions + entries (the ledger)     │
                          │   3. outbox row            (the event)       │
                          │   COMMIT  ← the only durability boundary     │
                          └───────────────────────────┬──────────────────┘
                                                      │
                                          ┌───────────▼───────────┐
                                          │  outbox relay          │
                                          │  (poll or Debezium CDC)│
                                          └───────────┬───────────┘
                                                      │ at-least-once
                                          ┌───────────▼───────────┐
                                          │  Redpanda / Kafka      │
                                          │  key = account_id      │
                                          └───┬────────┬───────┬───┘
                                              │        │       │
                   ┌──────────────────────────┘        │       └────────────────┐
                   ▼                                   ▼                        ▼
         ┌──────────────────┐              ┌────────────────────┐   ┌────────────────────┐
         │ normalizer       │              │ risk / features    │   │ webhook dispatcher │
         │ raw → canonical  │              │ Redis windows      │   │ HMAC · backoff·DLQ │
         │ merchant + cat   │              │ rules → signals    │   └────────────────────┘
         └────────┬─────────┘              └─────────┬──────────┘
                  │                                  │
                  ▼                                  ▼
         normalized_transactions             fraud_signals
                  │
                  └──────────► Parquet / DuckDB ──► analytics + backfills
```

The consumers are all **derived state**. If any of them is wrong, you delete its
output and replay the stream. Postgres holds the only state that cannot be
rebuilt: the ledger and the idempotency records.

---

## The four ideas worth building this for

### 1. The outbox, not "write to Postgres *and* Kafka"

You cannot atomically commit to a database and publish to a broker. The
temptation is to `INSERT ...; COMMIT; producer.send(...)`, and then the process
dies in the gap and the event is lost forever while the money moved.

So the event is written **as a row in the same transaction** as the ledger
entries, and a separate relay publishes it. Publication becomes at-least-once,
and every consumer dedupes on `event_id`. This is the single most important
structural decision in the project, and it is the one most portfolio versions of
this idea get wrong.

### 2. Idempotency that survives a crash *after* commit

The interesting failure isn't the duplicate request. It's:

```
request → DB commits → process dies → client retries
```

That only replays correctly if the idempotency record was committed *inside the
same transaction as the effect*. If it is written before (own transaction) you
can end up with a key claimed for work that never happened; if written after,
you can end up with money moved and no record that it was. There is exactly one
correct placement. See `docs/03-api.md`.

### 3. A ledger whose invariant is enforced by the database

`Σ debits = Σ credits`, per transaction, per currency — checked by a
`DEFERRABLE INITIALLY DEFERRED` constraint trigger that fires at `COMMIT`, after
all the entries of a transaction exist. Entries are append-only; a mistake is
corrected with a reversing entry, never an `UPDATE`. See `docs/02-schema.sql`.

### 4. Bitemporal balances

A balance has two time axes:

- `effective_at` — when the money moved (business time)
- `recorded_at` — when we learned about it (system time)

"What was the balance on Aug 1?" and "what did we *believe* the balance was on
Aug 1?" are different questions, and they diverge the moment a backdated or
late-arriving transaction lands. Storing both makes the time-travel slider in
the dashboard honest instead of decorative, and it is the reason reconciliation
is possible at all.

---

## Failure modes handled

| Failure | Mechanism |
|---|---|
| Duplicate API request | `Idempotency-Key` + request fingerprint; replayed response |
| Retry after post-commit crash | Idempotency row committed in the effect's transaction |
| Conflicting reuse of a key | `409 idempotency_key_reuse` (same key, different body) |
| Concurrent in-flight retry | `409 request_in_flight` + lease expiry |
| Event lost between DB and broker | Transactional outbox + relay |
| Duplicate stream delivery | `processed_events(consumer, event_id)` unique, same txn as effect |
| Out-of-order events | Partition by `account_id`; ledger keyed on `effective_at`, not arrival |
| Consumer crash mid-batch | At-least-once + dedupe = safe restart from last committed offset |
| Poison message | Bounded retries with jitter, then DLQ topic + `/v1/dead_letters` |
| Malformed payload | Rejected at the API edge; never enters the stream |
| Unbalanced posting | Domain object refuses to construct; DB trigger refuses to commit |
| Webhook endpoint down | Exponential backoff (1m → 24h), delivery log, manual redrive |
| Webhook replay attack | HMAC over `timestamp.body`, 5-minute tolerance window |
| Bad normalizer release | Raw retained forever; bump `normalizer_version` and replay |
| Drifted derived state | Reconciliation job: recompute balances from entries, diff, alert |

---

## Deliberately out of scope

- **Multi-currency FX.** Currencies are kept separate; the ledger will not let
  you balance USD against EUR. No conversion, no rate table.
- **Real bank connectivity.** Ingest is synthetic + replayed fixtures.
- **ML fraud models.** Rules and windowed features first. A model without a
  point-in-time-correct feature store is a demo, not a system.
- **"AI for your bank statements."** The infrastructure is the project.

---

## Layout

```
docs/
  01-domain-model.md    objects, invariants, posting rules, bitemporality
  02-schema.sql         full DDL, triggers, time-travel and reconciliation queries
  03-api.md             endpoints, the idempotency protocol, webhooks, versioning
  04-pipeline.md        outbox, topics, consumers, normalization, features
  05-roadmap.md         milestones with "done when" criteria
src/ledgerflow/domain/  pure-Python core (no I/O, no framework)
  money.py              integer minor units, currency-safe arithmetic
  ledger.py             Account, Entry, JournalTransaction, the balance invariant
  posting.py            event kind -> ledger entries, as a registry of rules
tests/test_domain.py    28 tests, no dependencies required
```

The `domain/` package imports nothing from FastAPI, SQLAlchemy, or Kafka. That
is not architecture astronomy — it is what keeps the accounting rules testable
in milliseconds and readable by someone who has never seen this codebase.

```
# domain invariants -- no dependencies needed
python3 -m unittest discover -s tests -v

# schema invariants -- needs a Postgres 16 database
createdb ledgerflow_test
psql -d ledgerflow_test -v ON_ERROR_STOP=1 -f docs/02-schema.sql
psql -d ledgerflow_test -f tests/test_schema_invariants.sql
```

Both suites pass as of the current commit: 28 domain tests, and 14 schema
assertions verified against PostgreSQL 16.13 — including that an unbalanced
transaction is rejected at `COMMIT`, that entries refuse `UPDATE` and `DELETE`,
and that a transaction cannot be reversed twice.

---

## Reading order

1. [`docs/01-domain-model.md`](docs/01-domain-model.md) — what the objects are and
   which invariants they hold
2. [`docs/02-schema.sql`](docs/02-schema.sql) — the same invariants, enforced by
   the database
3. [`docs/03-api.md`](docs/03-api.md) — the idempotency protocol, in detail
4. [`docs/04-pipeline.md`](docs/04-pipeline.md) — outbox, consumers, normalization
5. [`docs/05-roadmap.md`](docs/05-roadmap.md) — build order, and the questions this
   project should make answerable
