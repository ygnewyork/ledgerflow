# LedgerFlow

[![tests](https://github.com/ygnewyork/ledgerflow/actions/workflows/tests.yml/badge.svg)](https://github.com/ygnewyork/ledgerflow/actions/workflows/tests.yml)

A real-time financial event platform: an HTTP API that ingests transaction
events, posts them to an immutable double-entry ledger, fans them out through a
durable event stream, and exposes balances, analytics, and webhooks to
developers.

**Status: running.** The ledger, API, workers, Spark jobs, and dashboard are
built and tested — 99 tests against a real PostgreSQL, plus 14 SQL invariant
assertions. See [Running it](#running-it) below and
[`docs/05-roadmap.md`](docs/05-roadmap.md) for what each milestone delivered.

---

## The one-sentence version

> Money movement is recorded exactly once, is never silently lost or
> double-counted, and every derived number (balance, aggregate, fraud signal)
> can be recomputed from the log.

Everything below exists to defend that sentence.

---

![The LedgerFlow dashboard: system health tiles, a 90-day balance reconstruction
with a time-travel slider, fraud signals, spend by category, and a ledger
explorer showing a transaction's entries balancing.](docs/images/dashboard.png)

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
                  └──────────► Delta Lake / Spark ──► features + backfills
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
corrected with a reversing entry, never an `UPDATE`. See `migrations/001_core.sql`.

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

## Measured

On 4 vCPUs with Postgres on the same box, one uvicorn worker, full durability:
**192 write txn/s, p50 10.3 ms, p99 12.9 ms**, zero errors, ledger reconciled
clean. Against a *single* funding account it is 55 txn/s — every purchase from
one account serializes on that account's row lock, which is what stops two
concurrent transfers from both passing the same overdraft check.

The numbers are small because the hardware is. The useful part was what the
load test exposed: a balance cache that was never written (45x on reads once it
was), a rate limiter that allowed everything whenever Redis was absent, and
five redundant database round trips per write. Full write-up, including what
was ruled out and what would actually move the number:
[`docs/06-load-test.md`](docs/06-load-test.md).

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

## Running it

Needs **Python 3.11+** and **PostgreSQL 16**. Redis is optional; Kafka is
optional too — the `EventStream` port has a Postgres-backed implementation, so
the whole platform runs on one database.

### macOS

Paste these **one line at a time** — an interactive zsh does not strip `#`
comments, so a trailing comment becomes an argument.

```
brew install python postgresql@16 redis
brew services start postgresql@16
brew services start redis
```

Already have a PostgreSQL from the EDB installer (`/Library/PostgreSQL/...`)?
You do not need Homebrew's. Its binaries sit ahead of Homebrew's on `PATH` and
shadow them, which is the usual cause of "I started 16 but psql says 18".
Any server 16 or newer works — `make doctor` reports which one is answering.

```
git clone https://github.com/ygnewyork/ledgerflow
cd ledgerflow
make setup
make db-create
make demo
make serve
```

`make setup` builds `.venv` — that is what gives you `python` and `pip`, which
a clean macOS does not have. `make demo` prints the test API key; paste it into
the dashboard at `http://localhost:8000/dashboard/`.

`make brew-pg-5433` and `make env` both write a `.env`, which `make` reads
automatically — so the setup survives closing the terminal. Nothing to re-export.

Anything unclear: `make doctor` prints which Python, which `psql`, which server
is actually listening, and whether more than one Postgres is installed.

If the running server does not accept your macOS username (an EDB install
usually wants `postgres` and a password), point the app at it:

```
export LEDGERFLOW_DATABASE_URL="postgresql://postgres:YOURPASSWORD@localhost:5432/ledgerflow"
```

### Coming back to it later

A new terminal needs no setup, because `.env` holds the configuration:

```
cd ledgerflow
make doctor
make serve
```

If `doctor` says no server is listening, the database just is not running yet:

```
brew services start postgresql@16
```

To start over from an empty ledger:

```
make demo
```

For your own shell — running `python -m ledgerflow.cli ...` directly rather
than through `make` — load the same file:

```
source .env
```

### Linux, or with Docker

```bash
docker compose up -d        # postgres + redis
cp .env.example .env && source .env
make setup && make demo && make serve
```

### Without make

There is no bare `python` or `pip` on a clean macOS — those names only exist
inside a virtualenv, which is why `make setup` creates one:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[api,dev]"

export LEDGERFLOW_DATABASE_URL="postgresql://$(whoami)@localhost:5432/ledgerflow"

python -m ledgerflow.cli migrate      # schema + merchant dictionary
python -m ledgerflow.cli bootstrap    # tenant, API keys, chart of accounts
python -m ledgerflow.cli loadgen --days 90
python -m ledgerflow.cli worker all   # drain the pipeline once
python -m ledgerflow.cli serve
```

### Poking at it

```bash
# post a transaction
curl -X POST localhost:8000/v1/transactions \
  -H "Authorization: Bearer lf_test_..." \
  -H "Idempotency-Key: order_93842" \
  -d '{"kind":"card_purchase","amount":8437,
       "accounts":{"expense":"groceries","funding":"checking"},
       "descriptor":"SQ *TST* STARBUCKS 800-782-7282 CA"}'

# send it again -- identical response, Idempotent-Replay: true, money moves once
```

- `python -m ledgerflow.bench --key lf_test_... --sweep --funding-accounts 32`
  — the load test ([results](docs/06-load-test.md))
- `http://localhost:8000/docs` — the generated OpenAPI browser, all 22 routes
- `make test` — 99 tests against the real database. The same suite runs in CI
  against a PostgreSQL service container, because a suite that only runs on the
  author's laptop stops running
- `python -m ledgerflow.cli reconcile` — recompute every balance from entries
  and diff against the snapshot cache
- `python -m ledgerflow.spark.parity` — measure online/offline feature skew.
  Exits nonzero when the Spark features and the risk worker's features disagree
  ([what it found](docs/07-spark-run.md))

Kafka instead of Postgres for the stream, and the Spark jobs:

```bash
export LEDGERFLOW_STREAM=kafka LEDGERFLOW_KAFKA_BROKERS=localhost:9092
python -m ledgerflow.spark.jobs streaming --checkpoint ./_checkpoints
python -m ledgerflow.spark.jobs backfill --source ./data/bronze
```

---

## Layout

```
migrations/             001_core · 002_stream · 003_seed
docs/
  01-domain-model.md    objects, invariants, posting rules, bitemporality
  02-data-model.md      the query cookbook (DDL lives in migrations/)
  03-api.md             endpoints, the idempotency protocol, webhooks, versioning
  04-pipeline.md        outbox, topics, consumers, normalization, Spark
  05-roadmap.md         milestones and what each one delivered
  06-load-test.md       measured throughput, and the four bugs it exposed
src/ledgerflow/
  domain/               pure core -- money, ledger, posting rules. no I/O.
  adapters/             psycopg pool, one transaction boundary, every SQL query
  application/          use cases, errors, resource shapes
  api/                  FastAPI, auth, idempotency, rate limits, versioning
  stream/               EventStream port + Postgres and Kafka adapters
  workers/              outbox relay, normalizer, risk, webhooks, consumer runner
  normalization/        descriptor cleaning and merchant resolution
  features/             window definitions shared by the online and offline paths
  spark/                streaming + backfill jobs, point-in-time training joins
  dashboard/static/     the operator UI -- vanilla JS, no build step
tests/                  99 tests + 14 SQL invariant assertions
```

The `domain/` package imports nothing from FastAPI, SQLAlchemy, or Kafka, and
`features/windows.py` is imported by both the streaming risk worker and the
Spark jobs — so there is exactly one definition of "1h spend" in the system.

```bash
# domain invariants -- no dependencies, no database
python3 -m unittest discover -s tests -v

# everything -- needs a database
pytest

# schema invariants, straight SQL
psql -d ledgerflow_test -f migrations/001_core.sql
psql -d ledgerflow_test -f tests/test_schema_invariants.sql
```

The test worth reading first is
[`tests/test_idempotency.py::test_crash_between_commit_and_response`](tests/test_idempotency.py):
it commits a transfer, raises at the exact instant the process would die, then
retries through the real API and asserts the money moved once.

---

## Reading order

1. [`docs/01-domain-model.md`](docs/01-domain-model.md) — the objects and the
   invariants they hold
2. [`migrations/001_core.sql`](migrations/001_core.sql) — the same invariants,
   enforced by the database
3. [`docs/03-api.md`](docs/03-api.md) — the idempotency protocol, in detail
4. [`docs/04-pipeline.md`](docs/04-pipeline.md) — outbox, consumers, Spark
5. [`docs/05-roadmap.md`](docs/05-roadmap.md) — what is built, what is not, and
   the questions this project should make answerable
