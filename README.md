# LedgerFlow

[![tests](https://github.com/ygnewyork/ledgerflow/actions/workflows/tests.yml/badge.svg)](https://github.com/ygnewyork/ledgerflow/actions/workflows/tests.yml)

A real-time financial event platform, where an HTTP API ingests transaction
events, posts them to an immutable double-entry ledger, fans them out through a
durable event stream, and exposes balances, analytics, and webhooks to
developers.

---

> Money movement is recorded exactly once, is never silently lost or
> double-counted, and every derived number (balance, aggregate, fraud signal)
> can be recomputed from the log.

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

## Layout

```
migrations/             001_core · 002_stream · 003_seed
src/ledgerflow/
  domain/               pure core: money, ledger, posting rules.
  adapters/             psycopg pool, one transaction boundary, every SQL query
  application/          use cases, errors, resource shapes
  api/                  FastAPI, auth, idempotency, rate limits, versioning
  stream/               EventStream port + Postgres and Kafka adapters
  workers/              outbox relay, normalizer, risk, webhooks, consumer runner
  normalization/        descriptor cleaning and merchant resolution
  features/             window definitions shared by the online and offline paths
  spark/                streaming + backfill jobs, point-in-time training joins
  dashboard/static/     the operator UI, vanilla JS, no build step
tests/                  99 tests + 14 SQL invariant assertions
```
