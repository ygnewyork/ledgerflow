# Running the Spark jobs, and what running them found

Spark 3.5.3, Java 21, `local[*]`, no Delta jars — so these ran on the Parquet
fallback in `spark/session.py`, which is the path a laptop with no Maven access
takes. 234 normalized transactions out of a 90-day demo ledger.

## The three jobs

```bash
# 1. Postgres -> bronze. In production this is the Kafka sink; for a demo it is
#    a snapshot, so the offline pipeline runs without a broker.
python -c "from ledgerflow.spark.export import export_normalized as e; print(e('data/bronze/normalized.json'))"
# 234

# 2. bronze -> gold. Same account_features() the streaming job calls.
python -m ledgerflow.spark.jobs backfill \
    --source data/bronze/normalized.json --source-format json \
    --output data/gold/account_features
# 2,494 feature rows (1h window, 5m slide, so windows overlap heavily)

# 3. labels + gold -> training set, via the point-in-time join.
python -m ledgerflow.spark.jobs training \
    --labels data/labels --features data/gold/account_features \
    --output data/gold/training_set
```

## What the point-in-time join is worth

The first labels I built were stamped with wall-clock `now()`, and the join
reported zero leakage. That result was worthless: every label sat after all
transaction history, so there were no future windows available to leak, and a
naive join would have scored a clean zero too. A guard that cannot fail has not
been tested.

Rebuilt with each card purchase labelled **at its own event time**, which is
what a real training set looks like — the decision was made when the charge
arrived, using only what was known then:

| join | rows using a window that closed *after* the decision |
|---|---|
| naive "newest features per account" | **232 of 232** |
| `point_in_time_join` (the `<=` predicate) | **0 of 230** |

A June 25 decision was being handed September 20 features. That is the failure
the `<=` exists to prevent, and it is total when it happens — not a rounding
error on a few rows.

The as-of join returns 230 rows from 240 labels, and the ten that fall out
split into two very different groups:

- **8 lost to the join itself**, all on `Assets:Cash`. That account has no gold
  rows at all — see the skew section below. These are not a cold start, they
  are a pipeline gap wearing a cold start's clothes.
- **2 lost to the `<=` predicate**: the earliest decision on an account, where
  no window had closed yet. Dropping these is correct. There were no features,
  and inventing a zero would teach the model that "no history" looks like "no
  spending."

Worth separating, because only the second group is the join behaving as
designed.

## The skew this found

`python -m ledgerflow.spark.parity` recomputes every window the Spark job
produced and asks Postgres for the same window through the code path the risk
worker actually uses:

```
compared 2494 windows; 39 disagree (1.6%)

  Assets:Checking (asset) -- online>offline: 15 windows
    worst: 2026-09-15 07:40:00 -> 2026-09-15 08:40:00
      offline spend=     1711 txns=1
      online  spend=   309746 txns=2

  Liabilities:Credit Card (liability) -- offline>online: 24 windows
    worst: 2026-08-21 06:40:00 -> 2026-08-21 07:40:00
      offline spend=     5778 txns=1
      online  spend=        0 txns=0

  Assets:Cash (asset) -- NO offline rows at all
    online: 8 credit entries, spend=12595
```

Two separate mechanisms, and neither is a window-boundary problem — which is
the only thing sharing `features/windows.py` protects against:

1. **Online sees transactions offline never receives.** The offline source is
   `transactions.normalized.v1`, and the normalizer bails on any transaction
   with no descriptor to resolve (`raw_for_transaction` returns `None`). In this
   dataset that is every `deposit`, `transfer` and `opening_balance` — 30
   transactions, 0 of them normalized (against 240 card purchases, 232 of which
   normalized). A transfer out of checking is money
   leaving the account, the online feature counts it, and it is not in bronze
   at all.

2. **Offline counts transactions online deliberately excludes.** The online
   `spend_window` filters `direction = 'credit'` — money leaving this account.
   The normalized payload carries no direction at all, so Spark sums every row
   it sees. A refund *debits* the card, so online correctly excludes it and
   offline adds it to spend.

And one account is invisible end to end: `Assets:Cash` has 8 cash purchases,
none of which carry a card descriptor, so the normalizer skips all 8 and the
offline pipeline has never heard of the account. Not 1.6% wrong — 100% absent.
That is also why the parity check reports wholly-missing funding accounts
separately: comparing only the windows Spark produced would score an account it
never emitted as perfectly clean.

The root cause is one thing said two ways: **`transactions.normalized.v1` is a
card-activity-with-merchant-attribution stream, not a money-movement stream**,
and money-movement features are being built from it.

## The fix, and why it is not in this commit

Features about money movement belong on the ledger entry stream —
`ledger.events.v1` already carries every transaction with its entries, and each
entry has both an `account_id` and a `direction`. The normalized stream should
be joined in for merchant attribution, not used as the source of truth for
amounts.

That is a change to what a topic means, not a patch. Adding `direction` to the
normalized payload would fix mechanism 2 and leave mechanism 1 untouched, which
is worse than the current state: it would look fixed. Emitting one normalized
event per *entry* would fix both and silently double every risk score, since
the risk worker consumes that topic one-event-per-transaction.

So the skew is measured, reported by a check that exits nonzero, and left
visible rather than half-patched.
