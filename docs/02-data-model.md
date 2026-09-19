# Data model

The DDL lives in [`migrations/`](../migrations) so there is exactly one copy and
it cannot drift from what the code runs against:

| File | Contents |
|---|---|
| `001_core.sql` | tenancy, accounts, the ledger, idempotency, outbox, normalization, risk, webhooks |
| `002_stream.sql` | the Postgres-backed event stream used for local development |
| `003_seed.sql` | a starter merchant dictionary |

Apply them with `python -m ledgerflow.cli migrate`, or by hand in order.

The commentary on each table is inline in `001_core.sql`. What follows are the
queries that define how the ledger is read.

## The query cookbook

```sql

-- Current balance, from the snapshot cache plus the tail of the ledger.
-- O(entries since last snapshot), not O(all entries).
--
--   $1 = account_id
--
-- WITH snap AS (
--     SELECT up_to_entry_id, balance_minor
--       FROM balance_snapshots
--      WHERE account_id = $1
--      ORDER BY up_to_entry_id DESC
--      LIMIT 1
-- )
-- SELECT COALESCE((SELECT balance_minor FROM snap), 0)
--      + COALESCE(SUM(CASE WHEN e.direction = a.normal_balance
--                          THEN e.amount_minor ELSE -e.amount_minor END), 0) AS balance_minor
--   FROM entries e
--   JOIN accounts a ON a.id = e.account_id
--  WHERE e.account_id = $1
--    AND e.id > COALESCE((SELECT up_to_entry_id FROM snap), 0);


-- Bitemporal time travel. Two axes, two questions:
--
--   $2 = as_of        (business time)  — "what was true"
--   $3 = as_known_at  (system time)    — "what we believed"
--
-- Pass only $2 for the corrected history. Pass both to reproduce exactly what
-- the dashboard showed on some past date, backdated corrections excluded.
--
-- SELECT COALESCE(SUM(CASE WHEN e.direction = a.normal_balance
--                          THEN e.amount_minor ELSE -e.amount_minor END), 0) AS balance_minor
--   FROM entries e
--   JOIN accounts a ON a.id = e.account_id
--  WHERE e.account_id  = $1
--    AND e.effective_at <= $2
--    AND e.recorded_at  <= COALESCE($3, 'infinity'::timestamptz);


-- Reconciliation: recompute every balance from raw entries and diff against
-- the snapshot cache. Runs nightly. Any nonzero row is a bug worth paging for.
--
-- SELECT a.id,
--        s.balance_minor                                        AS cached,
--        SUM(CASE WHEN e.direction = a.normal_balance
--                 THEN e.amount_minor ELSE -e.amount_minor END) AS recomputed
--   FROM accounts a
--   JOIN entries e ON e.account_id = a.id
--   LEFT JOIN LATERAL (
--        SELECT balance_minor, up_to_entry_id FROM balance_snapshots
--         WHERE account_id = a.id ORDER BY up_to_entry_id DESC LIMIT 1
--   ) s ON TRUE
--  WHERE e.id <= s.up_to_entry_id
--  GROUP BY a.id, s.balance_minor
-- HAVING s.balance_minor IS DISTINCT FROM
--        SUM(CASE WHEN e.direction = a.normal_balance
--                 THEN e.amount_minor ELSE -e.amount_minor END);


-- The global invariant. Across the whole ledger, for every currency, the
-- debits and credits must cancel. If this ever returns a row, stop the world.
--
-- SELECT currency,
--        SUM(CASE WHEN direction = 'debit' THEN amount_minor ELSE -amount_minor END) AS drift
--   FROM entries
--  GROUP BY currency
-- HAVING SUM(CASE WHEN direction = 'debit' THEN amount_minor ELSE -amount_minor END) <> 0;
```
