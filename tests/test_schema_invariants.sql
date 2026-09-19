-- Schema invariant tests.
--
--   createdb ledgerflow_test
--   psql -d ledgerflow_test -v ON_ERROR_STOP=1 -f migrations/001_core.sql
--   psql -d ledgerflow_test -f tests/test_schema_invariants.sql
--
-- Each block asserts that the DATABASE refuses something, independently of any
-- application code. That independence is the point: these guarantees hold for
-- a migration script and for a human in psql at 2am, not just for the API.

\set ON_ERROR_STOP off
\set QUIET on

BEGIN;

INSERT INTO tenants (id, name) VALUES ('ten_1', 'Test Tenant');
INSERT INTO accounts (id, tenant_id, mode, name, type, normal_balance, currency) VALUES
    ('acct_checking',  'ten_1', 'test', 'Assets:Checking',    'asset',   'debit',  'usd'),
    ('acct_groceries', 'ten_1', 'test', 'Expenses:Groceries', 'expense', 'debit',  'usd'),
    ('acct_income',    'ten_1', 'test', 'Revenue:Income',     'revenue', 'credit', 'usd');

COMMIT;


-- ===========================================================================
\echo '1. a balanced transaction commits'
-- ===========================================================================
BEGIN;
INSERT INTO transactions (id, tenant_id, mode, kind, effective_at)
VALUES ('txn_ok', 'ten_1', 'test', 'card_purchase', '2026-09-17T16:21:00Z');
INSERT INTO entries (transaction_id, account_id, direction, amount_minor, currency, effective_at) VALUES
    ('txn_ok', 'acct_groceries', 'debit',  8437, 'usd', '2026-09-17T16:21:00Z'),
    ('txn_ok', 'acct_checking',  'credit', 8437, 'usd', '2026-09-17T16:21:00Z');
COMMIT;
\echo '   -> expected: COMMIT succeeded'


-- ===========================================================================
\echo '2. the first INSERT of a transaction is momentarily unbalanced -- and that is FINE'
--    This is exactly why the trigger is DEFERRABLE INITIALLY DEFERRED. An
--    immediate check would reject every transaction on its first row.
-- ===========================================================================
BEGIN;
INSERT INTO transactions (id, tenant_id, mode, kind, effective_at)
VALUES ('txn_deferred', 'ten_1', 'test', 'card_purchase', '2026-09-17T16:21:00Z');
INSERT INTO entries (transaction_id, account_id, direction, amount_minor, currency, effective_at)
VALUES ('txn_deferred', 'acct_groceries', 'debit', 5000, 'usd', '2026-09-17T16:21:00Z');
\echo '   -> the debit inserted without error (unbalanced, mid-transaction)'
INSERT INTO entries (transaction_id, account_id, direction, amount_minor, currency, effective_at)
VALUES ('txn_deferred', 'acct_checking', 'credit', 5000, 'usd', '2026-09-17T16:21:00Z');
COMMIT;
\echo '   -> expected: COMMIT succeeded once the credit balanced it'


-- ===========================================================================
\echo '3. an UNBALANCED transaction is rejected at COMMIT'
-- ===========================================================================
BEGIN;
INSERT INTO transactions (id, tenant_id, mode, kind, effective_at)
VALUES ('txn_bad', 'ten_1', 'test', 'card_purchase', '2026-09-17T16:21:00Z');
INSERT INTO entries (transaction_id, account_id, direction, amount_minor, currency, effective_at) VALUES
    ('txn_bad', 'acct_groceries', 'debit',  8437, 'usd', '2026-09-17T16:21:00Z'),
    ('txn_bad', 'acct_checking',  'credit', 8000, 'usd', '2026-09-17T16:21:00Z');
COMMIT;
\echo '   -> expected: ERROR  unbalanced transaction txn_bad in usd: debits=8437 credits=8000'


-- ===========================================================================
\echo '4. balancing ACROSS currencies does not count'
--    Sums to zero if you ignore the currency. Still wrong.
-- ===========================================================================
BEGIN;
INSERT INTO transactions (id, tenant_id, mode, kind, effective_at)
VALUES ('txn_fx', 'ten_1', 'test', 'transfer', '2026-09-17T16:21:00Z');
INSERT INTO entries (transaction_id, account_id, direction, amount_minor, currency, effective_at) VALUES
    ('txn_fx', 'acct_groceries', 'debit',  100, 'usd', '2026-09-17T16:21:00Z'),
    ('txn_fx', 'acct_checking',  'credit', 100, 'eur', '2026-09-17T16:21:00Z');
COMMIT;
\echo '   -> expected: ERROR  unbalanced transaction txn_fx'


-- ===========================================================================
\echo '5. a single-sided entry cannot be committed'
-- ===========================================================================
BEGIN;
INSERT INTO transactions (id, tenant_id, mode, kind, effective_at)
VALUES ('txn_single', 'ten_1', 'test', 'card_purchase', '2026-09-17T16:21:00Z');
INSERT INTO entries (transaction_id, account_id, direction, amount_minor, currency, effective_at)
VALUES ('txn_single', 'acct_groceries', 'debit', 100, 'usd', '2026-09-17T16:21:00Z');
COMMIT;
\echo '   -> expected: ERROR  unbalanced transaction txn_single'


-- ===========================================================================
\echo '6. entries are append-only: UPDATE is refused'
-- ===========================================================================
UPDATE entries SET amount_minor = 1 WHERE transaction_id = 'txn_ok';
\echo '   -> expected: ERROR  entries are append-only'


-- ===========================================================================
\echo '7. entries are append-only: DELETE is refused'
-- ===========================================================================
DELETE FROM entries WHERE transaction_id = 'txn_ok';
\echo '   -> expected: ERROR  entries are append-only'


-- ===========================================================================
\echo '8. a zero or negative amount is refused'
-- ===========================================================================
BEGIN;
INSERT INTO transactions (id, tenant_id, mode, kind, effective_at)
VALUES ('txn_neg', 'ten_1', 'test', 'card_purchase', '2026-09-17T16:21:00Z');
INSERT INTO entries (transaction_id, account_id, direction, amount_minor, currency, effective_at)
VALUES ('txn_neg', 'acct_groceries', 'debit', -100, 'usd', '2026-09-17T16:21:00Z');
COMMIT;
\echo '   -> expected: ERROR  violates check constraint "entries_amount_minor_check"'


-- ===========================================================================
\echo '9. an account type cannot contradict its normal balance'
-- ===========================================================================
INSERT INTO accounts (id, tenant_id, mode, name, type, normal_balance, currency)
VALUES ('acct_wrong', 'ten_1', 'test', 'Assets:Nonsense', 'asset', 'credit', 'usd');
\echo '   -> expected: ERROR  violates check constraint "normal_balance_matches_type"'


-- ===========================================================================
\echo '10. a transaction can be reversed at most once'
--     Without this, a reversal of a reversal of a reversal quietly doubles the
--     money back.
-- ===========================================================================
BEGIN;
INSERT INTO transactions (id, tenant_id, mode, kind, effective_at, reverses_id)
VALUES ('txn_rev1', 'ten_1', 'test', 'card_purchase.reversal', '2026-09-18T00:00:00Z', 'txn_ok');
INSERT INTO entries (transaction_id, account_id, direction, amount_minor, currency, effective_at) VALUES
    ('txn_rev1', 'acct_groceries', 'credit', 8437, 'usd', '2026-09-18T00:00:00Z'),
    ('txn_rev1', 'acct_checking',  'debit',  8437, 'usd', '2026-09-18T00:00:00Z');
COMMIT;
\echo '   -> first reversal committed'

BEGIN;
INSERT INTO transactions (id, tenant_id, mode, kind, effective_at, reverses_id)
VALUES ('txn_rev2', 'ten_1', 'test', 'card_purchase.reversal', '2026-09-18T01:00:00Z', 'txn_ok');
INSERT INTO entries (transaction_id, account_id, direction, amount_minor, currency, effective_at) VALUES
    ('txn_rev2', 'acct_groceries', 'credit', 8437, 'usd', '2026-09-18T01:00:00Z'),
    ('txn_rev2', 'acct_checking',  'debit',  8437, 'usd', '2026-09-18T01:00:00Z');
COMMIT;
\echo '   -> expected: ERROR  duplicate key value violates unique constraint "transactions_reverses_idx"'


-- ===========================================================================
\echo '11. idempotency: the same key cannot be claimed twice'
-- ===========================================================================
INSERT INTO api_keys (id, tenant_id, mode, key_hash, key_prefix, key_last4, api_version)
VALUES ('key_1', 'ten_1', 'test', '\x00'::bytea, 'lf_test_9fA2', 'zQ1x', '2026-09-17');

INSERT INTO idempotency_keys
    (id, tenant_id, api_key_id, idempotency_key, request_hash, status, lease_expires_at, expires_at)
VALUES ('idem_1', 'ten_1', 'key_1', 'transfer_9384abc', '\xaa'::bytea, 'in_progress',
        now() + interval '30 seconds', now() + interval '24 hours');

INSERT INTO idempotency_keys
    (id, tenant_id, api_key_id, idempotency_key, request_hash, status, lease_expires_at, expires_at)
VALUES ('idem_2', 'ten_1', 'key_1', 'transfer_9384abc', '\xbb'::bytea, 'in_progress',
        now() + interval '30 seconds', now() + interval '24 hours');
\echo '   -> expected: ERROR  duplicate key value violates unique constraint "idempotency_keys_scope_idx"'


-- ===========================================================================
\echo '12. consumer dedupe: the same event cannot be claimed twice by a group'
-- ===========================================================================
INSERT INTO processed_events (consumer_group, event_id) VALUES ('normalizer', 'evt_1');
INSERT INTO processed_events (consumer_group, event_id) VALUES ('normalizer', 'evt_1')
ON CONFLICT DO NOTHING;
\echo '   -> ON CONFLICT DO NOTHING absorbed the duplicate: this is the consumer fast-path'

SELECT count(*) AS should_be_1 FROM processed_events WHERE consumer_group = 'normalizer';

-- a DIFFERENT group must still get to process the same event
INSERT INTO processed_events (consumer_group, event_id) VALUES ('risk', 'evt_1');
SELECT count(*) AS should_be_2 FROM processed_events WHERE event_id = 'evt_1';


-- ===========================================================================
\echo '13. the global ledger invariant, over everything committed above'
-- ===========================================================================
SELECT currency,
       SUM(CASE WHEN direction = 'debit' THEN amount_minor ELSE -amount_minor END) AS drift_must_be_zero
  FROM entries
 GROUP BY currency;


-- ===========================================================================
\echo '14. balances use the account normal-balance sign convention'
-- ===========================================================================
SELECT a.name,
       a.normal_balance,
       COALESCE(SUM(CASE WHEN e.direction = a.normal_balance
                         THEN e.amount_minor ELSE -e.amount_minor END), 0) AS balance_minor
  FROM accounts a
  LEFT JOIN entries e ON e.account_id = a.id
 GROUP BY a.id, a.name, a.normal_balance
 ORDER BY a.name;
