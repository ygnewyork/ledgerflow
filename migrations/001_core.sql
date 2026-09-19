-- LedgerFlow — core schema
--
-- Conventions
--   * ids are ULIDs stored as TEXT with a type prefix (acct_, txn_, evt_)
--     -> sortable by creation time, safe in URLs, no sequence leakage
--   * money is BIGINT minor units + a currency code. never NUMERIC, never FLOAT
--   * every table that a tenant can read carries tenant_id (row-level scoping)
--   * timestamps are TIMESTAMPTZ. there is no such thing as a naive timestamp here

CREATE EXTENSION IF NOT EXISTS pg_trgm;     -- merchant fuzzy matching
CREATE EXTENSION IF NOT EXISTS citext;

-- ============================================================================
-- Tenancy and auth
-- ============================================================================

CREATE TABLE tenants (
    id          TEXT PRIMARY KEY,
    name        TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TYPE api_mode AS ENUM ('test', 'live');

CREATE TABLE api_keys (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT        NOT NULL REFERENCES tenants(id),
    mode         api_mode    NOT NULL,
    -- the plaintext key (lf_test_xxx) is shown exactly once, at creation.
    -- we store only a hash, so a database dump does not hand over live credentials.
    key_hash     BYTEA       NOT NULL,
    key_prefix   TEXT        NOT NULL,   -- 'lf_test_9fA2'  for display in the dashboard
    key_last4    TEXT        NOT NULL,
    api_version  DATE        NOT NULL,   -- pinned at creation; header can override per-request
    revoked_at   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX api_keys_hash_idx ON api_keys(key_hash);

-- test and live data share a schema but never share rows: mode is part of the
-- tenant scope on every query. simpler than two databases, and the demo can
-- show both side by side.

-- ============================================================================
-- Chart of accounts
-- ============================================================================

CREATE TYPE account_type   AS ENUM ('asset', 'liability', 'equity', 'revenue', 'expense');
CREATE TYPE balance_side   AS ENUM ('debit', 'credit');
CREATE TYPE account_status AS ENUM ('open', 'frozen', 'closed');

CREATE TABLE accounts (
    id             TEXT PRIMARY KEY,
    tenant_id      TEXT           NOT NULL REFERENCES tenants(id),
    mode           api_mode       NOT NULL,
    external_id    TEXT,                         -- the caller's own identifier
    name           TEXT           NOT NULL,      -- 'Assets:Checking'
    type           account_type   NOT NULL,
    normal_balance balance_side   NOT NULL,
    currency       TEXT           NOT NULL CHECK (currency = lower(currency)),
    status         account_status NOT NULL DEFAULT 'open',
    -- NULL = no floor (expense/revenue accounts). 0 = may not go negative.
    minimum_balance BIGINT,
    metadata       JSONB          NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),

    CONSTRAINT normal_balance_matches_type CHECK (
        (type IN ('asset', 'expense')                 AND normal_balance = 'debit') OR
        (type IN ('liability', 'equity', 'revenue')   AND normal_balance = 'credit')
    )
);
CREATE UNIQUE INDEX accounts_external_idx
    ON accounts(tenant_id, mode, external_id) WHERE external_id IS NOT NULL;

-- ============================================================================
-- The ledger
-- ============================================================================

CREATE TYPE txn_status AS ENUM ('posted', 'reversed');

CREATE TABLE transactions (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT        NOT NULL REFERENCES tenants(id),
    mode         api_mode    NOT NULL,
    kind         TEXT        NOT NULL,       -- card_purchase | transfer | fee | ...
    status       txn_status  NOT NULL DEFAULT 'posted',
    effective_at TIMESTAMPTZ NOT NULL,       -- business time: when money moved
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),  -- system time: when we learned
    reverses_id  TEXT        REFERENCES transactions(id),
    metadata     JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX transactions_tenant_time_idx ON transactions(tenant_id, mode, effective_at DESC);
CREATE UNIQUE INDEX transactions_reverses_idx ON transactions(reverses_id) WHERE reverses_id IS NOT NULL;
--   ^ a transaction can be reversed at most once. without this you can reverse
--     a reversal of a reversal and quietly double the money back.

CREATE TABLE entries (
    -- BIGSERIAL, not a ULID: this is the ledger's global ordering and the cursor
    -- that balance snapshots and stream replay are expressed in terms of.
    id             BIGSERIAL PRIMARY KEY,
    transaction_id TEXT         NOT NULL REFERENCES transactions(id),
    account_id     TEXT         NOT NULL REFERENCES accounts(id),
    direction      balance_side NOT NULL,
    amount_minor   BIGINT       NOT NULL CHECK (amount_minor > 0),
    currency       TEXT         NOT NULL,
    effective_at   TIMESTAMPTZ  NOT NULL,
    recorded_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- the workhorse index: every balance query is "this account, up to this cursor"
CREATE INDEX entries_account_cursor_idx ON entries(account_id, id);
-- time-travel by business time
CREATE INDEX entries_account_effective_idx ON entries(account_id, effective_at, id);
CREATE INDEX entries_txn_idx ON entries(transaction_id);

-- ---------------------------------------------------------------------------
-- Invariant 1: entries are append-only
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION entries_are_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'entries are append-only; correct mistakes with a reversing entry'
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER entries_no_mutation
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION entries_are_immutable();

-- ---------------------------------------------------------------------------
-- Invariant 2: Σ debits = Σ credits, per transaction, per currency
--
-- DEFERRABLE INITIALLY DEFERRED is the whole trick. A row-level check that ran
-- immediately would fail on the first INSERT of every transaction, because a
-- half-inserted transaction is necessarily unbalanced. Deferring to COMMIT
-- means the check sees the completed transaction — and means an unbalanced
-- write cannot be committed by ANY client: not this API, not a migration, not
-- someone in psql at 2am.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION assert_transaction_balanced() RETURNS trigger AS $$
DECLARE
    bad RECORD;
BEGIN
    SELECT currency,
           SUM(amount_minor) FILTER (WHERE direction = 'debit')  AS debits,
           SUM(amount_minor) FILTER (WHERE direction = 'credit') AS credits
      INTO bad
      FROM entries
     WHERE transaction_id = NEW.transaction_id
     GROUP BY currency
    HAVING COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'debit'), 0)
        <> COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'credit'), 0)
     LIMIT 1;

    IF FOUND THEN
        RAISE EXCEPTION
            'unbalanced transaction % in %: debits=% credits=%',
            NEW.transaction_id, bad.currency,
            COALESCE(bad.debits, 0), COALESCE(bad.credits, 0)
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER entries_must_balance
    AFTER INSERT ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_transaction_balanced();

-- ---------------------------------------------------------------------------
-- Balance snapshots — a cache, and only a cache.
-- Dropping this table loses nothing but speed.
-- ---------------------------------------------------------------------------
CREATE TABLE balance_snapshots (
    account_id    TEXT        NOT NULL REFERENCES accounts(id),
    up_to_entry_id BIGINT     NOT NULL,   -- inclusive cursor into entries.id
    balance_minor BIGINT      NOT NULL,   -- signed, relative to the account's normal side
    currency      TEXT        NOT NULL,
    taken_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, up_to_entry_id)
);

-- ============================================================================
-- Idempotency
-- ============================================================================

CREATE TYPE idem_status AS ENUM ('in_progress', 'completed');

CREATE TABLE idempotency_keys (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT        NOT NULL REFERENCES tenants(id),
    api_key_id      TEXT        NOT NULL REFERENCES api_keys(id),
    idempotency_key TEXT        NOT NULL,
    -- sha256 of (method, path, canonicalized body). reusing a key with a
    -- different body is a client bug we must surface, not silently satisfy.
    request_hash    BYTEA       NOT NULL,
    status          idem_status NOT NULL,
    -- the stored response, replayed verbatim on retry
    response_code   SMALLINT,
    response_body   JSONB,
    resource_id     TEXT,
    -- lease: an in_progress row older than this was abandoned by a dead process
    lease_expires_at TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL   -- keys are recyclable after 24h
);

-- scoped to the API key, not global: two tenants may use 'transfer_1' freely
CREATE UNIQUE INDEX idempotency_keys_scope_idx
    ON idempotency_keys(api_key_id, idempotency_key);
CREATE INDEX idempotency_keys_expiry_idx ON idempotency_keys(expires_at);

-- ============================================================================
-- Transactional outbox
--
-- Written in the SAME transaction as the ledger entries. This is what makes
-- "the ledger committed but the event was lost" impossible.
-- ============================================================================

CREATE TABLE outbox (
    id             BIGSERIAL PRIMARY KEY,
    event_id       TEXT        NOT NULL UNIQUE,   -- evt_… ; consumers dedupe on this
    tenant_id      TEXT        NOT NULL REFERENCES tenants(id),
    mode           api_mode    NOT NULL,
    -- the Kafka partition key. account_id => per-account ordering, which is the
    -- only ordering guarantee that actually means something here.
    partition_key  TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,          -- transaction.created, ...
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT         NOT NULL DEFAULT 0
);
-- partial index: the relay only ever scans unpublished rows, so this index
-- stays tiny no matter how many events have been emitted
CREATE INDEX outbox_unpublished_idx ON outbox(id) WHERE published_at IS NULL;

-- ============================================================================
-- Consumer-side dedupe
--
-- Inserted in the same transaction as whatever side effect the consumer
-- performs. If the insert conflicts, the event was already handled and the
-- consumer skips it. At-least-once delivery + this table = effectively-once
-- processing.
-- ============================================================================

CREATE TABLE processed_events (
    consumer_group TEXT        NOT NULL,
    event_id       TEXT        NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
);

CREATE TABLE dead_letters (
    id             TEXT PRIMARY KEY,
    consumer_group TEXT        NOT NULL,
    event_id       TEXT        NOT NULL,
    topic          TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    error_class    TEXT        NOT NULL,
    error_detail   TEXT,
    attempts       INT         NOT NULL,
    first_failed_at TIMESTAMPTZ NOT NULL,
    last_failed_at  TIMESTAMPTZ NOT NULL,
    resolved_at    TIMESTAMPTZ
);
CREATE INDEX dead_letters_open_idx ON dead_letters(consumer_group, last_failed_at)
    WHERE resolved_at IS NULL;

-- ============================================================================
-- Ingest + normalization  (the Plaid-shaped half)
-- ============================================================================

CREATE TABLE raw_transactions (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT        NOT NULL REFERENCES tenants(id),
    mode         api_mode    NOT NULL,
    account_id   TEXT        NOT NULL REFERENCES accounts(id),
    -- the posting this descriptor arrived with. an explicit FK, because
    -- matching raw rows to transactions on (timestamp, amount) is a heuristic
    -- that silently picks the wrong row the first time a customer buys two
    -- identical coffees in the same second.
    transaction_id TEXT      REFERENCES transactions(id),
    -- exactly what the caller sent, forever, untouched.
    -- every normalized row is derivable from this; the reverse is not true.
    payload      JSONB       NOT NULL,
    descriptor   TEXT        NOT NULL,   -- 'SQ *TST* STARBUCKS 800-782-7282 CA'
    amount_minor BIGINT      NOT NULL,
    currency     TEXT        NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX raw_transactions_txn_idx ON raw_transactions(transaction_id);

CREATE TABLE merchants (
    id            TEXT PRIMARY KEY,
    display_name  TEXT   NOT NULL,       -- 'Starbucks'
    category      TEXT   NOT NULL,       -- 'Food and Drink'
    website       TEXT,
    logo_url      TEXT
);

CREATE TABLE merchant_aliases (
    id           BIGSERIAL PRIMARY KEY,
    merchant_id  TEXT NOT NULL REFERENCES merchants(id),
    pattern      TEXT NOT NULL,          -- cleaned descriptor token, e.g. 'starbucks'
    source       TEXT NOT NULL,          -- seed | learned | manual
    weight       REAL NOT NULL DEFAULT 1.0
);
-- trigram index turns "which merchant is this garbage string" into an
-- indexed similarity search instead of a full-table scan
CREATE INDEX merchant_aliases_trgm_idx ON merchant_aliases USING gin (pattern gin_trgm_ops);

CREATE TABLE normalized_transactions (
    id                 TEXT PRIMARY KEY,
    raw_transaction_id TEXT        NOT NULL REFERENCES raw_transactions(id),
    tenant_id          TEXT        NOT NULL REFERENCES tenants(id),
    account_id         TEXT        NOT NULL REFERENCES accounts(id),
    merchant_id        TEXT        REFERENCES merchants(id),
    merchant_name      TEXT,
    category           TEXT,
    confidence         REAL        CHECK (confidence BETWEEN 0 AND 1),
    -- bump this and replay the stream to re-derive every row with new logic.
    -- keeping it lets you diff v3 against v2 before you trust v3.
    normalizer_version INT         NOT NULL,
    normalized_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX normalized_version_idx
    ON normalized_transactions(raw_transaction_id, normalizer_version);

-- ============================================================================
-- Risk
-- ============================================================================

CREATE TABLE fraud_signals (
    id             TEXT PRIMARY KEY,
    tenant_id      TEXT        NOT NULL REFERENCES tenants(id),
    account_id     TEXT        NOT NULL REFERENCES accounts(id),
    transaction_id TEXT        REFERENCES transactions(id),
    rule           TEXT        NOT NULL,   -- velocity_1h | amount_zscore | novel_merchant
    score          REAL        NOT NULL,
    -- the feature values AS OF the moment of evaluation. without this you
    -- cannot explain, reproduce, or later train on the decision.
    features       JSONB       NOT NULL,
    evaluated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX fraud_signals_account_idx ON fraud_signals(account_id, evaluated_at DESC);

-- ============================================================================
-- Webhooks
-- ============================================================================

CREATE TABLE webhook_endpoints (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT        NOT NULL REFERENCES tenants(id),
    mode         api_mode    NOT NULL,
    url          TEXT        NOT NULL,
    secret_hash  BYTEA       NOT NULL,
    enabled_events TEXT[]    NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'enabled',  -- enabled | disabled
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE webhook_deliveries (
    id              TEXT PRIMARY KEY,
    endpoint_id     TEXT        NOT NULL REFERENCES webhook_endpoints(id),
    event_id        TEXT        NOT NULL,
    attempt         INT         NOT NULL DEFAULT 0,
    status          TEXT        NOT NULL,   -- pending | succeeded | failed | exhausted
    response_code   SMALLINT,
    response_body   TEXT,
    error           TEXT,
    next_attempt_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);
CREATE INDEX webhook_deliveries_due_idx ON webhook_deliveries(next_attempt_at)
    WHERE status = 'pending';
CREATE INDEX webhook_deliveries_endpoint_idx ON webhook_deliveries(endpoint_id, created_at DESC);
