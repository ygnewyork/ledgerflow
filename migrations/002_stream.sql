-- The event stream, backed by Postgres.
--
-- This is a local-development transport that mimics the parts of Kafka the
-- system actually depends on: append-only ordered messages, per-partition
-- ordering, and consumer groups that track their own offsets independently.
--
-- It exists so the whole platform runs with `docker compose up postgres` and
-- nothing else. The `EventStream` port in src/ledgerflow/stream/base.py has a
-- Kafka implementation alongside this one; swapping them changes no consumer
-- code, which is the point of having defined the port in the first place.

CREATE TABLE stream_messages (
    -- the offset. monotonic per topic, which is what consumers resume from.
    offset_id     BIGSERIAL PRIMARY KEY,
    topic         TEXT        NOT NULL,
    partition_key TEXT        NOT NULL,
    event_id      TEXT        NOT NULL,
    event_type    TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    published_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX stream_messages_topic_idx ON stream_messages(topic, offset_id);
CREATE INDEX stream_messages_key_idx   ON stream_messages(topic, partition_key, offset_id);

-- One row per (consumer group, topic). Committed AFTER the work transaction,
-- never with the work itself -- the same rule as Kafka offset handling, and
-- for the same reason: committing the offset first turns a crash into silent
-- data loss.
CREATE TABLE consumer_offsets (
    consumer_group TEXT        NOT NULL,
    topic          TEXT        NOT NULL,
    last_offset    BIGINT      NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic)
);

-- Replay bookkeeping: rewinding a consumer group over a range of history is an
-- auditable operation, not an ad-hoc UPDATE someone ran once.
CREATE TABLE replays (
    id             TEXT PRIMARY KEY,
    consumer_group TEXT        NOT NULL,
    topic          TEXT        NOT NULL,
    from_offset    BIGINT      NOT NULL,
    to_offset      BIGINT,
    reason         TEXT        NOT NULL,
    requested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at   TIMESTAMPTZ
);

-- Events that arrived later than the feature pipeline's watermark. Dropping
-- them from an aggregate is correct; dropping them silently is not.
CREATE TABLE late_arrivals (
    id            BIGSERIAL PRIMARY KEY,
    event_id      TEXT        NOT NULL,
    account_id    TEXT        NOT NULL,
    effective_at  TIMESTAMPTZ NOT NULL,
    watermark_at  TIMESTAMPTZ NOT NULL,
    lateness      INTERVAL    NOT NULL,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Materialized online features, written by the risk worker.
-- Redis holds the hot copy; this is the durable one the dashboard reads and
-- the offline pipeline reconciles against.
CREATE TABLE account_features (
    account_id          TEXT        NOT NULL REFERENCES accounts(id),
    window_end          TIMESTAMPTZ NOT NULL,
    spend_1h_minor      BIGINT      NOT NULL DEFAULT 0,
    spend_24h_minor     BIGINT      NOT NULL DEFAULT 0,
    txn_count_1h        INT         NOT NULL DEFAULT 0,
    max_amount_1h_minor BIGINT      NOT NULL DEFAULT 0,
    distinct_merchants_7d INT       NOT NULL DEFAULT 0,
    avg_amount_90d_minor BIGINT     NOT NULL DEFAULT 0,
    stddev_amount_90d   DOUBLE PRECISION NOT NULL DEFAULT 0,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, window_end)
);

-- Rate limiting falls back to this table when Redis is not configured, so the
-- API is never silently unlimited just because a dependency is missing.
CREATE TABLE rate_limit_buckets (
    bucket_key TEXT        PRIMARY KEY,
    tokens     DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
