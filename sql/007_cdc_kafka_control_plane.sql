-- CDC Kafka control plane: apply cursor, GTID capture, signals, fairness, quarantine

CREATE SCHEMA IF NOT EXISTS cdc_catalog;

DO $$
BEGIN
    CREATE TYPE cdc_catalog.cdc_health_status AS ENUM (
        'healthy',
        'lagging',
        'stale',
        'gap_detected',
        'rebootstrap_pending',
        'snapshot_running',
        'quarantined',
        'failed'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- GTID-first capture cursor (Debezium connect-offsets mirror for observability)
CREATE TABLE IF NOT EXISTS cdc_catalog.capture_position (
    conn_id             TEXT PRIMARY KEY,
    gtid_set            TEXT NOT NULL DEFAULT '',
    binlog_file         TEXT,
    binlog_position     BIGINT,
    kafka_connect_name  TEXT,
    last_event_ts       TIMESTAMPTZ,
    capture_lag_seconds INTEGER NOT NULL DEFAULT 0,
    server_uuid         TEXT,
    status              cdc_catalog.cdc_health_status NOT NULL DEFAULT 'healthy',
    last_error          TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Apply cursor per catalog object (Kafka consumer independent of capture)
CREATE TABLE IF NOT EXISTS cdc_catalog.apply_position (
    catalog_id          BIGINT PRIMARY KEY REFERENCES cdc_catalog.catalog (catalog_id) ON DELETE CASCADE,
    conn_id             TEXT NOT NULL,
    source_schema       TEXT NOT NULL,
    source_table        TEXT NOT NULL,
    kafka_topic         TEXT NOT NULL DEFAULT '',
    kafka_partition     INTEGER NOT NULL DEFAULT 0,
    kafka_offset        BIGINT NOT NULL DEFAULT -1,
    last_applied_gtid   TEXT,
    last_applied_at     TIMESTAMPTZ,
    apply_lag_seconds   INTEGER NOT NULL DEFAULT 0,
    status              cdc_catalog.cdc_health_status NOT NULL DEFAULT 'healthy',
    last_error          TEXT,
    quarantined_at      TIMESTAMPTZ,
    quarantine_reason   TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT apply_position_object_uk UNIQUE (conn_id, source_schema, source_table)
);

CREATE INDEX IF NOT EXISTS apply_position_stale_idx
    ON cdc_catalog.apply_position (status, apply_lag_seconds DESC)
    WHERE status IN ('stale', 'lagging', 'gap_detected');

-- Idempotent apply dedup (at-least-once Kafka delivery)
CREATE TABLE IF NOT EXISTS cdc_catalog.cdc_applied_events (
    event_id            TEXT PRIMARY KEY,
    conn_id             TEXT NOT NULL,
    source_schema       TEXT NOT NULL,
    source_table        TEXT NOT NULL,
    op                  CHAR(1) NOT NULL,
    gtid                TEXT,
    kafka_topic         TEXT,
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,
    applied_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cdc_applied_events_table_idx
    ON cdc_catalog.cdc_applied_events (conn_id, source_schema, source_table, applied_at DESC);

-- Fairness metrics per apply slice (anti-starvation reporting)
CREATE TABLE IF NOT EXISTS cdc_catalog.cdc_run_fairness_metrics (
    run_id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id            TEXT NOT NULL,
    conn_id             TEXT NOT NULL,
    service_tier        TEXT,
    stop_reason         TEXT NOT NULL,
    tables_total        INTEGER NOT NULL DEFAULT 0,
    tables_met_target   INTEGER NOT NULL DEFAULT 0,
    tables_starved      INTEGER NOT NULL DEFAULT 0,
    tables_quiet        INTEGER NOT NULL DEFAULT 0,
    oldest_lag_seconds  INTEGER NOT NULL DEFAULT 0,
    events_seen         BIGINT NOT NULL DEFAULT 0,
    events_applied      BIGINT NOT NULL DEFAULT 0,
    duration_ms         BIGINT NOT NULL DEFAULT 0,
    context             JSONB NOT NULL DEFAULT '{}',
    logged_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cdc_fairness_metrics_batch_idx
    ON cdc_catalog.cdc_run_fairness_metrics (batch_id, conn_id);

CREATE TABLE IF NOT EXISTS cdc_catalog.apply_batch_stats (
    stat_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id            TEXT NOT NULL,
    conn_id             TEXT NOT NULL,
    catalog_id          BIGINT,
    source_schema       TEXT NOT NULL,
    source_table        TEXT NOT NULL,
    service_tier        TEXT,
    events_inserts      BIGINT NOT NULL DEFAULT 0,
    events_updates      BIGINT NOT NULL DEFAULT 0,
    events_deletes      BIGINT NOT NULL DEFAULT 0,
    events_total        BIGINT NOT NULL DEFAULT 0,
    duration_ms         BIGINT NOT NULL DEFAULT 0,
    events_per_minute   BIGINT NOT NULL DEFAULT 0,
    kafka_topic         TEXT,
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,
    is_stale            BOOLEAN NOT NULL DEFAULT false,
    is_starving         BOOLEAN NOT NULL DEFAULT false,
    is_inactive         BOOLEAN NOT NULL DEFAULT false,
    is_quarantined      BOOLEAN NOT NULL DEFAULT false,
    reconciliation_rag  TEXT NOT NULL DEFAULT 'UNKNOWN'
        CHECK (reconciliation_rag IN ('GREEN', 'AMBER', 'RED', 'UNKNOWN')),
    apply_lag_seconds   INTEGER NOT NULL DEFAULT 0,
    apply_position_status TEXT,
    events_seen_in_slice INTEGER NOT NULL DEFAULT 0,
    catalog_active      BOOLEAN,
    cdc_enabled         BOOLEAN,
    capture_lag_seconds INTEGER NOT NULL DEFAULT 0,
    kafka_consumer_lag  BIGINT NOT NULL DEFAULT 0,
    reconcile_row_delta BIGINT,
    catchup_triggered   BOOLEAN NOT NULL DEFAULT false,
    fk_deferred_retries INTEGER NOT NULL DEFAULT 0,
    dedup_skipped       INTEGER NOT NULL DEFAULT 0,
    semaphore           TEXT GENERATED ALWAYS AS (reconciliation_rag) STORED,
    context             JSONB NOT NULL DEFAULT '{}',
    logged_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS apply_batch_stats_batch_idx
    ON cdc_catalog.apply_batch_stats (batch_id, conn_id);

CREATE INDEX IF NOT EXISTS apply_batch_stats_table_idx
    ON cdc_catalog.apply_batch_stats (conn_id, source_schema, source_table, logged_at DESC);

COMMENT ON TABLE cdc_catalog.apply_batch_stats IS
    'Per-table apply slice stats: I/U/D, duration, events/min — keyed by batch_id';

COMMENT ON TABLE cdc_catalog.capture_position IS
    'GTID-first capture cursor mirror; Debezium connect-offsets is source of truth for resume.';
COMMENT ON TABLE cdc_catalog.apply_position IS
    'Per-table Kafka apply cursor; independent lag and quarantine state.';
COMMENT ON TABLE cdc_catalog.cdc_applied_events IS
    'Dedup ledger for at-least-once Kafka apply.';
