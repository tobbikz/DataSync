-- CDC Kafka table reconciliation (separate from capture/apply daemon)

CREATE TABLE IF NOT EXISTS cdc_catalog.reconciliation_run (
    run_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id        TEXT NOT NULL,
    conn_id         TEXT NOT NULL,
    service_tier    TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'ok', 'warn', 'fail')),
    tables_checked  INT NOT NULL DEFAULT 0,
    tables_ok       INT NOT NULL DEFAULT 0,
    tables_warn     INT NOT NULL DEFAULT 0,
    tables_fail     INT NOT NULL DEFAULT 0,
    stale_tables    INT NOT NULL DEFAULT 0,
    context         JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS reconciliation_run_conn_started_idx
    ON cdc_catalog.reconciliation_run (conn_id, started_at DESC);

CREATE TABLE IF NOT EXISTS cdc_catalog.reconciliation_result (
    result_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              BIGINT NOT NULL REFERENCES cdc_catalog.reconciliation_run (run_id) ON DELETE CASCADE,
    catalog_id          BIGINT,
    conn_id             TEXT NOT NULL,
    source_schema       TEXT NOT NULL,
    source_table        TEXT NOT NULL,
    service_tier        TEXT,
    source_row_count    BIGINT,
    lake_row_count      BIGINT,
    row_count_delta     BIGINT,
    row_count_status    TEXT NOT NULL DEFAULT 'skip'
        CHECK (row_count_status IN ('ok', 'warn', 'fail', 'skip')),
    apply_lag_seconds   INT,
    apply_status        TEXT,
    pk_checksum_match   BOOLEAN,
    status              TEXT NOT NULL DEFAULT 'ok'
        CHECK (status IN ('ok', 'warn', 'fail', 'skip')),
    checks              JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT reconciliation_result_run_table_uk
        UNIQUE (run_id, source_schema, source_table)
);

CREATE INDEX IF NOT EXISTS reconciliation_result_run_idx
    ON cdc_catalog.reconciliation_result (run_id);

CREATE INDEX IF NOT EXISTS reconciliation_result_status_idx
    ON cdc_catalog.reconciliation_result (conn_id, status, source_schema, source_table);

COMMENT ON TABLE cdc_catalog.reconciliation_run IS
    'One row per cdc_kafka reconcile CLI run (independent of capture/apply daemon).';
COMMENT ON TABLE cdc_catalog.reconciliation_result IS
    'Per-table reconciliation outcome: row counts, apply lag, optional PK sample checksum.';

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('reconcile_row_abs_tolerance', 'cdc_kafka_reconcile', '', '50'::jsonb,
     'Fail row-count drift when abs(source-lake) exceeds this (non-large tables)'),
    ('reconcile_row_pct_tolerance', 'cdc_kafka_reconcile', '', '0.01'::jsonb,
     'Fail row-count drift when pct gap exceeds this (1% default)'),
    ('reconcile_row_warn_abs_tolerance', 'cdc_kafka_reconcile', '', '10'::jsonb,
     'Warn row-count drift when abs gap exceeds this but below fail threshold'),
    ('reconcile_row_warn_pct_tolerance', 'cdc_kafka_reconcile', '', '0.005'::jsonb,
     'Warn row-count drift when pct gap exceeds this but below fail threshold'),
    ('reconcile_large_table_min_rows', 'cdc_kafka_reconcile', '', '100000'::jsonb,
     'Tables with at least this many source rows use large-table abs cap'),
    ('reconcile_large_table_abs_cap', 'cdc_kafka_reconcile', '', '5000'::jsonb,
     'Max abs row gap allowed for large tables within pct tolerance'),
    ('reconcile_apply_lag_warn_seconds', 'cdc_kafka_reconcile', '', '600'::jsonb,
     'Warn when apply_lag_seconds exceeds this'),
    ('reconcile_apply_lag_fail_seconds', 'cdc_kafka_reconcile', '', '1800'::jsonb,
     'Fail when apply_lag_seconds exceeds this'),
    ('reconcile_pk_sample_size', 'cdc_kafka_reconcile', '', '100'::jsonb,
     'PK rows to sample for checksum (0=disabled; hot tier only by default in CLI)'),
    ('reconcile_max_tables', 'cdc_kafka_reconcile', '', '0'::jsonb,
     'Cap tables per run (0=no cap)')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
