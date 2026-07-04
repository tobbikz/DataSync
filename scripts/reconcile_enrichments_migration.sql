-- Reconcile enrichments schema (migration 043 standalone)
-- Run via: datasync migrate --incremental  OR apply manually on cdc_catalog DB

ALTER TABLE cdc_catalog.reconciliation_result
    DROP CONSTRAINT IF EXISTS reconciliation_result_run_table_uk;

ALTER TABLE cdc_catalog.reconciliation_result
    DROP CONSTRAINT IF EXISTS reconciliation_result_run_conn_table_uk;

ALTER TABLE cdc_catalog.reconciliation_result
    ADD CONSTRAINT reconciliation_result_run_conn_table_uk
    UNIQUE (run_id, conn_id, source_schema, source_table);

CREATE TABLE IF NOT EXISTS cdc_catalog.reconciliation_result_daily (
    snapshot_date date NOT NULL,
    catalog_id bigint NOT NULL,
    conn_id text NOT NULL,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    status text NOT NULL,
    row_count_delta bigint,
    drift_kind text,
    semaphore_reason text,
    recommended_action text,
    source_row_count bigint,
    lake_row_count bigint,
    run_id bigint NOT NULL,
    checks jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    PRIMARY KEY (snapshot_date, catalog_id)
);

CREATE INDEX IF NOT EXISTS reconciliation_result_daily_conn_idx
    ON cdc_catalog.reconciliation_result_daily (conn_id, status, snapshot_date DESC);

COMMENT ON TABLE cdc_catalog.reconciliation_result_daily IS
    'Compact daily reconcile snapshot per catalog_id for trends and SLA dashboards';
