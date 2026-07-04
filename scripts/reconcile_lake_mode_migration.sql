-- Migration 044: allow reconcile_mode 'lake' + default runtime_config
ALTER TABLE cdc_catalog.reconciliation_run
    DROP CONSTRAINT IF EXISTS reconciliation_run_mode_check;

ALTER TABLE cdc_catalog.reconciliation_run
    ADD CONSTRAINT reconciliation_run_mode_check
    CHECK (reconcile_mode = ANY (ARRAY['full'::text, 'lake'::text]));

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
VALUES (
    'reconcile_cycle_mode',
    'cdc_kafka_reconcile',
    '*',
    '"lake"'::jsonb,
    'Cycle reconcile mode: lake (PG+Kafka metadata only) or full (source row counts + PK sample)'
)
ON CONFLICT (config_key, component, conn_id) DO NOTHING;

INSERT INTO cdc_catalog.schema_migrations (version, description)
VALUES (44, 'reconcile: lake pipeline-only mode + reconcile_cycle_mode default')
ON CONFLICT DO NOTHING;
