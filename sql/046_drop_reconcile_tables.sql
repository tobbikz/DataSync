-- Migration 046 (manual): drop legacy reconcile tables after C++ reconcile removal.
-- Safe to run if tables were already dropped manually.
-- After this, redeploy DataSync binary with guarded schema_patches (migration 046 embedded).

BEGIN;

DROP TABLE IF EXISTS cdc_catalog.reconciliation_result CASCADE;
DROP TABLE IF EXISTS cdc_catalog.reconciliation_result_daily CASCADE;
DROP TABLE IF EXISTS cdc_catalog.reconciliation_run CASCADE;

DELETE FROM cdc_catalog.runtime_config
WHERE component = 'cdc_kafka_reconcile'
   OR config_key LIKE 'reconcile%';

INSERT INTO cdc_catalog.schema_migrations (version, description)
VALUES (46, 'drop reconcile tables; apply-health-only CDC pipeline')
ON CONFLICT (version) DO NOTHING;

COMMIT;
