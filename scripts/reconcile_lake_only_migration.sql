-- Migration 045: reconcile lake-only (pipeline metadata; no OLTP row counts).
-- Applied automatically on DataSync daemon / reconcile-loop startup via schema_patches.
-- Run manually only if startup migrate is disabled.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 45) THEN
        ALTER TABLE cdc_catalog.reconciliation_run
            DROP CONSTRAINT IF EXISTS reconciliation_run_mode_check;
        ALTER TABLE cdc_catalog.reconciliation_run
            ADD CONSTRAINT reconciliation_run_mode_check
            CHECK (reconcile_mode = 'lake'::text);

        UPDATE cdc_catalog.reconciliation_run
        SET reconcile_mode = 'lake'
        WHERE reconcile_mode IS DISTINCT FROM 'lake';

        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES (
            'reconcile_cycle_mode',
            'cdc_kafka_reconcile',
            '*',
            '"lake"'::jsonb,
            'Pipeline reconcile only: cdc_catalog + Kafka lag metadata (no OLTP)'
        )
        ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
            config_value = EXCLUDED.config_value,
            description = EXCLUDED.description,
            updated_at = now();

        COMMENT ON TABLE cdc_catalog.reconciliation_run IS
            'One row per reconcile CLI run (lake: pipeline metadata only, no OLTP row counts).';

        COMMENT ON TABLE cdc_catalog.reconciliation_result IS
            'Per-table pipeline reconcile: apply/capture/Kafka lag from cdc_catalog metadata';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (45, 'reconcile lake-only; remove full/OLTP mode');
        RAISE NOTICE 'migration 045: reconcile lake-only';
    END IF;
END $$;
