-- Health + reconciliation RAG on apply_batch_stats.

ALTER TABLE cdc_catalog.apply_batch_stats
    ADD COLUMN IF NOT EXISTS is_stale BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_starving BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_inactive BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_quarantined BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS reconciliation_rag TEXT NOT NULL DEFAULT 'UNKNOWN',
    ADD COLUMN IF NOT EXISTS apply_lag_seconds INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS apply_position_status TEXT,
    ADD COLUMN IF NOT EXISTS events_seen_in_slice INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS catalog_active BOOLEAN,
    ADD COLUMN IF NOT EXISTS cdc_enabled BOOLEAN;

DO $$
BEGIN
    ALTER TABLE cdc_catalog.apply_batch_stats
        ADD CONSTRAINT apply_batch_stats_reconciliation_rag_chk
        CHECK (reconciliation_rag IN ('GREEN', 'AMBER', 'RED', 'UNKNOWN'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS apply_batch_stats_rag_idx
    ON cdc_catalog.apply_batch_stats (conn_id, reconciliation_rag, logged_at DESC);

CREATE INDEX IF NOT EXISTS apply_batch_stats_health_idx
    ON cdc_catalog.apply_batch_stats (conn_id, is_stale, is_starving, is_inactive);

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.is_stale IS
    'Snapshot after apply: apply_position lag exceeds apply_max_table_staleness_seconds or status stale/lagging/gap';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.is_starving IS
    'Slice had Kafka events for table but zero applied (fairness starvation); false when events_total > 0';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.is_inactive IS
    'No CDC events seen in slice (quiet table)';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.reconciliation_rag IS
    'Latest reconcile: ok→GREEN, warn→AMBER, fail→RED';

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_inactive_seconds', 'cdc_kafka_apply', '', '3600'::jsonb,
     'Mark is_inactive when last_applied_at older than N seconds and no events in slice')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
