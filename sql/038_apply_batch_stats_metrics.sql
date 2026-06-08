-- Extended apply_batch_stats metrics + semaphore alias for reconciliation RAG.

ALTER TABLE cdc_catalog.apply_batch_stats
    ADD COLUMN IF NOT EXISTS capture_lag_seconds INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS kafka_consumer_lag BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reconcile_row_delta BIGINT,
    ADD COLUMN IF NOT EXISTS catchup_triggered BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS fk_deferred_retries INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS dedup_skipped INTEGER NOT NULL DEFAULT 0;

DO $$
BEGIN
    ALTER TABLE cdc_catalog.apply_batch_stats
        ADD COLUMN semaphore TEXT GENERATED ALWAYS AS (reconciliation_rag) STORED;
EXCEPTION
    WHEN duplicate_column THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS apply_batch_stats_semaphore_idx
    ON cdc_catalog.apply_batch_stats (conn_id, semaphore, logged_at DESC);

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.capture_lag_seconds IS
    'Conn-level capture lag from capture_position at slice time';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.kafka_consumer_lag IS
    'High watermark minus consumed offset for table topic/partition';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.reconcile_row_delta IS
    'source_row_count - lake_row_count from latest reconciliation_result';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.catchup_triggered IS
    'Mini full-load catchup ran for this table in same batch_id';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.fk_deferred_retries IS
    'FK violation fallbacks to row-level merge in this batch';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.dedup_skipped IS
    'Kafka events skipped because event_id already in cdc_applied_events';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.semaphore IS
    'GREEN/AMBER/RED mirror of reconciliation_rag (latest reconcile)';
