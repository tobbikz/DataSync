-- Snapshot + concurrent stream: capture to Kafka while full load runs.
ALTER TABLE cdc_catalog.catalog
    ADD COLUMN IF NOT EXISTS capture_during_full_load boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN cdc_catalog.catalog.capture_during_full_load IS
    'When true with needs_full_load: capture publishes to Kafka; apply waits until full load completes then replays from stream bookmark in engine_meta.';
