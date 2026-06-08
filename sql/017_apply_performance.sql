-- Apply throughput: 50k batches, append COPY, batch audit, Kafka fetch tuning
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_append_only', 'cdc_kafka_apply', '', 'true'::jsonb, 'Direct COPY append for insert-only ops (fallback ON CONFLICT on duplicate)'),
    ('apply_audit_enabled', 'cdc_kafka_apply', '', 'true'::jsonb, 'Batch insert cdc_applied_events (disable only for stress)'),
    ('apply_dedup_enabled', 'cdc_kafka_apply', '', 'true'::jsonb, 'Skip already-applied event_ids before lake write'),
    ('apply_poll_timeout_ms', 'cdc_kafka_apply', '', '100'::jsonb, 'Kafka consumer poll timeout ms'),
    ('apply_fetch_max_bytes', 'cdc_kafka_apply', '', '52428800'::jsonb, 'Kafka fetch.max.bytes (50MB)'),
    ('apply_max_partition_fetch_bytes', 'cdc_kafka_apply', '', '10485760'::jsonb, 'Kafka max.partition.fetch.bytes (10MB)')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;

UPDATE cdc_catalog.runtime_config
SET config_value = '50000'::jsonb, updated_at = now()
WHERE config_key = 'apply_batch_size' AND component = 'cdc_kafka_apply' AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = '15000000'::jsonb, updated_at = now()
WHERE config_key = 'apply_max_events' AND component = 'cdc_kafka_apply' AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = '0'::jsonb, updated_at = now()
WHERE config_key = 'apply_target_events_per_table' AND component = 'cdc_kafka_apply' AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = '"copy"'::jsonb, updated_at = now()
WHERE config_key = 'apply_mode' AND component = 'cdc_kafka_apply' AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = '"binary"'::jsonb, updated_at = now()
WHERE config_key = 'apply_copy_format' AND component = 'cdc_kafka_apply' AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = 'true'::jsonb, updated_at = now()
WHERE config_key = 'apply_append_only' AND component = 'cdc_kafka_apply' AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = 'true'::jsonb, updated_at = now()
WHERE config_key = 'apply_audit_enabled' AND component = 'cdc_kafka_apply' AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = '100'::jsonb, updated_at = now()
WHERE config_key = 'apply_poll_timeout_ms' AND component = 'cdc_kafka_apply' AND conn_id = '';
