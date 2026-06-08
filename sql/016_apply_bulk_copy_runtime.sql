-- Bulk COPY apply + unlimited per-table fairness (catch-up / stress)
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_mode', 'cdc_kafka_apply', '', '"copy"'::jsonb, 'Lake apply: copy (bulk COPY) or row (per-event upsert)'),
    ('apply_copy_format', 'cdc_kafka_apply', '', '"binary"'::jsonb, 'COPY format: binary (auto-fallback csv) or csv')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;

UPDATE cdc_catalog.runtime_config
SET config_value = '0'::jsonb,
    updated_at = now(),
    description = 'Per-table soft cap per slice; 0 = unlimited (drain Kafka as fast as PG allows)'
WHERE config_key = 'apply_target_events_per_table'
  AND component = 'cdc_kafka_apply'
  AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = '15000000'::jsonb, updated_at = now()
WHERE config_key = 'apply_max_events'
  AND component = 'cdc_kafka_apply'
  AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = '5000'::jsonb, updated_at = now()
WHERE config_key = 'apply_batch_size'
  AND component = 'cdc_kafka_apply'
  AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = '"copy"'::jsonb, updated_at = now()
WHERE config_key = 'apply_mode'
  AND component = 'cdc_kafka_apply'
  AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = '"binary"'::jsonb, updated_at = now()
WHERE config_key = 'apply_copy_format'
  AND component = 'cdc_kafka_apply'
  AND conn_id = '';
