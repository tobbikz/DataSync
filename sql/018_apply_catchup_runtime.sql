-- Catch-up mode: C++ full-load when apply/Kafka lag exceeds threshold
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_catchup_enabled', 'cdc_kafka_apply', '', 'true'::jsonb, 'Run C++ full-load catch-up when lag exceeds threshold'),
    ('apply_catchup_lag_seconds', 'cdc_kafka_apply', '', '120'::jsonb, 'Table apply lag seconds before catch-up'),
    ('apply_catchup_kafka_messages', 'cdc_kafka_apply', '', '200000'::jsonb, 'Kafka backlog messages before catch-up'),
    ('apply_catchup_max_tables', 'cdc_kafka_apply', '', '1'::jsonb, 'Max tables to catch-up per daemon cycle')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;

UPDATE cdc_catalog.runtime_config SET config_value = 'true'::jsonb, updated_at = now()
WHERE config_key = 'apply_catchup_enabled' AND component = 'cdc_kafka_apply';

UPDATE cdc_catalog.runtime_config SET config_value = '120'::jsonb, updated_at = now()
WHERE config_key = 'apply_catchup_lag_seconds' AND component = 'cdc_kafka_apply';

UPDATE cdc_catalog.runtime_config SET config_value = '200000'::jsonb, updated_at = now()
WHERE config_key = 'apply_catchup_kafka_messages' AND component = 'cdc_kafka_apply';
