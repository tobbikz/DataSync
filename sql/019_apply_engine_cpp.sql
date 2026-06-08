-- Default apply engine: C++ COPY bridge (Python Kafka consumer + datalake-catalog kafka-apply-batch)
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_engine', 'cdc_kafka_apply', '', '"cpp"'::jsonb, 'Apply engine: cpp (COPY via datalake-catalog) or python')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;

UPDATE cdc_catalog.runtime_config
SET config_value = '"cpp"'::jsonb, updated_at = now()
WHERE config_key = 'apply_engine' AND component = 'cdc_kafka_apply' AND conn_id = '';
