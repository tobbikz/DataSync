-- Native C++ apply only (datalake-catalog kafka-apply). Bridge/python paths removed.
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_engine_mode', 'cdc_kafka_apply', '', '"native"'::jsonb,
     'Apply engine: native C++ kafka-apply (librdkafka consumer + COPY)')
ON CONFLICT (config_key, component, conn_id) DO UPDATE
SET config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();

UPDATE cdc_catalog.runtime_config
SET config_value = '"native"'::jsonb,
    description = 'DEPRECATED — native kafka-apply is the only apply path',
    updated_at = now()
WHERE config_key = 'apply_engine' AND component = 'cdc_kafka_apply' AND conn_id = '';
