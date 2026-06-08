-- Native C++ Kafka apply engine mode (full consumer in datalake-catalog kafka-apply)
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_engine_mode', 'cdc_kafka_apply', '', '"native"'::jsonb,
     'Apply engine: native C++ kafka-apply (librdkafka consumer + COPY)')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
