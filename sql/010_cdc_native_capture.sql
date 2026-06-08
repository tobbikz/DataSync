-- Native capture runtime keys (schema_history removed — DDL via full-load/catchup ddl sync)

COMMENT ON TABLE cdc_catalog.capture_position IS
    'GTID-first capture cursor; source of truth for native binlog capture resume.';

-- Rename legacy column usage (optional metadata)
COMMENT ON COLUMN cdc_catalog.capture_position.kafka_connect_name IS
    'Deprecated; kept for compatibility. Use capture service batch id if needed.';

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('capture_max_seconds', 'cdc_kafka_capture', '', '300'::jsonb, 'Max seconds per capture slice (test: 60 via reset_kafka_topics.sh --test-runtime)'),
    ('capture_max_events', 'cdc_kafka_capture', '', '10000000'::jsonb, 'Max row events published per slice'),
    ('capture_topic_prefix', 'cdc_kafka_capture', '', '"MARIADB_LOCAL"'::jsonb, 'Kafka topic prefix conn_id'),
    ('capture_heartbeat_seconds', 'cdc_kafka_capture', '', '60'::jsonb, 'Bump heartbeat table when idle'),
    ('kafka_topic_prefix', 'cdc_kafka_apply', '', '"MARIADB_LOCAL"'::jsonb, 'Kafka topic prefix (replaces debezium_topic_prefix)')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
