-- Remove runtime keys unused by native C++ CDC (Debezium / Connect / stale prefixes).
-- psql -d datasync -f deploy/runtime_config_legacy_cleanup.sql

\echo '=== keys to delete ==='
SELECT config_key, component, conn_id, config_value
FROM cdc_catalog.runtime_config
WHERE (config_key, component) IN (
    ('debezium_topic_prefix', 'cdc_kafka_apply'),
    ('debezium_connect_url', 'cdc_kafka_health'),
    ('debezium_connector_name', 'cdc_kafka_health'),
    ('capture_topic_prefix', 'cdc_kafka_capture'),
    ('kafka_topic_prefix', 'cdc_kafka_apply'),
    ('capture_binlog_mode', 'cdc_kafka_capture')
)
   OR (config_key = 'cdc_apply_batch_size' AND component = 'mariadb_cdc')
   OR component = 'cdc_kafka_health';

DELETE FROM cdc_catalog.runtime_config
WHERE (config_key, component) IN (
    ('debezium_topic_prefix', 'cdc_kafka_apply'),
    ('debezium_connect_url', 'cdc_kafka_health'),
    ('debezium_connector_name', 'cdc_kafka_health'),
    ('capture_topic_prefix', 'cdc_kafka_capture'),
    ('kafka_topic_prefix', 'cdc_kafka_apply'),
    ('capture_binlog_mode', 'cdc_kafka_capture')
)
   OR (config_key = 'cdc_apply_batch_size' AND component = 'mariadb_cdc')
   OR component = 'cdc_kafka_health';

\echo '=== remaining row count ==='
SELECT COUNT(*) AS runtime_config_rows FROM cdc_catalog.runtime_config;
