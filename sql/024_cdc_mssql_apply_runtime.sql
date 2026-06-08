-- MSSQL native apply catch-up tuning (conn_id override via RuntimeConfig scope lookup)
-- Matches MariaDB hot-path throughput (~340k events/min) on single-table bursts.

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_empty_poll_quiet_threshold', 'cdc_kafka_apply', 'MSSQL_LOCAL', '10'::jsonb,
     'MSSQL catch-up: empty polls before quiet slice exit (same as MariaDB global default)'),
    ('apply_poll_timeout_ms', 'cdc_kafka_apply', 'MSSQL_LOCAL', '100'::jsonb,
     'MSSQL: librdkafka poll timeout ms (match MariaDB hot path)'),
    ('apply_max_partition_fetch_bytes', 'cdc_kafka_apply', 'MSSQL_LOCAL', '20971520'::jsonb,
     'MSSQL: max bytes per Kafka partition per fetch (20MB)'),
    ('apply_fetch_max_bytes', 'cdc_kafka_apply', 'MSSQL_LOCAL', '104857600'::jsonb,
     'MSSQL: max total fetch bytes per poll (100MB)'),
    ('apply_batch_size', 'cdc_kafka_apply', 'MSSQL_LOCAL', '50000'::jsonb,
     'MSSQL: PG commit batch size (same as MariaDB hot path)'),
    ('apply_max_seconds', 'cdc_kafka_apply', 'MSSQL_LOCAL', '60'::jsonb,
     'MSSQL catch-up slice max seconds (parity with MariaDB benchmark slice)'),
    ('apply_queued_min_messages', 'cdc_kafka_apply', 'MSSQL_LOCAL', '100000'::jsonb,
     'MSSQL: librdkafka queued.min.messages (prefetch for catch-up)'),
    ('apply_fetch_wait_max_ms', 'cdc_kafka_apply', 'MSSQL_LOCAL', '500'::jsonb,
     'MSSQL: librdkafka fetch.wait.max.ms'),
    ('capture_producer_linger_ms', 'cdc_kafka_mssql_capture', 'MSSQL_LOCAL', '5'::jsonb,
     'MSSQL capture producer linger ms'),
    ('capture_producer_batch_size', 'cdc_kafka_mssql_capture', 'MSSQL_LOCAL', '10000'::jsonb,
     'MSSQL capture producer batch.num.messages'),
    ('kafka_topic_partitions', 'cdc_kafka_mssql_capture', '', '6'::jsonb,
     'MSSQL bucket topic partition count (ensure_bucket_topics)'),
    ('apply_worker_count', 'cdc_kafka_apply', 'MSSQL_LOCAL', '1'::jsonb,
     'MSSQL apply workers (raise when catalog has many tables; stress bench uses 1)')
ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();
