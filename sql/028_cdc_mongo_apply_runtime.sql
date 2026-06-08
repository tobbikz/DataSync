-- MongoDB native apply catch-up tuning (conn_id override via RuntimeConfig scope lookup)
-- Matches MariaDB/MSSQL hot-path throughput (~280-340k events/min) on single-collection bursts.

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_empty_poll_quiet_threshold', 'cdc_kafka_apply', 'MONGO_LOCAL', '10'::jsonb,
     'MongoDB catch-up: empty polls before quiet slice exit (same as MariaDB global default)'),
    ('apply_poll_timeout_ms', 'cdc_kafka_apply', 'MONGO_LOCAL', '100'::jsonb,
     'MongoDB: librdkafka poll timeout ms (match MariaDB hot path)'),
    ('apply_max_partition_fetch_bytes', 'cdc_kafka_apply', 'MONGO_LOCAL', '20971520'::jsonb,
     'MongoDB: max bytes per Kafka partition per fetch (20MB)'),
    ('apply_fetch_max_bytes', 'cdc_kafka_apply', 'MONGO_LOCAL', '104857600'::jsonb,
     'MongoDB: max total fetch bytes per poll (100MB)'),
    ('apply_batch_size', 'cdc_kafka_apply', 'MONGO_LOCAL', '50000'::jsonb,
     'MongoDB: PG commit batch size (same as MariaDB hot path)'),
    ('apply_max_seconds', 'cdc_kafka_apply', 'MONGO_LOCAL', '60'::jsonb,
     'MongoDB catch-up slice max seconds (parity with MariaDB benchmark slice)'),
    ('apply_queued_min_messages', 'cdc_kafka_apply', 'MONGO_LOCAL', '100000'::jsonb,
     'MongoDB: librdkafka queued.min.messages (prefetch for catch-up)'),
    ('apply_fetch_wait_max_ms', 'cdc_kafka_apply', 'MONGO_LOCAL', '500'::jsonb,
     'MongoDB: librdkafka fetch.wait.max.ms'),
    ('capture_producer_linger_ms', 'cdc_kafka_mongo_capture', 'MONGO_LOCAL', '5'::jsonb,
     'MongoDB capture producer linger ms'),
    ('capture_producer_batch_size', 'cdc_kafka_mongo_capture', 'MONGO_LOCAL', '10000'::jsonb,
     'MongoDB capture producer batch.num.messages'),
    ('kafka_topic_partitions', 'cdc_kafka_mongo_capture', '', '6'::jsonb,
     'MongoDB bucket topic partition count (ensure_bucket_topics)'),
    ('apply_worker_count', 'cdc_kafka_apply', 'MONGO_LOCAL', '1'::jsonb,
     'MongoDB apply workers (raise when catalog has many collections; stress bench uses 1)'),
    ('capture_worker_count', 'cdc_kafka_mongo_capture', 'MONGO_LOCAL', '1'::jsonb,
     'MongoDB capture workers (hash sharding by catalog_id)'),
    ('capture_max_events', 'cdc_kafka_mongo_capture', 'MONGO_LOCAL', '50000'::jsonb,
     'MongoDB capture max events per slice (benchmark override via shell script)')
ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();
