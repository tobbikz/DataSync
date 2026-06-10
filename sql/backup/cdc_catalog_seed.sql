-- Idempotent runtime_config seeds (safe to re-run on every install).
-- Credentials live in config.json / cdc_catalog.connections only.

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
VALUES
    ('logs_retention_days', 'global', '', '7'::jsonb, 'Purge cdc_catalog.logs retention'),
    ('reconcile_interval_hours', 'cdc_kafka_reconcile', '', '4'::jsonb, 'Hours between reconcile-loop runs'),
    ('reconcile_enabled', 'cdc_kafka_reconcile', '', 'true'::jsonb, 'Enable reconciliation pipeline'),
    ('applied_events_retention_days', 'cdc_kafka_apply', '', '7'::jsonb, 'Dedup audit retention'),
    ('apply_process_rss_cap_mb', 'cdc_kafka_apply', '', '10240'::jsonb, 'Soft RSS cap MB for native apply'),
    ('apply_batch_size', 'cdc_kafka_apply', '', '50000'::jsonb, 'Apply batch before flush'),
    ('apply_max_seconds', 'cdc_kafka_apply', '', '300'::jsonb, 'Apply slice max seconds'),
    ('apply_max_events', 'cdc_kafka_apply', '', '10000000'::jsonb, 'Apply slice max events per conn/tier'),
    ('apply_worker_count', 'cdc_kafka_apply', '', '1'::jsonb, 'Native apply worker threads (overrides config.json tier apply_workers)'),
    ('capture_max_seconds', 'cdc_kafka_capture', '', '300'::jsonb, 'MariaDB capture slice max'),
    ('capture_max_events', 'cdc_kafka_capture', '', '10000000'::jsonb, 'MariaDB capture slice max binlog events'),
    ('capture_producer_queue_max_messages', 'cdc_kafka_capture', '', '500000'::jsonb, 'Kafka producer queue depth'),
    ('capture_producer_queue_max_kbytes', 'cdc_kafka_capture', '', '1048576'::jsonb, 'Kafka producer queue memory KB'),
    ('kafka_topic_mode', 'global', '', '"bucketed"'::jsonb, 'Kafka topic layout'),
    ('kafka_topic_buckets', 'global', '', '64'::jsonb, 'Bucket count when bucketed mode'),
    ('catalog_sync_interval_rounds', 'catalog', '', '12'::jsonb, 'Daemon: run discover every N rounds (1=every round)'),
    ('full_load_max_fail_retries', 'mariadb_load', '', '5'::jsonb, 'Pause full-load retries after N failures (needs_full_load=false)'),
    ('full_load_failed_cooldown_minutes', 'mariadb_load', '', '240'::jsonb, 'Re-enable paused full-load tables after cooldown minutes')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
