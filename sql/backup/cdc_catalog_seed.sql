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
    ('capture_max_seconds', 'cdc_kafka_capture', '', '300'::jsonb, 'MariaDB capture slice max'),
    ('kafka_bootstrap_servers', 'cdc_kafka_apply', '', '"localhost:9092"'::jsonb, 'Kafka bootstrap (same-host docker compose)'),
    ('kafka_topic_mode', 'global', '', '"bucketed"'::jsonb, 'Kafka topic layout'),
    ('kafka_topic_buckets', 'global', '', '64'::jsonb, 'Bucket count when bucketed mode'),
    ('catalog_sync_interval_rounds', 'catalog', '', '12'::jsonb, 'Daemon: run discover every N rounds (1=every round)')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
