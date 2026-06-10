-- Apply / catch-up / full-load parallelism / reconcile / logs (prod).
-- Run after: deploy/runtime_config_prod_slice_tune.sql
-- psql -d datasync -f deploy/runtime_config_prod_apply_tune.sql

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
VALUES
    ('apply_batch_size', 'cdc_kafka_apply', '', '50000'::jsonb, 'Rows before PG flush'),
    ('kafka_topic_partitions', 'cdc_kafka_capture', '', '6'::jsonb, 'Partitions per bucket topic (capture)'),
    ('kafka_topic_partitions', 'cdc_kafka_apply', '', '6'::jsonb, 'Partitions per bucket topic (apply)'),
    ('apply_catchup_enabled', 'cdc_kafka_apply', '', 'true'::jsonb, 'Mini full-load on high lag'),
    ('apply_catchup_kafka_messages', 'cdc_kafka_apply', '', '20000'::jsonb, 'Trigger catch-up (LiveChat-scale lag)'),
    ('apply_catchup_lag_seconds', 'cdc_kafka_apply', '', '180'::jsonb, 'Apply lag seconds trigger'),
    ('apply_catchup_max_tables', 'cdc_kafka_apply', '', '2'::jsonb, 'Max catch-up tables per slice'),
    ('apply_process_rss_cap_mb', 'cdc_kafka_apply', '', '10240'::jsonb, 'Soft RSS cap MB'),
    ('full_load_parallel_tables', 'mariadb_load', '', '4'::jsonb, 'Parallel full-load tables per conn'),
    ('reconcile_enabled', 'cdc_kafka_reconcile', '', 'true'::jsonb, 'Enable reconcile loop'),
    ('reconcile_interval_hours', 'cdc_kafka_reconcile', '', '4'::jsonb, 'Hours between reconcile runs'),
    ('logs_retention_days', 'global', '', '7'::jsonb, 'Purge cdc_catalog.logs retention')
ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();

\echo '=== apply / catchup / full-load parallel ==='
SELECT config_key, component, config_value
FROM cdc_catalog.runtime_config
WHERE config_key IN (
    'apply_batch_size', 'kafka_topic_partitions', 'apply_catchup_enabled',
    'apply_catchup_kafka_messages', 'full_load_parallel_tables',
    'reconcile_enabled', 'logs_retention_days'
)
ORDER BY config_key, component;
