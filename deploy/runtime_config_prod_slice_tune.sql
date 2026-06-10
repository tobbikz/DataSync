-- Prod slice tuning: runtime_config wins over config.json (post f71ffd3+ deploy).
-- Run: psql -d datasync -f deploy/runtime_config_prod_slice_tune.sql
--
-- Targets: 900s slices, 1M events/slice, 6 apply workers, full-load retry cap.

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
VALUES
    ('apply_max_seconds', 'cdc_kafka_apply', '', '900'::jsonb,
     'Apply slice max seconds (runtime overrides config.json)'),
    ('apply_max_events', 'cdc_kafka_apply', '', '1000000'::jsonb,
     'Apply slice max Kafka events'),
    ('apply_worker_count', 'cdc_kafka_apply', '', '6'::jsonb,
     'Native apply worker threads (catalog_id hash sharding)'),
    ('capture_max_seconds', 'cdc_kafka_capture', '', '900'::jsonb,
     'MariaDB capture slice max seconds'),
    ('capture_max_events', 'cdc_kafka_capture', '', '1000000'::jsonb,
     'MariaDB capture slice max binlog events published'),
    ('full_load_max_fail_retries', 'mariadb_load', '', '5'::jsonb,
     'Pause full-load after N COPY failures (needs_full_load=false)'),
    ('full_load_failed_cooldown_minutes', 'mariadb_load', '', '240'::jsonb,
     'Re-enable paused full-load tables after cooldown')
ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();

-- Optional: per-conn overrides (uncomment conn_id as needed)
-- INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
-- VALUES
--     ('apply_max_seconds', 'cdc_kafka_apply', 'MARIADB21', '900'::jsonb, 'Apply slice MARIADB21'),
--     ('capture_max_seconds', 'cdc_kafka_capture', 'MARIADB21', '900'::jsonb, 'Capture slice MARIADB21')
-- ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
--     config_value = EXCLUDED.config_value,
--     updated_at = now();

\echo '=== slice / worker keys (global) ==='
SELECT config_key, component, conn_id, config_value, updated_at
FROM cdc_catalog.runtime_config
WHERE config_key IN (
    'apply_max_seconds', 'apply_max_events', 'apply_worker_count',
    'capture_max_seconds', 'capture_max_events',
    'full_load_max_fail_retries', 'full_load_failed_cooldown_minutes'
)
ORDER BY config_key;
