-- Auto reconcile: full (source+lake+pipeline) every 4h; light only if manual run between.

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('reconcile_mode', 'cdc_kafka_reconcile', '', '"auto"'::jsonb,
     'auto: full every reconcile_full_interval_hours; light between'),
    ('reconcile_full_interval_hours', 'cdc_kafka_reconcile', '', '4'::jsonb,
     'Hours between full reconcile runs when mode=auto (match reconcile_interval_hours)')
ON CONFLICT (config_key, component, conn_id) DO UPDATE
SET config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();
