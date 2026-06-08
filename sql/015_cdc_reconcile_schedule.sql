-- Reconcile schedule keys (separate migration so 014 can stay idempotent on existing DBs)

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('reconcile_interval_hours', 'cdc_kafka_reconcile', '', '4'::jsonb,
     'Hours between scheduled reconcile runs (main loop / systemd timer)'),
    ('reconcile_enabled', 'cdc_kafka_reconcile', '', 'true'::jsonb,
     'When false, reconcile-loop sleeps but skips table checks')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
