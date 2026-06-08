-- Reconcile pipeline checks (Kafka lag, capture lag) + light/auto modes for scale.

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('reconcile_mode', 'cdc_kafka_reconcile', '', '"auto"'::jsonb,
     'full | light | auto — light skips source COUNT/PK; auto runs full every reconcile_full_interval_hours'),
    ('reconcile_full_interval_hours', 'cdc_kafka_reconcile', '', '4'::jsonb,
     'When reconcile_mode=auto, hours between full source-vs-lake runs (4h = every timer tick)'),
    ('reconcile_kafka_lag_warn', 'cdc_kafka_reconcile', '', '100'::jsonb,
     'Warn when kafka_consumer_lag (messages behind apply offset) exceeds this'),
    ('reconcile_kafka_lag_fail', 'cdc_kafka_reconcile', '', '10000'::jsonb,
     'Fail when kafka_consumer_lag exceeds this'),
    ('reconcile_capture_lag_warn_seconds', 'cdc_kafka_reconcile', '', '300'::jsonb,
     'Warn when per-table/conn capture lag seconds exceeds this'),
    ('reconcile_capture_lag_fail_seconds', 'cdc_kafka_reconcile', '', '900'::jsonb,
     'Fail when capture lag seconds exceeds this')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;

COMMENT ON TABLE cdc_catalog.reconciliation_result IS
    'Per-table reconciliation: row counts (full mode), apply/capture/Kafka pipeline lag, optional PK sample checksum in checks JSON';
