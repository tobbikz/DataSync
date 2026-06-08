-- Native CDC daemon: tier idle seconds between capture+apply cycles

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('daemon_round_idle_seconds', 'cdc_kafka_daemon', '', '5'::jsonb, 'Sleep after parallel tier round (min with service_tiers.daemon_idle_seconds)'),
    ('daemon_idle_seconds_hot', 'cdc_kafka_daemon', '', '5'::jsonb, 'Doc mirror; live idle in service_tiers + daemon_round_idle_seconds'),
    ('daemon_idle_seconds_gold', 'cdc_kafka_daemon', '', '5'::jsonb, 'Doc mirror; live idle in service_tiers + daemon_round_idle_seconds'),
    ('daemon_idle_seconds_silver', 'cdc_kafka_daemon', '', '5'::jsonb, 'Doc mirror; live idle in service_tiers + daemon_round_idle_seconds'),
    ('daemon_idle_seconds_bronze', 'cdc_kafka_daemon', '', '5'::jsonb, 'Doc mirror; live idle in service_tiers + daemon_round_idle_seconds')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
