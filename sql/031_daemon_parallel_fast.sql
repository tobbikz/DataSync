-- Fast parallel daemon round: all tiers capture+apply concurrently, short sleep between rounds.
-- Slice budget: 10M events (runtime_config); idle from min(service_tiers, daemon_round_idle_seconds).

UPDATE cdc_catalog.service_tiers
SET daemon_idle_seconds = 5, updated_at = now()
WHERE tier_code IN ('hot', 'gold', 'silver', 'bronze');

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('daemon_round_idle_seconds', 'cdc_kafka_daemon', '', '5'::jsonb,
     'Sleep seconds after parallel tier round (min with service_tiers.daemon_idle_seconds)'),
    ('daemon_idle_seconds_hot', 'cdc_kafka_daemon', '', '5'::jsonb, 'Legacy doc; parallel mode uses service_tiers + daemon_round_idle_seconds'),
    ('daemon_idle_seconds_gold', 'cdc_kafka_daemon', '', '5'::jsonb, 'Legacy doc; parallel mode uses service_tiers + daemon_round_idle_seconds'),
    ('daemon_idle_seconds_silver', 'cdc_kafka_daemon', '', '5'::jsonb, 'Legacy doc; parallel mode uses service_tiers + daemon_round_idle_seconds'),
    ('daemon_idle_seconds_bronze', 'cdc_kafka_daemon', '', '5'::jsonb, 'Legacy doc; parallel mode uses service_tiers + daemon_round_idle_seconds'),
    ('capture_max_events', 'cdc_kafka_capture', '', '10000000'::jsonb, 'Max row events published per slice'),
    ('apply_max_events', 'cdc_kafka_apply', '', '10000000'::jsonb, 'Max Kafka events per apply slice'),
    ('capture_max_events', 'cdc_kafka_mssql_capture', '', '10000000'::jsonb, 'MSSQL capture max events per slice'),
    ('capture_max_events', 'cdc_kafka_mongo_capture', '', '10000000'::jsonb, 'MongoDB capture max events per slice')
ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();
