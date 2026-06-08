INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_empty_poll_quiet_threshold', 'cdc_kafka_apply', '', '10'::jsonb,
     'Empty Kafka polls (1s each) before allow_quiet slice stop; test: 2 via reset_kafka_topics.sh --test-runtime')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
