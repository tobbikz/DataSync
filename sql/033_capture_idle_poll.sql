-- Chunk timeout for mariadb-binlog subprocess during capture (early exit when caught up).
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
VALUES
    ('capture_idle_poll_seconds', 'cdc_kafka_capture', '', '3'::jsonb,
     'Per-chunk mariadb-binlog timeout; slice still capped by slice_max_seconds in config.json')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
