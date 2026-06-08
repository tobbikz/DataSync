-- Conn-scoped capture slice seconds (parity MSSQL_LOCAL ↔ MARIADB_LOCAL when config.json slice unset)

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('capture_max_seconds', 'cdc_kafka_capture', 'MARIADB_LOCAL', '60'::jsonb,
     'MariaDB test slice seconds (parity MSSQL_LOCAL conn override)')
ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();
