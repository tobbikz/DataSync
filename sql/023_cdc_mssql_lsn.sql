-- MSSQL CDC LSN resume (per table), used by cdc_kafka MSSQL capture
CREATE TABLE IF NOT EXISTS cdc_catalog.cdc_mssql_lsn (
    conn_id         TEXT NOT NULL,
    database        TEXT NOT NULL,
    schema_name     TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    last_start_lsn  BYTEA NOT NULL,
    last_seqval     BYTEA,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (conn_id, database, schema_name, table_name)
);

CREATE INDEX IF NOT EXISTS cdc_mssql_lsn_updated_idx
    ON cdc_catalog.cdc_mssql_lsn (conn_id, updated_at DESC);

COMMENT ON TABLE cdc_catalog.cdc_mssql_lsn IS
    'Per-table SQL Server CDC LSN checkpoint for cdc_kafka MSSQL capture';

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('capture_max_seconds', 'cdc_kafka_mssql_capture', '', '60'::jsonb, 'MSSQL capture slice max seconds'),
    ('capture_max_events', 'cdc_kafka_mssql_capture', '', '50000'::jsonb, 'MSSQL capture max events per slice'),
    ('capture_topic_prefix', 'cdc_kafka_mssql_capture', 'MSSQL_LOCAL', '"MSSQL_LOCAL"'::jsonb, 'Kafka topic prefix for MSSQL capture')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
