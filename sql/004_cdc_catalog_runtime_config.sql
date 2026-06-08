-- Runtime configuration: read at job start and before each table/batch (hot reload).
-- Scope: (config_key, component, conn_id). conn_id='' = default for all connections.

CREATE TABLE IF NOT EXISTS cdc_catalog.runtime_config (
    config_key      TEXT NOT NULL,
    component       TEXT NOT NULL DEFAULT 'global',
    conn_id         TEXT NOT NULL DEFAULT '',
    config_value    JSONB NOT NULL,
    description     TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT runtime_config_pk PRIMARY KEY (config_key, component, conn_id),
    CONSTRAINT runtime_config_value_is_scalar CHECK (
        jsonb_typeof(config_value) IN ('number', 'string', 'boolean')
    )
);

CREATE INDEX IF NOT EXISTS runtime_config_component_idx
    ON cdc_catalog.runtime_config (component, conn_id);

DROP TRIGGER IF EXISTS runtime_config_updated_at_trg ON cdc_catalog.runtime_config;
CREATE TRIGGER runtime_config_updated_at_trg
    BEFORE UPDATE ON cdc_catalog.runtime_config
    FOR EACH ROW
    EXECUTE FUNCTION cdc_catalog.touch_updated_at();

COMMENT ON TABLE cdc_catalog.runtime_config IS
    'Hot-reload runtime knobs. C++ reads before each run/table; JSON file only holds connection credentials.';

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('full_load_batch_size', 'mariadb_load', '', '5000'::jsonb, 'Rows per COPY keyset batch'),
    ('full_load_workers', 'mariadb_load', '', '1'::jsonb, 'Parallel worker threads per conn (1=sequential)'),
    ('full_load_parallel_tables', 'mariadb_load', '', '1'::jsonb, 'Max tables loaded in parallel per conn'),
    ('full_load_source_sleep_ms', 'mariadb_load', '', '0'::jsonb, 'Sleep ms between source read batches'),
    ('catalog_chunk_size', 'catalog', '', '500'::jsonb, 'Discover upsert chunk size'),
    ('catalog_batch_sleep_ms', 'catalog', '', '200'::jsonb, 'Sleep ms between MariaDB discover batches'),
    ('catalog_metadata_workers', 'catalog', '', '3'::jsonb, 'Parallel PK sync workers'),
    ('logs_retention_days', 'global', '', '7'::jsonb, 'cdc_catalog.logs purge age in days'),
    ('ddl_sync_indexes', 'mariadb_load', '', 'true'::jsonb, 'Sync secondary indexes after truncate'),
    ('ddl_sync_foreign_keys', 'mariadb_load', '', 'true'::jsonb, 'Sync FK constraints after truncate')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
