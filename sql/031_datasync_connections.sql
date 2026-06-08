-- DataSync control plane: Airflow-style connection catalog
-- Run on database: DataSync
-- Operational key: alias (same value stored in cdc_catalog.catalog.conn_id)

CREATE TABLE IF NOT EXISTS cdc_catalog.connections (
    alias           TEXT PRIMARY KEY,
    db_engine       cdc_catalog.db_engine NOT NULL,
    host            TEXT NOT NULL DEFAULT 'localhost',
    port            INT NOT NULL,
    db_name         TEXT NOT NULL DEFAULT '',
    username        TEXT NOT NULL DEFAULT '',
    password        TEXT NOT NULL DEFAULT '',
    extras          JSONB NOT NULL DEFAULT '{}'::jsonb,
    active          BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT connections_port_range CHECK (port > 0 AND port <= 65535),
    CONSTRAINT connections_extras_is_object CHECK (jsonb_typeof(extras) = 'object')
);

CREATE INDEX IF NOT EXISTS connections_active_engine_idx
    ON cdc_catalog.connections (db_engine, alias)
    WHERE active = true;

DROP TRIGGER IF EXISTS connections_updated_at_trg ON cdc_catalog.connections;
CREATE TRIGGER connections_updated_at_trg
    BEFORE UPDATE ON cdc_catalog.connections
    FOR EACH ROW
    EXECUTE FUNCTION cdc_catalog.touch_updated_at();

COMMENT ON TABLE cdc_catalog.connections IS
    'Source connection registry (Airflow-style). alias is the operational conn_id used in catalog and runtime_config.';
COMMENT ON COLUMN cdc_catalog.connections.alias IS
    'Unique connection alias; maps 1:1 to cdc_catalog.catalog.conn_id';
COMMENT ON COLUMN cdc_catalog.connections.db_name IS
    'Default database on source (MSSQL); empty for MariaDB/Mongo when not applicable';
COMMENT ON COLUMN cdc_catalog.connections.password IS
    'Source password; never log to cdc_catalog.logs';
COMMENT ON COLUMN cdc_catalog.connections.extras IS
    'Engine-specific options, e.g. {"replica_set":"rs0"} for MongoDB';

-- Seed placeholders (no real passwords; inactive until operator sets password + active=true)
INSERT INTO cdc_catalog.connections (
    alias, db_engine, host, port, db_name, username, password, extras, active
) VALUES
    ('MARIADB_LOCAL', 'mariadb', 'localhost', 3306, '', '', '', '{}'::jsonb, false),
    ('MSSQL_LOCAL',   'mssql',   'localhost', 1433, 'master', '', '', '{}'::jsonb, false),
    ('MONGO_LOCAL',   'mongodb', 'localhost', 27017, '', '', '', '{"replica_set":"rs0"}'::jsonb, false)
ON CONFLICT (alias) DO UPDATE SET
    db_engine = EXCLUDED.db_engine,
    host = EXCLUDED.host,
    port = EXCLUDED.port,
    db_name = EXCLUDED.db_name,
    extras = EXCLUDED.extras,
    updated_at = now();
