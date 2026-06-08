-- Central application logs (control plane: cdc_catalog.logs)
-- Database: DataLake

DO $$
BEGIN
    CREATE TYPE cdc_catalog.log_level AS ENUM ('debug', 'info', 'warning', 'error');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS cdc_catalog.logs (
    log_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    level           cdc_catalog.log_level NOT NULL,
    component       TEXT NOT NULL,
    message         TEXT NOT NULL,
    context         JSONB NOT NULL DEFAULT '{}',

    batch_id        TEXT,
    conn_id         TEXT,
    source_schema   TEXT,
    source_table    TEXT,

    CONSTRAINT logs_context_is_object CHECK (jsonb_typeof(context) = 'object')
);

CREATE INDEX IF NOT EXISTS logs_logged_at_idx
    ON cdc_catalog.logs (logged_at DESC);

CREATE INDEX IF NOT EXISTS logs_component_logged_at_idx
    ON cdc_catalog.logs (component, logged_at DESC);

CREATE INDEX IF NOT EXISTS logs_errors_idx
    ON cdc_catalog.logs (logged_at DESC)
    WHERE level IN ('warning', 'error');

CREATE INDEX IF NOT EXISTS logs_context_gin
    ON cdc_catalog.logs USING GIN (context jsonb_path_ops);

COMMENT ON TABLE cdc_catalog.logs IS
    'Append-only application log. All pipeline components MUST persist operational logs here.';

COMMENT ON COLUMN cdc_catalog.logs.component IS
    'Logical owner: catalog, mariadb_load, mariadb_cdc, reconcile, etc.';

COMMENT ON COLUMN cdc_catalog.logs.context IS
    'Structured fields (error code, row counts, duration_ms). No secrets.';
