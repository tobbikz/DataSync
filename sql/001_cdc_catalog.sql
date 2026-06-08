-- DataLake CDC catalog (MariaDB-first, KISS)
-- Database: DataLake

CREATE SCHEMA IF NOT EXISTS cdc_catalog;

DO $$
BEGIN
    CREATE TYPE cdc_catalog.db_engine AS ENUM ('mariadb', 'mssql', 'mongodb');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    CREATE TYPE cdc_catalog.service_tier AS ENUM (
        'platinum', 'gold', 'silver', 'bronze', 'trash', 'firehose'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    CREATE TYPE cdc_catalog.replication_status AS ENUM (
        'pending',
        'success',
        'failed',
        'skipped',
        'disabled'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS cdc_catalog.catalog (
    catalog_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    conn_id             TEXT NOT NULL,
    db_engine           cdc_catalog.db_engine NOT NULL,
    source_database     TEXT NOT NULL DEFAULT '',
    source_schema       TEXT NOT NULL,
    source_table        TEXT NOT NULL,

    has_pk              BOOLEAN NOT NULL DEFAULT false,
    pk_columns          TEXT,

    discovered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    active              BOOLEAN NOT NULL DEFAULT false,
    cdc_enabled         BOOLEAN NOT NULL DEFAULT false,
    needs_full_load     BOOLEAN NOT NULL DEFAULT true,
    service_tier        cdc_catalog.service_tier NOT NULL DEFAULT 'bronze',

    status              cdc_catalog.replication_status NOT NULL DEFAULT 'pending',
    last_full_load_at   TIMESTAMPTZ,
    last_cdc_at         TIMESTAMPTZ,
    last_error_at       TIMESTAMPTZ,
    last_error          TEXT,

    engine_meta         JSONB NOT NULL DEFAULT '{}',

    CONSTRAINT catalog_source_object_uk UNIQUE (
        conn_id, db_engine, source_database, source_schema, source_table
    ),
    CONSTRAINT catalog_pk_columns_when_has_pk CHECK (
        NOT has_pk OR (pk_columns IS NOT NULL AND length(trim(pk_columns)) > 0)
    ),
    CONSTRAINT catalog_engine_meta_is_object CHECK (jsonb_typeof(engine_meta) = 'object')
);

CREATE INDEX IF NOT EXISTS catalog_active_cdc_idx
    ON cdc_catalog.catalog (service_tier, conn_id)
    WHERE active = true AND cdc_enabled = true AND needs_full_load = false;

CREATE INDEX IF NOT EXISTS catalog_needs_full_load_idx
    ON cdc_catalog.catalog (service_tier, conn_id)
    WHERE active = true AND needs_full_load = true;

CREATE INDEX IF NOT EXISTS catalog_failed_idx
    ON cdc_catalog.catalog (last_error_at DESC)
    WHERE status = 'failed';

CREATE INDEX IF NOT EXISTS catalog_engine_meta_gin
    ON cdc_catalog.catalog USING GIN (engine_meta jsonb_path_ops);

CREATE OR REPLACE FUNCTION cdc_catalog.touch_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS catalog_updated_at_trg ON cdc_catalog.catalog;
CREATE TRIGGER catalog_updated_at_trg
    BEFORE UPDATE ON cdc_catalog.catalog
    FOR EACH ROW
    EXECUTE FUNCTION cdc_catalog.touch_updated_at();

COMMENT ON TABLE cdc_catalog.catalog IS
    'Replication registry: one row per source object per conn_id. Discover writes identity; operator sets flags; ETL writes runtime state.';
