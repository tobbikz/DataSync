-- Move lake partition helpers + observability logs into cdc_catalog (single control plane).

-- === Logs ===
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

DO $$
BEGIN
    IF to_regclass('observability.logs') IS NOT NULL THEN
        INSERT INTO cdc_catalog.logs (
            log_id, logged_at, level, component, message, context,
            batch_id, conn_id, source_schema, source_table
        )
        OVERRIDING SYSTEM VALUE
        SELECT
            o.log_id,
            o.logged_at,
            o.level::text::cdc_catalog.log_level,
            o.component,
            o.message,
            o.context,
            o.batch_id,
            o.conn_id,
            o.source_schema,
            o.source_table
        FROM observability.logs o
        WHERE NOT EXISTS (
            SELECT 1 FROM cdc_catalog.logs c WHERE c.log_id = o.log_id
        );
    END IF;
END $$;

DO $$
DECLARE
    seq_name text;
    max_id bigint;
BEGIN
    SELECT pg_get_serial_sequence('cdc_catalog.logs', 'log_id') INTO seq_name;
    IF seq_name IS NOT NULL THEN
        SELECT COALESCE(MAX(log_id), 1) INTO max_id FROM cdc_catalog.logs;
        EXECUTE format('SELECT setval(%L, %s, true)', seq_name, max_id);
    END IF;
END $$;

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

CREATE OR REPLACE FUNCTION cdc_catalog.purge_logs(p_retention_days integer DEFAULT 7)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    deleted bigint;
BEGIN
    IF p_retention_days IS NULL OR p_retention_days < 1 THEN
        RETURN 0;
    END IF;
    DELETE FROM cdc_catalog.logs
    WHERE logged_at < now() - make_interval(days => p_retention_days);
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END;
$$;

COMMENT ON FUNCTION cdc_catalog.purge_logs(integer) IS
    'Delete observability log rows older than retention window (append-only table).';

-- === Lake monthly partitions (was schema lake) ===
CREATE OR REPLACE FUNCTION cdc_catalog.month_bounds(p_month date)
RETURNS TABLE(start_date date, end_date date)
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT date_trunc('month', p_month)::date,
           (date_trunc('month', p_month) + interval '1 month')::date;
$$;

CREATE OR REPLACE FUNCTION cdc_catalog.ensure_monthly_partitions(
    p_schema text,
    p_table text,
    p_months_ahead integer DEFAULT 3
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    m date;
    end_m date;
    part_name text;
    start_d date;
    end_d date;
    created integer := 0;
BEGIN
    m := date_trunc('month', CURRENT_DATE)::date;
    end_m := (date_trunc('month', CURRENT_DATE) + make_interval(months => p_months_ahead + 1))::date;

    WHILE m < end_m LOOP
        SELECT lb.start_date, lb.end_date INTO start_d, end_d FROM cdc_catalog.month_bounds(m) lb;
        part_name := format('%s_%s', p_table, to_char(start_d, 'YYYY_MM'));

        IF to_regclass(format('%I.%I', p_schema, part_name)) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
                p_schema, part_name, p_schema, p_table, start_d, end_d
            );
            created := created + 1;
        END IF;
        m := (m + interval '1 month')::date;
    END LOOP;
    RETURN created;
END;
$$;

COMMENT ON FUNCTION cdc_catalog.ensure_monthly_partitions IS
    'Create monthly RANGE partitions on _dl_load_date for a partitioned lake table.';

UPDATE cdc_catalog.runtime_config
SET description = 'cdc_catalog.logs purge age in days',
    updated_at = now()
WHERE config_key = 'logs_retention_days'
  AND component = 'global'
  AND conn_id = '';

DROP SCHEMA IF EXISTS observability CASCADE;
DROP SCHEMA IF EXISTS lake CASCADE;
