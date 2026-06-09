-- DataLake helpers — apply to config.json → datalake.database (idempotent via install).
-- Partition DDL for raw/staging tables lives here; control plane stays in cdc_catalog (datasync DB).

CREATE SCHEMA IF NOT EXISTS lake;

CREATE OR REPLACE FUNCTION lake.month_bounds(p_month date)
    RETURNS TABLE(start_date date, end_date date)
    LANGUAGE sql
    IMMUTABLE
    AS $$
    SELECT date_trunc('month', p_month)::date,
           (date_trunc('month', p_month) + interval '1 month')::date;
$$;

CREATE OR REPLACE FUNCTION lake.ensure_monthly_partitions(
    p_schema text,
    p_table text,
    p_months_ahead integer DEFAULT 3
) RETURNS integer
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
        SELECT lb.start_date, lb.end_date INTO start_d, end_d FROM lake.month_bounds(m) lb;
        part_name := format('%s_%s', p_table, to_char(start_d, 'YYYY_MM'));

        IF to_regclass(format('%I.%I', p_schema, part_name)) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
                p_schema, part_name, p_schema, p_table,
                start_d::text || ' 00:00:00+00',
                end_d::text || ' 00:00:00+00'
            );
            created := created + 1;
        END IF;
        m := (m + interval '1 month')::date;
    END LOOP;
    RETURN created;
END;
$$;

COMMENT ON FUNCTION lake.ensure_monthly_partitions(text, text, integer) IS
    'Create monthly RANGE partitions on _dl_load_timestamp for a partitioned lake table.';
