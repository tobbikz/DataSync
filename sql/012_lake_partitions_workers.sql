-- Lake table monthly partitions on _dl_load_date (helpers live in cdc_catalog)

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

-- Runtime: workers + native capture
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_worker_count', 'cdc_kafka_apply', '', '4'::jsonb, 'Total apply worker processes (hash sharding by catalog_id)'),
    ('capture_worker_count', 'cdc_kafka_capture', '', '1'::jsonb, 'Total capture worker processes (hash sharding by catalog_id)'),
    ('capture_binlog_mode', 'cdc_kafka_capture', '', '"native"'::jsonb, 'native (pymysqlreplication) | cli (mariadb-binlog)'),
    ('catalog_default_tier', 'cdc_catalog', '', '"bronze"'::jsonb, 'Default service_tier for discovered tables'),
    ('lake_partition_months_ahead', 'mariadb_load', '', '3'::jsonb, 'Monthly partitions to create ahead of current month')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
