-- Retention: purge cdc_catalog.logs older than N days (default 7)

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
    'Delete log rows older than retention window (append-only table).';
