-- CDC catalog diagnostics (prod: psql -d datasync -f deploy/diagnose-catalog.sql)
-- Why capture/apply show 0 events or no_tables

\echo '=== connections ==='
SELECT conn_id, engine, host, port, active
FROM cdc_catalog.connections
ORDER BY conn_id;

\echo '=== catalog summary by conn / tier ==='
SELECT
    conn_id,
    service_tier,
    db_engine,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE active) AS active,
    COUNT(*) FILTER (WHERE cdc_enabled) AS cdc_enabled,
    COUNT(*) FILTER (WHERE needs_full_load) AS needs_full_load,
    COUNT(*) FILTER (WHERE NOT has_pk) AS no_pk,
    COUNT(*) FILTER (WHERE status IN ('skipped', 'disabled')) AS skipped_or_disabled,
    COUNT(*) FILTER (
        WHERE active
          AND cdc_enabled
          AND NOT needs_full_load
          AND has_pk
          AND status NOT IN ('skipped', 'disabled')
    ) AS capture_ready
FROM cdc_catalog.catalog
GROUP BY conn_id, service_tier, db_engine
ORDER BY conn_id, service_tier;

\echo '=== capture_position (binlog cursor) ==='
SELECT conn_id, binlog_file, binlog_position, status, last_error, updated_at
FROM cdc_catalog.capture_position
ORDER BY conn_id;

\echo '=== recent capture logs ==='
SELECT created_at, level, component, message, context
FROM cdc_catalog.logs
WHERE component IN ('cdc_kafka_capture', 'cdc_kafka_daemon', 'cdc_kafka_apply_cpp')
  AND created_at > now() - interval '2 hours'
ORDER BY created_at DESC
LIMIT 30;
