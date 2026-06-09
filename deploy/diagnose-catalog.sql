-- CDC catalog diagnostics (prod: psql -d datasync -f deploy/diagnose-catalog.sql)
-- Why capture/apply show "skipped: no tables" despite status=success

\echo '=== connections (daemon only captures these conn_id) ==='
SELECT conn_id, engine, host, port, active
FROM cdc_catalog.connections
ORDER BY conn_id;

\echo '=== per-table capture eligibility (capture_ready must be true) ==='
SELECT
    conn_id,
    source_schema,
    source_table,
    active,
    cdc_enabled,
    needs_full_load,
    capture_during_full_load,
    has_pk,
    status,
    service_tier,
    (active
     AND cdc_enabled
     AND (NOT needs_full_load OR capture_during_full_load)
     AND has_pk
     AND status NOT IN ('skipped', 'disabled')) AS capture_ready,
    CASE
        WHEN NOT active THEN 'active=false'
        WHEN NOT cdc_enabled THEN 'cdc_enabled=false'
        WHEN needs_full_load AND NOT capture_during_full_load THEN 'needs_full_load=true'
        WHEN NOT has_pk THEN 'has_pk=false'
        WHEN status IN ('skipped', 'disabled') THEN 'status=' || status::text
        ELSE 'ok'
    END AS block_reason
FROM cdc_catalog.catalog
ORDER BY conn_id, capture_ready DESC, source_schema, source_table;

\echo '=== summary by conn / tier ==='
SELECT
    conn_id,
    service_tier,
    db_engine,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE active) AS active,
    COUNT(*) FILTER (WHERE cdc_enabled) AS cdc_enabled,
    COUNT(*) FILTER (WHERE needs_full_load) AS needs_full_load,
    COUNT(*) FILTER (WHERE capture_during_full_load) AS capture_during_full_load,
    COUNT(*) FILTER (WHERE status = 'success') AS status_success,
    COUNT(*) FILTER (
        WHERE active
          AND cdc_enabled
          AND (NOT needs_full_load OR capture_during_full_load)
          AND has_pk
          AND status NOT IN ('skipped', 'disabled')
    ) AS capture_ready
FROM cdc_catalog.catalog
GROUP BY conn_id, service_tier, db_engine
ORDER BY conn_id, service_tier;

\echo '=== capture_position (binlog cursor — required for MariaDB capture) ==='
SELECT conn_id, binlog_file, binlog_position, status, last_error, updated_at
FROM cdc_catalog.capture_position
ORDER BY conn_id;

\echo '=== recent capture skip / errors ==='
SELECT created_at, level, component, message, conn_id, context
FROM cdc_catalog.logs
WHERE component IN ('cdc_kafka_capture', 'cdc_kafka_daemon', 'cdc_kafka_apply_cpp')
  AND (message ILIKE '%skip%' OR message ILIKE '%capture%')
  AND created_at > now() - interval '6 hours'
ORDER BY created_at DESC
LIMIT 20;
