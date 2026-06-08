-- Operational views for stale, starved, and gap monitoring

CREATE OR REPLACE VIEW cdc_catalog.v_apply_stale AS
SELECT
    ap.catalog_id,
    ap.conn_id,
    ap.source_schema,
    ap.source_table,
    c.service_tier,
    ap.last_applied_at,
    ap.apply_lag_seconds,
    ap.status,
    ap.quarantined_at,
    ap.last_error,
    now() - ap.last_applied_at AS lag_interval
FROM cdc_catalog.apply_position ap
JOIN cdc_catalog.catalog c USING (catalog_id)
WHERE c.active = true
  AND c.cdc_enabled = true
  AND c.needs_full_load = false
  AND ap.status IN ('stale', 'lagging', 'gap_detected', 'quarantined', 'failed');

CREATE OR REPLACE VIEW cdc_catalog.v_capture_health AS
SELECT
    cp.conn_id,
    cp.gtid_set,
    cp.binlog_file,
    cp.binlog_position,
    cp.last_event_ts,
    cp.capture_lag_seconds,
    cp.status,
    cp.last_error,
    cp.updated_at
FROM cdc_catalog.capture_position cp;

CREATE OR REPLACE VIEW cdc_catalog.v_cdc_pipeline_summary AS
SELECT
    c.conn_id,
    c.service_tier::text AS service_tier,
    count(*) FILTER (WHERE c.active AND c.cdc_enabled AND NOT c.needs_full_load) AS cdc_ready,
    count(*) FILTER (WHERE ap.status = 'healthy') AS apply_healthy,
    count(*) FILTER (WHERE ap.status IN ('stale', 'lagging')) AS apply_lagging,
    count(*) FILTER (WHERE ap.status = 'quarantined') AS apply_quarantined,
    max(ap.apply_lag_seconds) AS max_apply_lag_seconds
FROM cdc_catalog.catalog c
LEFT JOIN cdc_catalog.apply_position ap USING (catalog_id)
WHERE c.db_engine = 'mariadb'
GROUP BY c.conn_id, c.service_tier;

COMMENT ON VIEW cdc_catalog.v_apply_stale IS 'Tables with elevated apply lag or unhealthy status.';
COMMENT ON VIEW cdc_catalog.v_capture_health IS 'GTID capture cursor health per conn_id.';
