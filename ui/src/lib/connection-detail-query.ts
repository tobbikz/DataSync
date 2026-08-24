export const CONNECTION_CATALOG_SUMMARY_SQL = `
  SELECT
    COUNT(*)::int AS tables_total,
    COUNT(*) FILTER (WHERE active)::int AS tables_active,
    COUNT(*) FILTER (WHERE active AND cdc_enabled AND NOT needs_full_load)::int AS cdc_ready,
    COUNT(*) FILTER (WHERE needs_full_load)::int AS needs_full_load,
    COUNT(*) FILTER (WHERE status = 'success'::cdc_catalog.replication_status)::int AS success,
    COUNT(*) FILTER (WHERE status IN ('error'::cdc_catalog.replication_status, 'failed'::cdc_catalog.replication_status))::int AS failed
  FROM cdc_catalog.catalog
  WHERE conn_id = $1
`;

export const CONNECTION_HOURLY_METRICS_SQL = `
  SELECT
    date_trunc('hour', logged_at) AS bucket,
    COALESCE(SUM(events_total), 0)::bigint AS events,
    COALESCE(MAX(kafka_consumer_lag), 0)::bigint AS kafka_lag,
    COALESCE(AVG(apply_lag_seconds) FILTER (WHERE apply_lag_seconds IS NOT NULL), 0)::int AS apply_lag
  FROM cdc_catalog.apply_batch_stats
  WHERE conn_id = $1
    AND logged_at >= now() - interval '24 hours'
  GROUP BY 1
  ORDER BY 1 ASC
`;

export const CONNECTION_RECENT_EVENTS_SQL = `
  SELECT COALESCE(SUM(events_total), 0)::bigint AS events_5m
  FROM cdc_catalog.apply_batch_stats
  WHERE conn_id = $1
    AND logged_at >= now() - interval '5 minutes'
`;

export const CONNECTION_CAPTURE_SQL = `
  SELECT status::text, capture_lag_seconds
  FROM cdc_catalog.capture_position
  WHERE conn_id = $1
`;
