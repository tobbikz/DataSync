/** Ops dashboard queries — live from base tables (MVs optional for heavy rollups). */

export const OPS_KPIS_SQL = `
  SELECT
    (SELECT COUNT(*)::int FROM cdc_catalog.connections WHERE active = true) AS connections,
    (SELECT COUNT(*)::int FROM cdc_catalog.catalog
       WHERE active = true AND cdc_enabled = true AND NOT needs_full_load) AS cdc_ready,
    (SELECT COUNT(*)::int FROM cdc_catalog.catalog
       WHERE active = true AND status = 'cdc_in_progress'::cdc_catalog.replication_status) AS cdc_in_progress,
    (SELECT COUNT(*)::int FROM cdc_catalog.catalog
       WHERE active = true AND needs_full_load) AS needs_full_load,
    (SELECT COUNT(*)::int FROM (
       SELECT DISTINCT ON (s.catalog_id) s.apply_health_rag
       FROM cdc_catalog.apply_batch_stats s
       WHERE s.logged_at >= now() - interval '3 days'
       ORDER BY s.catalog_id, s.logged_at DESC
     ) latest WHERE apply_health_rag = 'GREEN') AS apply_green,
    (SELECT COUNT(*)::int FROM (
       SELECT DISTINCT ON (s.catalog_id) s.apply_health_rag
       FROM cdc_catalog.apply_batch_stats s
       WHERE s.logged_at >= now() - interval '3 days'
       ORDER BY s.catalog_id, s.logged_at DESC
     ) latest WHERE apply_health_rag = 'AMBER') AS apply_amber,
    (SELECT COUNT(*)::int FROM (
       SELECT DISTINCT ON (s.catalog_id) s.apply_health_rag
       FROM cdc_catalog.apply_batch_stats s
       WHERE s.logged_at >= now() - interval '3 days'
       ORDER BY s.catalog_id, s.logged_at DESC
     ) latest WHERE apply_health_rag = 'RED') AS apply_red,
    (SELECT COUNT(*)::int FROM cdc_catalog.logs
       WHERE level = 'error' AND logged_at > now() - interval '24 hours') AS errors_24h,
    (SELECT COALESCE(SUM(lag), 0)::bigint FROM (
       SELECT DISTINCT ON (conn_id) kafka_consumer_lag AS lag
       FROM cdc_catalog.apply_batch_stats
       WHERE logged_at >= now() - interval '1 hour'
       ORDER BY conn_id, logged_at DESC
     ) t) AS total_kafka_lag,
    (SELECT COALESCE(SUM(dropped_unrecoverable), 0)::bigint FROM cdc_catalog.apply_batch_stats
       WHERE logged_at >= now() - interval '24 hours') AS dropped_unrecoverable_24h,
    (SELECT COALESCE(SUM(parse_skipped), 0)::bigint FROM cdc_catalog.apply_batch_stats
       WHERE logged_at >= now() - interval '24 hours') AS parse_skipped_24h,
    (SELECT COALESCE(SUM(dedup_skipped), 0)::bigint FROM cdc_catalog.apply_batch_stats
       WHERE logged_at >= now() - interval '24 hours') AS dedup_skipped_24h,
    (SELECT COUNT(*)::int FROM (
       SELECT DISTINCT ON (s.catalog_id) s.event_loss_status
       FROM cdc_catalog.apply_batch_stats s
       WHERE s.logged_at >= now() - interval '3 days'
       ORDER BY s.catalog_id, s.logged_at DESC
     ) latest WHERE event_loss_status <> 'ok') AS event_loss_tables,
    (SELECT COUNT(*)::int FROM cdc_catalog.capture_position
       WHERE status <> 'healthy'::cdc_catalog.cdc_health_status) AS capture_unhealthy,
    (SELECT COALESCE(MAX(capture_lag_seconds), 0)::bigint FROM cdc_catalog.capture_position) AS max_capture_lag,
    (SELECT COUNT(*)::int FROM cdc_catalog.catalog
       WHERE active = true AND status = 'quarantined'::cdc_catalog.replication_status) AS quarantined_tables,
    (SELECT COUNT(*)::int FROM cdc_catalog.apply_dlq
       WHERE status = 'parked') AS dlq_parked,
    (SELECT COALESCE(
       EXTRACT(EPOCH FROM (now() - MAX(logged_at)))::bigint, -1)
       FROM cdc_catalog.apply_batch_stats) AS seconds_since_last_slice
`;

export const OPS_EVENTS_HOURLY_SQL = `
  SELECT
    date_trunc('hour', s.logged_at) AS event_ts,
    s.conn_id,
    SUM(s.events_total)::bigint AS events_total,
    SUM(s.events_inserts)::bigint AS events_inserts,
    SUM(s.events_updates)::bigint AS events_updates,
    SUM(s.events_deletes)::bigint AS events_deletes
  FROM cdc_catalog.apply_batch_stats s
  WHERE s.logged_at >= now() - interval '3 days'
  GROUP BY 1, 2
  ORDER BY 1 ASC, 2 ASC
`;

export const OPS_CAPTURE_LAG_SQL = `
  SELECT
    cp.conn_id,
    cp.status::text AS capture_status,
    cp.capture_lag_seconds,
    CASE WHEN cp.status <> 'healthy'::cdc_catalog.cdc_health_status THEN 1 ELSE 0 END AS is_unhealthy
  FROM cdc_catalog.capture_position cp
  ORDER BY cp.capture_lag_seconds DESC NULLS LAST
`;

export const OPS_CATALOG_STATUS_SQL = `
  SELECT status::text AS status, COUNT(*)::int AS count
  FROM cdc_catalog.catalog
  WHERE active = true
  GROUP BY status
  ORDER BY count DESC
`;

export const OPS_ENGINE_MIX_SQL = `
  SELECT db_engine::text AS db_engine, COUNT(*)::int AS count
  FROM cdc_catalog.catalog
  WHERE active = true
  GROUP BY db_engine
  ORDER BY count DESC
`;

export const OPS_ERRORS_BY_COMPONENT_SQL = `
  SELECT component, COUNT(*)::int AS error_count
  FROM cdc_catalog.logs
  WHERE level = 'error' AND logged_at > now() - interval '24 hours'
  GROUP BY component
  ORDER BY error_count DESC
  LIMIT 12
`;

export const OPS_RECENT_ERRORS_SQL = `
  SELECT log_id, logged_at, level::text AS level, component, conn_id, message
  FROM cdc_catalog.logs
  WHERE level IN ('error', 'warning')
    AND logged_at > now() - interval '24 hours'
  ORDER BY logged_at DESC
  LIMIT 20
`;

export const OPS_APPLY_TAIL_SQL = `
  SELECT
    logged_at, conn_id, source_schema, source_table,
    apply_health_rag, events_total, events_inserts, events_updates, events_deletes,
    duration_ms, kafka_consumer_lag, capture_lag_seconds, health_reason
  FROM cdc_catalog.apply_batch_stats
  WHERE logged_at >= now() - interval '1 day'
  ORDER BY logged_at DESC
  LIMIT 30
`;

export const OPS_PIPELINE_TABLES_SQL = `
  SELECT
    c.conn_id,
    c.db_engine::text AS db_engine,
    c.source_schema,
    c.source_table,
    c.status::text AS status,
    c.cdc_enabled,
    c.needs_full_load,
    COALESCE(ev.events_24h, 0)::bigint AS events_24h,
    COALESCE(recent.events_5m, 0)::bigint AS events_5m,
    COALESCE(recent.events_inserts_5m, 0)::bigint AS events_inserts_5m,
    COALESCE(recent.events_updates_5m, 0)::bigint AS events_updates_5m,
    COALESCE(recent.events_deletes_5m, 0)::bigint AS events_deletes_5m,
    cp.status::text AS capture_status,
    cp.capture_lag_seconds,
    ap.apply_lag_seconds,
    ap.kafka_topic,
    ap.kafka_partition,
    COALESCE(lag.kafka_consumer_lag, 0)::bigint AS kafka_consumer_lag
  FROM cdc_catalog.catalog c
  LEFT JOIN cdc_catalog.capture_position cp ON cp.conn_id = c.conn_id
  LEFT JOIN cdc_catalog.apply_position ap ON ap.catalog_id = c.catalog_id
  LEFT JOIN LATERAL (
    SELECT SUM(s.events_total)::bigint AS events_24h
    FROM cdc_catalog.apply_batch_stats s
    WHERE s.catalog_id = c.catalog_id
      AND s.logged_at >= now() - interval '24 hours'
  ) ev ON true
  LEFT JOIN LATERAL (
    SELECT
      SUM(s.events_total)::bigint AS events_5m,
      SUM(s.events_inserts)::bigint AS events_inserts_5m,
      SUM(s.events_updates)::bigint AS events_updates_5m,
      SUM(s.events_deletes)::bigint AS events_deletes_5m
    FROM cdc_catalog.apply_batch_stats s
    WHERE s.catalog_id = c.catalog_id
      AND s.logged_at >= now() - interval '5 minutes'
  ) recent ON true
  LEFT JOIN LATERAL (
    SELECT s.kafka_consumer_lag
    FROM cdc_catalog.apply_batch_stats s
    WHERE s.catalog_id = c.catalog_id
    ORDER BY s.logged_at DESC
    LIMIT 1
  ) lag ON true
  WHERE c.active = true
  ORDER BY c.conn_id, c.source_schema, c.source_table
`;

export const REFRESH_OPS_VIEWS_SQL = `
  REFRESH MATERIALIZED VIEW CONCURRENTLY cdc_catalog.mv_tab_health_latest_3d;
  REFRESH MATERIALIZED VIEW CONCURRENTLY cdc_catalog.mv_tab_catalog;
  REFRESH MATERIALIZED VIEW CONCURRENTLY cdc_catalog.mv_tab_capture_latest;
  REFRESH MATERIALIZED VIEW CONCURRENTLY cdc_catalog.mv_tab_events_hourly_3d;
  REFRESH MATERIALIZED VIEW CONCURRENTLY cdc_catalog.mv_tab_kafka_hourly_3d;
  REFRESH MATERIALIZED VIEW CONCURRENTLY cdc_catalog.mv_tab_logs_hourly_3d;
`;

export const OPS_GAP_EVENTS_SQL = `
  SELECT
    gap_id,
    detected_at,
    conn_id,
    db_engine::text,
    gap_side,
    gap_kind,
    source_schema,
    source_table,
    detail,
    remediation,
    tables_flagged::int,
    resolved_at,
    batch_id
  FROM cdc_catalog.gap_events
  WHERE resolved_at IS NULL
  ORDER BY detected_at DESC
  LIMIT 25
`;

/** True hourly apply duration — matches Superset "Apply duration avg / hour". */
export const OPS_APPLY_DURATION_HOURLY_SQL = `
  SELECT
    date_trunc('hour', logged_at) AS event_ts,
    ROUND(AVG(duration_ms)::numeric, 1) AS avg_duration_ms,
    MAX(duration_ms)::bigint AS max_duration_ms
  FROM cdc_catalog.apply_batch_stats
  WHERE logged_at >= now() - interval '3 days'
  GROUP BY 1
  ORDER BY 1 ASC
`;

export const OPS_QUARANTINE_BY_CONN_SQL = `
  SELECT conn_id, COUNT(*)::int AS quarantined
  FROM cdc_catalog.catalog
  WHERE active = true
    AND status = 'quarantined'::cdc_catalog.replication_status
  GROUP BY conn_id
  ORDER BY quarantined DESC, conn_id
`;

export const OPS_EVENT_LOSS_MIX_SQL = `
  SELECT event_loss_status AS status, COUNT(*)::int AS count
  FROM (
    SELECT DISTINCT ON (catalog_id) event_loss_status
    FROM cdc_catalog.apply_batch_stats
    WHERE logged_at >= now() - interval '3 days'
    ORDER BY catalog_id, logged_at DESC
  ) latest
  GROUP BY event_loss_status
  ORDER BY count DESC
`;

export const OPS_TOP_LAG_TABLES_SQL = `
  SELECT
    conn_id,
    source_schema,
    source_table,
    kafka_consumer_lag::bigint AS kafka_consumer_lag,
    apply_health_rag
  FROM (
    SELECT DISTINCT ON (catalog_id)
      conn_id, source_schema, source_table, kafka_consumer_lag, apply_health_rag
    FROM cdc_catalog.apply_batch_stats
    WHERE logged_at >= now() - interval '3 days'
    ORDER BY catalog_id, logged_at DESC
  ) latest
  WHERE COALESCE(kafka_consumer_lag, 0) > 0
  ORDER BY kafka_consumer_lag DESC
  LIMIT 12
`;

export const OPS_DLQ_TAIL_SQL = `
  SELECT
    dlq_id,
    logged_at,
    conn_id,
    source_schema,
    source_table,
    op,
    reason,
    error_sqlstate,
    LEFT(error_message, 240) AS error_message,
    status
  FROM cdc_catalog.apply_dlq
  WHERE status = 'parked'
  ORDER BY logged_at DESC
  LIMIT 25
`;
