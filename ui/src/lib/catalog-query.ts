import type { CatalogFilters } from "./catalog-types";

const STATS_LATERAL = `
  LEFT JOIN LATERAL (
    SELECT
      s.kafka_consumer_lag,
      s.apply_lag_seconds,
      s.capture_lag_seconds,
      s.apply_health_rag,
      s.health_reason,
      s.is_quarantined
    FROM cdc_catalog.apply_batch_stats s
    WHERE s.catalog_id = c.catalog_id
    ORDER BY s.logged_at DESC
    LIMIT 1
  ) stats ON true
`;

const BASE_COLUMNS = `
  c.catalog_id,
  c.conn_id,
  c.db_engine::text AS db_engine,
  c.source_schema,
  c.source_table,
  c.active,
  c.cdc_enabled,
  c.capture_during_full_load,
  c.scd2_enabled,
  c.status::text AS status,
  c.needs_full_load,
  c.last_full_load_at,
  stats.kafka_consumer_lag,
  stats.capture_lag_seconds AS stats_capture_lag_seconds,
  stats.apply_lag_seconds AS stats_apply_lag_seconds,
  stats.apply_health_rag,
  stats.health_reason,
  stats.is_quarantined AS stats_quarantined,
  ap.status::text AS apply_status,
  ap.apply_lag_seconds AS position_apply_lag_seconds,
  ap.last_applied_at,
  ap.last_error,
  ap.quarantine_reason
`;

const ENRICHED_SELECT = `
  SELECT
    b.*,
    (
      b.status = 'quarantined'
      OR b.apply_status = 'quarantined'
      OR COALESCE(b.stats_quarantined, false)
    ) AS quarantined,
    CASE
      WHEN b.status = 'quarantined' OR b.apply_status = 'quarantined'
        OR COALESCE(b.stats_quarantined, false) THEN 'RED'
      WHEN b.status = 'error' THEN 'RED'
      WHEN b.needs_full_load
        OR b.status IN ('needs_full_load', 'full_load_in_progress', 'pending') THEN
        CASE COALESCE(NULLIF(b.apply_health_rag, ''), 'UNKNOWN')
          WHEN 'RED' THEN 'RED'
          ELSE 'AMBER'
        END
      WHEN NOT b.cdc_enabled THEN 'UNKNOWN'
      WHEN COALESCE(NULLIF(b.apply_health_rag, ''), '') <> '' THEN b.apply_health_rag
      WHEN b.kafka_consumer_lag >= 50000 THEN 'RED'
      WHEN b.kafka_consumer_lag >= 1000 THEN 'AMBER'
      WHEN b.status IN ('syncing', 'success') AND b.cdc_enabled THEN 'GREEN'
      ELSE 'UNKNOWN'
    END AS health_rag,
    b.kafka_consumer_lag AS kafka_lag,
    b.stats_capture_lag_seconds AS capture_lag_seconds,
    COALESCE(b.stats_apply_lag_seconds, b.position_apply_lag_seconds) AS apply_lag_seconds,
    COALESCE(b.quarantine_reason, b.last_error) AS quarantine_reason_resolved
  FROM base b
`;

export const CATALOG_ROW_SELECT = `
  catalog_id,
  conn_id,
  db_engine,
  source_schema,
  source_table,
  active,
  cdc_enabled,
  capture_during_full_load,
  scd2_enabled,
  status,
  needs_full_load,
  last_full_load_at,
  health_rag,
  kafka_lag,
  capture_lag_seconds,
  apply_lag_seconds,
  quarantined,
  quarantine_reason_resolved AS quarantine_reason,
  last_applied_at AS last_apply_at,
  CASE
    WHEN needs_full_load
      OR status IN ('needs_full_load', 'full_load_in_progress', 'pending') THEN
      CASE
        WHEN health_reason IS NULL OR TRIM(health_reason) = '' OR health_reason = 'healthy'
          THEN 'needs full-load reboot'
        ELSE health_reason || ' · needs full-load reboot'
      END
    ELSE health_reason
  END AS health_reason
`;

function catalogFromClause(whereSql: string) {
  return `
    FROM cdc_catalog.catalog c
    ${STATS_LATERAL}
    LEFT JOIN cdc_catalog.apply_position ap ON ap.catalog_id = c.catalog_id
    ${whereSql}
  `;
}

/** Shared list query — works on bootstrap + production schema. */
export function buildCatalogListQuery(filters: CatalogFilters) {
  const params: unknown[] = [];
  const where: string[] = [];
  let p = 1;

  function bind(value: unknown) {
    params.push(value);
    return `$${p++}`;
  }

  if (filters.conn) {
    where.push(`c.conn_id = ${bind(filters.conn)}`);
  }
  if (filters.status) {
    where.push(`c.status::text = ${bind(filters.status)}`);
  }
  if (filters.cdc === true) {
    where.push("c.cdc_enabled = true");
  } else if (filters.cdc === false) {
    where.push("c.cdc_enabled = false");
  }
  if (filters.needsFullLoad === true) {
    where.push("c.needs_full_load = true");
  } else if (filters.needsFullLoad === false) {
    where.push("c.needs_full_load = false");
  }
  if (filters.q?.trim()) {
    const pattern = `%${filters.q.trim()}%`;
    const a = bind(pattern);
    const b = bind(pattern);
    const c = bind(pattern);
    where.push(
      `(c.conn_id ILIKE ${a} OR c.source_schema ILIKE ${b} OR c.source_table ILIKE ${c})`,
    );
  }

  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const outerWhere: string[] = [];

  if (filters.rag) {
    outerWhere.push(`health_rag = ${bind(filters.rag)}`);
  }
  if (filters.quarantined === true) {
    outerWhere.push("quarantined = true");
  } else if (filters.quarantined === false) {
    outerWhere.push("quarantined = false");
  }

  const outerWhereSql = outerWhere.length
    ? `WHERE ${outerWhere.join(" AND ")}`
    : "";

  const limit = Math.min(filters.limit ?? 10, 100);
  const offset = Math.max(filters.offset ?? 0, 0);
  const limitParam = bind(limit);
  const offsetParam = bind(offset);

  const sql = `
    WITH base AS (
      SELECT ${BASE_COLUMNS}
      ${catalogFromClause(whereSql)}
    ),
    enriched AS (
      ${ENRICHED_SELECT}
    )
    SELECT
      ${CATALOG_ROW_SELECT},
      COUNT(*) OVER() AS total_count
    FROM enriched
    ${outerWhereSql}
    ORDER BY
      CASE health_rag
        WHEN 'RED' THEN 0
        WHEN 'AMBER' THEN 1
        WHEN 'UNKNOWN' THEN 2
        ELSE 3
      END,
      conn_id,
      source_schema,
      source_table
    LIMIT ${limitParam}
    OFFSET ${offsetParam}
  `;

  return { sql, params };
}

export function buildOverviewSummaryQuery() {
  const sql = `
    WITH base AS (
      SELECT ${BASE_COLUMNS}
      ${catalogFromClause("WHERE c.active = true")}
    ),
    enriched AS (
      ${ENRICHED_SELECT}
    )
    SELECT
      (SELECT COUNT(*)::int FROM cdc_catalog.connections WHERE active = true) AS connections,
      COUNT(*)::int AS tables_active,
      COUNT(*) FILTER (WHERE cdc_enabled)::int AS tables_cdc,
      (SELECT COUNT(*)::int FROM cdc_catalog.logs
         WHERE level = 'error' AND logged_at > now() - interval '24 hours') AS errors_24h,
      COUNT(*) FILTER (WHERE health_rag = 'RED')::int AS rag_red,
      COUNT(*) FILTER (WHERE health_rag = 'AMBER')::int AS rag_amber,
      COUNT(*) FILTER (WHERE health_rag = 'GREEN')::int AS rag_green,
      COUNT(*) FILTER (WHERE quarantined)::int AS quarantined,
      COUNT(*) FILTER (WHERE needs_full_load)::int AS needs_full_load
    FROM enriched
  `;
  return { sql, params: [] as unknown[] };
}

export function buildOverviewQueueQuery(limit = 50) {
  const sql = `
    WITH base AS (
      SELECT ${BASE_COLUMNS}
      ${catalogFromClause("WHERE c.active = true")}
    ),
    enriched AS (
      ${ENRICHED_SELECT}
    )
    SELECT ${CATALOG_ROW_SELECT}
    FROM enriched
    WHERE health_rag IN ('RED', 'AMBER')
       OR quarantined
       OR needs_full_load
       OR status IN ('error', 'full_load_in_progress', 'pending', 'needs_full_load')
    ORDER BY
      CASE health_rag WHEN 'RED' THEN 0 WHEN 'AMBER' THEN 1 ELSE 2 END,
      conn_id,
      source_schema,
      source_table
    LIMIT $1
  `;
  return { sql, params: [limit] };
}

export const catalogDetailSql = `
  WITH base AS (
    SELECT
      ${BASE_COLUMNS},
      c.has_pk,
      c.pk_columns,
      ap.kafka_topic,
      ap.kafka_partition,
      ap.kafka_offset,
      ap.quarantined_at
    ${catalogFromClause("WHERE c.catalog_id = $1")}
  ),
  enriched AS (
    ${ENRICHED_SELECT}
  )
  SELECT
    ${CATALOG_ROW_SELECT},
    has_pk,
    pk_columns,
    apply_status,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    quarantined_at,
    last_error,
    position_apply_lag_seconds
  FROM enriched
`;
