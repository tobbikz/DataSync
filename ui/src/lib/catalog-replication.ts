export type CatalogReplicationMode =
  | "enable"
  | "disable"
  | "reset"
  | "unquarantine"
  | "scd2-enable"
  | "scd2-disable";

/**
 * SCD Type 2 history is deliberately its own switch: a table can replicate for years
 * without history, and turning history on or off must not disturb whether it replicates.
 * That is why enable/disable above never touch this column.
 */
export const SET_CATALOG_SCD2_SQL = `
  UPDATE cdc_catalog.catalog
  SET scd2_enabled = $2::boolean,
      updated_at = now()
  WHERE catalog_id = $1::bigint
  RETURNING catalog_id, conn_id, source_schema, source_table, active, cdc_enabled,
            capture_during_full_load, scd2_enabled
`;

export const ENABLE_CATALOG_REPLICATION_SQL = `
  UPDATE cdc_catalog.catalog
  SET active = true,
      cdc_enabled = true,
      capture_during_full_load = true,
      updated_at = now()
  WHERE catalog_id = $1::bigint
  RETURNING catalog_id, conn_id, source_schema, source_table, active, cdc_enabled,
            capture_during_full_load, scd2_enabled
`;

export const DISABLE_CATALOG_REPLICATION_SQL = `
  UPDATE cdc_catalog.catalog
  SET active = false,
      cdc_enabled = false,
      capture_during_full_load = false,
      updated_at = now()
  WHERE catalog_id = $1::bigint
  RETURNING catalog_id, conn_id, source_schema, source_table, active, cdc_enabled,
            capture_during_full_load, scd2_enabled
`;

export const LOG_CATALOG_REPLICATION_SQL = `
  INSERT INTO cdc_catalog.logs (level, component, message, conn_id, source_schema, source_table, context)
  VALUES ('info'::cdc_catalog.log_level, 'control_plane', $1, $2, $3, $4, $5::jsonb)
`;

/** Clears apply_position + catalog quarantine; returns apply_rows and catalog_rows updated. */
export const UNQUARANTINE_CATALOG_SQL = `
WITH apply_upd AS (
  UPDATE cdc_catalog.apply_position
  SET status = 'healthy'::cdc_catalog.cdc_health_status,
      quarantined_at = NULL,
      quarantine_reason = NULL,
      last_error = NULL,
      updated_at = now()
  WHERE catalog_id = $1::bigint
    AND status = 'quarantined'::cdc_catalog.cdc_health_status
  RETURNING catalog_id
),
catalog_upd AS (
  UPDATE cdc_catalog.catalog c
  SET status = CASE
        WHEN c.needs_full_load THEN 'pending'::cdc_catalog.replication_status
        WHEN c.cdc_enabled THEN 'success'::cdc_catalog.replication_status
        ELSE 'pending'::cdc_catalog.replication_status
      END,
      last_error = NULL,
      last_error_at = NULL,
      updated_at = now()
  WHERE c.catalog_id = $1::bigint
    AND c.status = 'quarantined'::cdc_catalog.replication_status
  RETURNING c.catalog_id
)
SELECT
  (SELECT COUNT(*)::int FROM apply_upd) AS apply_rows,
  (SELECT COUNT(*)::int FROM catalog_upd) AS catalog_rows,
  c.catalog_id,
  c.conn_id,
  c.source_schema,
  c.source_table,
  c.active,
  c.cdc_enabled,
  c.capture_during_full_load,
  c.scd2_enabled
FROM cdc_catalog.catalog c
WHERE c.catalog_id = $1::bigint
`;
