export type CatalogReplicationMode = "enable" | "disable";

export const ENABLE_CATALOG_REPLICATION_SQL = `
  UPDATE cdc_catalog.catalog
  SET active = true,
      cdc_enabled = true,
      capture_during_full_load = true,
      updated_at = now()
  WHERE catalog_id = $1::bigint
  RETURNING catalog_id, conn_id, source_schema, source_table, active, cdc_enabled, capture_during_full_load
`;

export const DISABLE_CATALOG_REPLICATION_SQL = `
  UPDATE cdc_catalog.catalog
  SET active = false,
      cdc_enabled = false,
      capture_during_full_load = false,
      updated_at = now()
  WHERE catalog_id = $1::bigint
  RETURNING catalog_id, conn_id, source_schema, source_table, active, cdc_enabled, capture_during_full_load
`;

export const LOG_CATALOG_REPLICATION_SQL = `
  INSERT INTO cdc_catalog.logs (level, component, message, conn_id, source_schema, source_table, context)
  VALUES ('info'::cdc_catalog.log_level, 'control_plane', $1, $2, $3, $4, $5::jsonb)
`;
