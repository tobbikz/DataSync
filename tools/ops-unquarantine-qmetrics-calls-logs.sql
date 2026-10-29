-- 1) DataLake: add missing column
ALTER TABLE qmetrics.calls_logs ADD COLUMN IF NOT EXISTS agent_account text NULL;

-- 2) DataSync: unquarantine catalog_id 449
WITH apply_upd AS (
  UPDATE cdc_catalog.apply_position
  SET status = 'healthy'::cdc_catalog.cdc_health_status,
      quarantined_at = NULL,
      quarantine_reason = NULL,
      last_error = NULL,
      updated_at = now()
  WHERE catalog_id = 449
    AND status = 'quarantined'::cdc_catalog.cdc_health_status
  RETURNING catalog_id
),
catalog_upd AS (
  UPDATE cdc_catalog.catalog c
  SET last_error = NULL,
      last_error_at = NULL,
      updated_at = now()
  WHERE c.catalog_id = 449
  RETURNING c.catalog_id
)
SELECT
  (SELECT COUNT(*)::int FROM apply_upd) AS apply_rows,
  (SELECT COUNT(*)::int FROM catalog_upd) AS catalog_rows;

-- 3) On datasync host after SQL:
-- docker compose -f /opt/DataSync/docker-compose.yml restart datasync
