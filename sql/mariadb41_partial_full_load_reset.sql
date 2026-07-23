-- Partial full-load reset for MARIADB41: all tables EXCEPT casino.transactions + universal_casino.transactions
--
-- Keeps:
--   casino.transactions          → full_load_in_progress + checkpoint (COPY in flight)
--   universal_casino.transactions → disabled (capture off until dedicated full load)
-- Does NOT touch capture_position (CDC binlog cursor preserved).
--
-- Order:
--   1. systemctl stop DataSync.service
--   2. Run SECTION A on database DataSync (cdc_catalog)
--   3. Run SECTION B generator on DataSync, then run TRUNCATE output on DataLake
--   4. systemctl start DataSync.service

-- =============================================================================
-- SECTION A — DataSync (cdc_catalog)
-- =============================================================================

BEGIN;

-- 1) Disable universal_casino.transactions capture until Plan B full load
UPDATE cdc_catalog.catalog
SET active = true,
    cdc_enabled = false,
    needs_full_load = true,
    capture_during_full_load = false,
    status = 'disabled'::cdc_catalog.replication_status,
    last_error = NULL,
    last_error_at = NULL,
    updated_at = now()
WHERE conn_id = 'MARIADB41'
  AND db_engine = 'mariadb'
  AND source_schema = 'universal_casino'
  AND source_table = 'transactions';

-- 2) Ensure casino.transactions stays on in-flight full load (do not reset checkpoint)
UPDATE cdc_catalog.catalog
SET active = true,
    cdc_enabled = true,
    needs_full_load = true,
    capture_during_full_load = true,
    status = 'full_load_in_progress'::cdc_catalog.replication_status,
    last_error = NULL,
    last_error_at = NULL,
    updated_at = now()
WHERE conn_id = 'MARIADB41'
  AND db_engine = 'mariadb'
  AND source_schema = 'casino'
  AND source_table = 'transactions';

-- 3) Reset all other MARIADB41 tables for clean full load
UPDATE cdc_catalog.catalog
SET active = true,
    cdc_enabled = true,
    needs_full_load = true,
    capture_during_full_load = false,
    status = 'pending'::cdc_catalog.replication_status,
    last_error = NULL,
    last_error_at = NULL,
    engine_meta = COALESCE(engine_meta, '{}'::jsonb)
        - 'full_load_fail_count'
        - 'stream_kafka_offsets'
        - 'stream_bookmarked_at',
    updated_at = now()
WHERE conn_id = 'MARIADB41'
  AND db_engine = 'mariadb'
  AND NOT (
      source_schema = 'casino' AND source_table = 'transactions'
      OR source_schema = 'universal_casino' AND source_table = 'transactions'
  );

-- 4) Drop full-load checkpoints except the two transactions tables
DELETE FROM cdc_catalog.full_load_checkpoint
WHERE catalog_id IN (
    SELECT catalog_id
    FROM cdc_catalog.catalog
    WHERE conn_id = 'MARIADB41'
      AND db_engine = 'mariadb'
      AND NOT (
          source_schema = 'casino' AND source_table = 'transactions'
          OR source_schema = 'universal_casino' AND source_table = 'transactions'
      )
);

-- 5) Verify catalog state
SELECT catalog_id,
       source_schema,
       source_table,
       status,
       needs_full_load,
       capture_during_full_load,
       cdc_enabled,
       last_error
FROM cdc_catalog.catalog
WHERE conn_id = 'MARIADB41'
ORDER BY source_schema, source_table;

SELECT c.source_schema,
       c.source_table,
       cp.catalog_id IS NOT NULL AS has_checkpoint
FROM cdc_catalog.catalog c
LEFT JOIN cdc_catalog.full_load_checkpoint cp ON cp.catalog_id = c.catalog_id
WHERE c.conn_id = 'MARIADB41'
  AND c.source_table = 'transactions'
  AND c.source_schema IN ('casino', 'universal_casino')
ORDER BY c.source_schema;

COMMIT;

-- =============================================================================
-- SECTION B — DataLake TRUNCATE generator (run on DataSync, copy output to DataLake)
-- =============================================================================
-- Produces one TRUNCATE per lake table; excludes casino/universal_casino.transactions.

SELECT format(
    'TRUNCATE TABLE %I.%I;',
    source_schema,
    source_table
) AS truncate_sql
FROM cdc_catalog.catalog
WHERE conn_id = 'MARIADB41'
  AND db_engine = 'mariadb'
  AND active = true
  AND NOT (
      source_schema = 'casino' AND source_table = 'transactions'
      OR source_schema = 'universal_casino' AND source_table = 'transactions'
  )
ORDER BY source_schema, source_table;

-- Run generated statements on DataLake inside:
-- BEGIN;
--   <paste TRUNCATE lines>
-- COMMIT;
