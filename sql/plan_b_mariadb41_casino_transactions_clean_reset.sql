-- Clean full-load reset for MARIADB41 casino.transactions ONLY
--
-- Use when lake has duplicate/partial data or after verify/truncate incidents.
-- Does NOT touch capture_position (binlog cursor preserved).
-- Does NOT touch universal_casino or other casino tables.
--
-- Order:
--   1. systemctl stop DataSync.service
--   2. Deploy binary with streaming verify + resume fixes
--   3. SECTION A on database DataSync
--   4. SECTION B on database DataLake
--   5. systemctl start DataSync.service

-- =============================================================================
-- SECTION A — DataSync (cdc_catalog)
-- =============================================================================

BEGIN;

UPDATE cdc_catalog.catalog
SET active = true,
    cdc_enabled = true,
    needs_full_load = true,
    capture_during_full_load = true,
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
  AND source_schema = 'casino'
  AND source_table = 'transactions';

DELETE FROM cdc_catalog.full_load_checkpoint
WHERE catalog_id = (
    SELECT catalog_id
    FROM cdc_catalog.catalog
    WHERE conn_id = 'MARIADB41'
      AND db_engine = 'mariadb'
      AND source_schema = 'casino'
      AND source_table = 'transactions'
);

SELECT catalog_id,
       source_schema,
       source_table,
       status,
       needs_full_load,
       capture_during_full_load,
       cdc_enabled,
       last_error,
       engine_meta
FROM cdc_catalog.catalog
WHERE conn_id = 'MARIADB41'
  AND source_schema = 'casino'
  AND source_table = 'transactions';

SELECT COUNT(*) AS checkpoints_remaining
FROM cdc_catalog.full_load_checkpoint cp
JOIN cdc_catalog.catalog c ON c.catalog_id = cp.catalog_id
WHERE c.conn_id = 'MARIADB41'
  AND c.source_schema = 'casino'
  AND c.source_table = 'transactions';

COMMIT;

-- =============================================================================
-- SECTION B — DataLake (run connected to DataLake)
-- =============================================================================

BEGIN;
TRUNCATE TABLE casino.transactions;
COMMIT;

-- Optional sanity (estimate — avoid COUNT(*) on empty/small table right after truncate)
-- SELECT reltuples::bigint FROM pg_class c
-- JOIN pg_namespace n ON n.oid = c.relnamespace
-- WHERE n.nspname = 'casino' AND c.relname = 'transactions';
