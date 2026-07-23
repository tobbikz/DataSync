-- Plan B: clean full-load reset for MARIADB41 casino.transactions + universal_casino.transactions
-- Run AFTER deploying DataSync binary with baseline verify + binlog resolver fixes.
--
-- Order:
--   1. systemctl stop DataSync.service
--   2. Run SECTION A on database DataSync (cdc_catalog)
--   3. Run SECTION B on database DataLake (lake tables)
--   4. systemctl start DataSync.service
--
-- Full load runs automatically (needs_full_load=true). Do NOT restart during COPY.

-- =============================================================================
-- SECTION A — DataSync (cdc_catalog)
-- =============================================================================

BEGIN;

-- 1) Ensure universal_casino.transactions exists in catalog (clone PK/metadata from casino)
INSERT INTO cdc_catalog.catalog (
    conn_id,
    db_engine,
    source_database,
    source_schema,
    source_table,
    has_pk,
    pk_columns,
    active,
    cdc_enabled,
    needs_full_load,
    capture_during_full_load,
    status,
    hot
)
SELECT
    conn_id,
    db_engine,
    source_database,
    'universal_casino',
    source_table,
    has_pk,
    pk_columns,
    true,
    true,
    true,
    true,
    'pending'::cdc_catalog.replication_status,
    hot
FROM cdc_catalog.catalog
WHERE conn_id = 'MARIADB41'
  AND db_engine = 'mariadb'
  AND source_schema = 'casino'
  AND source_table = 'transactions'
ON CONFLICT (conn_id, db_engine, source_database, source_schema, source_table)
DO UPDATE SET
    active = true,
    cdc_enabled = true,
    needs_full_load = true,
    capture_during_full_load = true,
    status = 'pending'::cdc_catalog.replication_status,
    last_error = NULL,
    last_error_at = NULL,
    engine_meta = COALESCE(cdc_catalog.catalog.engine_meta, '{}'::jsonb)
        - 'full_load_fail_count'
        - 'stream_kafka_offsets'
        - 'stream_bookmarked_at',
    updated_at = now();

-- 2) Reset casino.transactions catalog row
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

-- 3) Clear full-load checkpoints for both tables
DELETE FROM cdc_catalog.full_load_checkpoint
WHERE catalog_id IN (
    SELECT catalog_id
    FROM cdc_catalog.catalog
    WHERE conn_id = 'MARIADB41'
      AND db_engine = 'mariadb'
      AND source_table = 'transactions'
      AND source_schema IN ('casino', 'universal_casino')
);

-- 4) Verify catalog state
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
  AND source_table = 'transactions'
  AND source_schema IN ('casino', 'universal_casino')
ORDER BY source_schema;

SELECT COUNT(*) AS checkpoints_remaining
FROM cdc_catalog.full_load_checkpoint cp
JOIN cdc_catalog.catalog c ON c.catalog_id = cp.catalog_id
WHERE c.conn_id = 'MARIADB41'
  AND c.source_table = 'transactions'
  AND c.source_schema IN ('casino', 'universal_casino');

COMMIT;

-- =============================================================================
-- SECTION B — DataLake (run connected to DataLake, NOT DataSync)
-- =============================================================================
-- BEGIN;
-- TRUNCATE TABLE casino.transactions;
-- TRUNCATE TABLE universal_casino.transactions;
-- COMMIT;
--
-- Optional: confirm empty / size
-- SELECT pg_size_pretty(pg_total_relation_size('casino.transactions'));
-- SELECT pg_size_pretty(pg_total_relation_size('universal_casino.transactions'));
