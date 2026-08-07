-- Fix deposit_note monthly partition overlap + unquarantine apply.
-- Symptom: ensure_monthly_partitions failed: partition "deposit_note_2026_11"
--          would overlap partition "deposit_note_2026_10"
--
-- Run on DataLake first (inspect + fix partitions), then on DataSync (unquarantine).
-- Adjust schema/table if your lake names differ.

-- =============================================================================
-- A) DataLake — inspect
-- =============================================================================
-- psql -h <datalake> -d <datalake_db> -v ON_ERROR_STOP=1

SELECT c.relname AS partition,
       pg_get_expr(c.relpartbound, c.oid) AS bounds
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'deposit_service_jetu_cr.deposit_note'::regclass
ORDER BY c.relname;

-- Row counts (safe drop only if empty / disposable)
SELECT 'deposit_note_2026_10' AS part, COUNT(*) AS rows
FROM deposit_service_jetu_cr.deposit_note_2026_10
UNION ALL
SELECT 'deposit_note_2026_11', COUNT(*)
FROM deposit_service_jetu_cr.deposit_note_2026_11;

-- =============================================================================
-- B) DataLake — typical fix when _2026_11 was created against bad/overlapping bounds
-- =============================================================================
-- 1) If _2026_11 is empty (or data can be re-applied via CDC), detach + drop it.
-- 2) Recreate months with the hardened helper (date literals, skip-on-overlap).

BEGIN;

ALTER TABLE deposit_service_jetu_cr.deposit_note
  DETACH PARTITION deposit_service_jetu_cr.deposit_note_2026_11;

DROP TABLE IF EXISTS deposit_service_jetu_cr.deposit_note_2026_11;

-- If _2026_10 bounds are wrong (e.g. open-ended or TO past Nov 1), fix similarly:
--   DETACH → recreate with FOR VALUES FROM ('2026-10-01') TO ('2026-11-01') → ATTACH
-- Only do that after confirming bounds from the inspect query above.

SELECT lake.ensure_monthly_partitions('deposit_service_jetu_cr', 'deposit_note', 3);

-- Re-check
SELECT c.relname AS partition,
       pg_get_expr(c.relpartbound, c.oid) AS bounds
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'deposit_service_jetu_cr.deposit_note'::regclass
ORDER BY c.relname;

COMMIT;

-- Optional: refresh lake helper without full DataSync migrate
-- CREATE OR REPLACE from cpp/include/prod_ops_embedded.hpp → datalake_lake()
-- or: DataSync migrate --lake

-- =============================================================================
-- C) DataSync — unquarantine apply_position (after lake partitions are healthy)
-- =============================================================================
-- psql -h <datasync> -d datasync -v ON_ERROR_STOP=1

BEGIN;

UPDATE cdc_catalog.apply_position
SET status = 'healthy'::cdc_catalog.cdc_health_status,
    last_error = NULL,
    quarantined_at = NULL,
    updated_at = now()
WHERE conn_id = 'MARIADB01'
  AND source_schema = 'deposit_service_jetu_cr'
  AND source_table = 'deposit_note'
  AND status::text = 'quarantined';

-- If catalog row was marked failed for the same reason:
UPDATE cdc_catalog.catalog
SET status = 'success',
    last_error = NULL,
    last_error_at = NULL,
    updated_at = now()
WHERE conn_id = 'MARIADB01'
  AND source_schema = 'deposit_service_jetu_cr'
  AND source_table = 'deposit_note'
  AND status::text = 'failed'
  AND COALESCE(last_error, '') ILIKE '%ensure_monthly_partitions%';

COMMIT;

-- Verify
SELECT ap.status, ap.last_error, ap.updated_at
FROM cdc_catalog.apply_position ap
WHERE ap.conn_id = 'MARIADB01'
  AND ap.source_schema = 'deposit_service_jetu_cr'
  AND ap.source_table = 'deposit_note';
