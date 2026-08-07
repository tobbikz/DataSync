-- Clear sticky catalog.failed caused by transient apply_position upsert races
-- (apply_position_object_uk / apply_position_pkey / 23505 / 40P01).
-- Covers MariaDB + MSSQL (e.g. MSSQL_CRM).
--
-- Does NOT delete apply_position rows (preserves kafka_offset). Missing rows are
-- re-seeded by DataSync after the upsert fix (apply seed-only / onboard ensure).
--
-- Prefer: deploy fixed binary first, then run this, then start daemon.
-- If overlapping daemons are running, stop them first to avoid re-failing mid-script.
--
-- Usage:
--   psql -h <host> -U <user> -d datasync -v ON_ERROR_STOP=1 -f sql/recover_apply_position_object_uk_failed.sql

BEGIN;

UPDATE cdc_catalog.catalog
SET status = 'success',
    last_error = NULL,
    last_error_at = NULL,
    updated_at = now()
WHERE status = 'failed'
  AND last_error LIKE '%apply_position%upsert failed%'
  AND (
    last_error LIKE '%apply_position_object_uk%'
    OR last_error LIKE '%apply_position_pkey%'
    OR last_error LIKE '%23505%'
    OR last_error LIKE '%40P01%'
  );
  -- Optional filter: AND conn_id = 'MSSQL_CRM'

COMMIT;
