-- Recover MariaDB catalog tables stuck in failed after apply_position_object_uk races.
-- Run ONLY when a single DataSync instance is stopped (no overlapping daemon containers).
--
-- Usage:
--   psql -h <host> -U <user> -d datasync -v ON_ERROR_STOP=1 -f sql/recover_apply_position_object_uk_failed.sql

BEGIN;

DELETE FROM cdc_catalog.apply_position ap
USING cdc_catalog.catalog c
WHERE ap.catalog_id = c.catalog_id
  AND c.db_engine = 'mariadb'::cdc_catalog.db_engine
  AND c.status = 'failed'
  AND c.last_error LIKE '%apply_position_object_uk%';

UPDATE cdc_catalog.catalog
SET status = 'success',
    last_error = NULL,
    last_error_at = NULL,
    updated_at = now()
WHERE db_engine = 'mariadb'::cdc_catalog.db_engine
  AND status = 'failed'
  AND last_error LIKE '%apply_position_object_uk%';

COMMIT;
