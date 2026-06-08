-- Mongo apply_batch_stats parity: backfill empty source_schema + drop orphaned catalog_id rows.

UPDATE cdc_catalog.apply_batch_stats abs
SET source_schema = COALESCE(NULLIF(c.source_schema, ''), c.source_database)
FROM cdc_catalog.catalog c
WHERE c.conn_id = abs.conn_id
  AND c.source_table = abs.source_table
  AND c.db_engine = 'mongodb'
  AND (abs.source_schema IS NULL OR abs.source_schema = '');

DELETE FROM cdc_catalog.apply_batch_stats abs
WHERE abs.conn_id IN (SELECT conn_id FROM cdc_catalog.connections WHERE db_engine = 'mongodb')
  AND abs.catalog_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM cdc_catalog.catalog c WHERE c.catalog_id = abs.catalog_id
  );
