-- Mongo catalog parity: source_schema = source_database (like reconcile/stats keys).

UPDATE cdc_catalog.catalog
SET source_schema = source_database,
    updated_at = now()
WHERE db_engine = 'mongodb'
  AND (source_schema IS NULL OR source_schema = '');

UPDATE cdc_catalog.apply_position ap
SET source_schema = c.source_database,
    updated_at = now()
FROM cdc_catalog.catalog c
WHERE ap.catalog_id = c.catalog_id
  AND c.db_engine = 'mongodb'
  AND (ap.source_schema IS NULL OR ap.source_schema = '');

UPDATE cdc_catalog.reconciliation_result rr
SET source_schema = c.source_database
FROM cdc_catalog.catalog c
WHERE rr.catalog_id = c.catalog_id
  AND c.db_engine = 'mongodb'
  AND (rr.source_schema IS NULL OR rr.source_schema = '');
