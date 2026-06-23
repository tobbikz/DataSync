-- One-shot generator for dl_mir_*_pk indexes on MariaDB mirror tables (lake schema = source_schema).
-- Run each CREATE INDEX CONCURRENTLY outside a transaction block.
-- MSSQL/Mongo lake names differ; use DataSync apply worker backfill or full-load for those engines.

SELECT format(
  'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I.%I (%s);',
  left('dl_mir_' || c.source_schema || '_' || c.source_table || '_pk', 63),
  c.source_schema,
  c.source_table,
  (
    SELECT string_agg(quote_ident(trim(pk)), ', ' ORDER BY ord)
    FROM unnest(string_to_array(c.pk_columns, ',')) WITH ORDINALITY AS t(pk, ord)
  )
) AS ddl
FROM cdc_catalog.catalog c
WHERE c.active = true
  AND c.has_pk = true
  AND c.db_engine = 'mariadb'
  AND trim(COALESCE(c.pk_columns, '')) <> ''
  AND c.status NOT IN ('skipped', 'disabled')
ORDER BY c.source_schema, c.source_table;
