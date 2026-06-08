-- MSSQL full-load runtime keys (mirror mariadb_load; Python bootstrap/catchup reads mssql_load)
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('full_load_batch_size', 'mssql_load', '', '5000'::jsonb, 'Rows per SELECT/INSERT batch (Python full-load)'),
    ('full_load_workers', 'mssql_load', '', '1'::jsonb, 'Parallel worker threads per table (PK numeric keyset; 1=sequential)'),
    ('full_load_parallel_tables', 'mssql_load', '', '1'::jsonb, 'Max tables loaded in parallel per conn'),
    ('full_load_source_sleep_ms', 'mssql_load', '', '0'::jsonb, 'Sleep ms between source read batches'),
    ('lake_partition_months_ahead', 'mssql_load', '', '3'::jsonb, 'Monthly partitions to create ahead of current month'),
    ('ddl_sync_indexes', 'mssql_load', '', 'true'::jsonb, 'Sync secondary indexes after truncate (parity mariadb_load)'),
    ('ddl_sync_foreign_keys', 'mssql_load', '', 'true'::jsonb, 'Sync FK constraints after truncate (parity mariadb_load)'),
    ('ddl_sync_columns', 'mssql_load', '', 'true'::jsonb, 'ADD COLUMN for new MSSQL columns vs lake')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
