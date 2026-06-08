-- MongoDB full-load runtime keys (mirror mariadb_load / mssql_load; Python bootstrap/catchup reads mongo_load)
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('full_load_batch_size', 'mongo_load', '', '5000'::jsonb, 'Documents per find batch (C++/Python full-load)'),
    ('full_load_workers', 'mongo_load', '', '1'::jsonb, 'Parallel worker threads per collection (1=sequential)'),
    ('full_load_parallel_tables', 'mongo_load', '', '1'::jsonb, 'Max collections loaded in parallel per conn'),
    ('full_load_source_sleep_ms', 'mongo_load', '', '0'::jsonb, 'Sleep ms between source read batches'),
    ('lake_partition_months_ahead', 'mongo_load', '', '3'::jsonb, 'Monthly partitions to create ahead of current month'),
    ('ddl_sync_indexes', 'mongo_load', '', 'false'::jsonb, 'Mongo lake: no secondary indexes in CDC path'),
    ('ddl_sync_foreign_keys', 'mongo_load', '', 'false'::jsonb, 'Mongo lake: no FK constraints'),
    ('ddl_sync_columns', 'mongo_load', '', 'true'::jsonb, 'ADD COLUMN for new flattened fields vs lake'),
    ('ddl_sync_sample_size', 'mongo_load', '', '1000'::jsonb, 'Documents sampled for pre-apply / full-load DDL inference')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
