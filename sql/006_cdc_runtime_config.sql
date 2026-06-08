INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('cdc_max_seconds', 'mariadb_cdc', '', '300'::jsonb, 'Max seconds per cdc run'),
    ('cdc_max_events', 'mariadb_cdc', '', '50000'::jsonb, 'Max binlog row events per conn per run'),
    ('cdc_apply_batch_size', 'mariadb_cdc', '', '500'::jsonb, 'Rows buffered before lake upsert/delete'),
    ('cdc_server_id_base', 'mariadb_cdc', '', '9000'::jsonb, 'Base for synthetic replica server_id per conn')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
