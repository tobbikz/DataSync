-- MSSQL ↔ MariaDB runtime parity: slice defaults, DDL, apply/capture tuning per conn_id

-- Full-load DDL parity (indexes + FKs on lake, same as mariadb_load)
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('ddl_sync_indexes', 'mssql_load', '', 'true'::jsonb, 'Sync secondary indexes after truncate (parity mariadb_load)'),
    ('ddl_sync_foreign_keys', 'mssql_load', '', 'true'::jsonb, 'Sync FK constraints after truncate (parity mariadb_load)')
ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();

-- Capture slice defaults aligned with cdc_kafka_capture (config.json slice_max_seconds=60 overrides in test)
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('capture_max_seconds', 'cdc_kafka_mssql_capture', '', '300'::jsonb, 'MSSQL capture slice max seconds (parity cdc_kafka_capture global)'),
    ('capture_max_events', 'cdc_kafka_mssql_capture', '', '10000000'::jsonb, 'MSSQL capture max events per slice'),
    ('capture_idle_poll_seconds', 'cdc_kafka_mssql_capture', '', '3'::jsonb, 'Idle poll between LSN windows (parity cdc_kafka_capture)'),
    ('capture_heartbeat_seconds', 'cdc_kafka_mssql_capture', '', '60'::jsonb, 'Bump heartbeat when capture idle'),
    ('kafka_topic_mode', 'cdc_kafka_mssql_capture', '', '"bucketed"'::jsonb, 'Topic routing mode (parity cdc_kafka_capture)'),
    ('kafka_topic_buckets', 'cdc_kafka_mssql_capture', '', '64'::jsonb, 'Bucket count when mode=bucketed'),
    ('mssql_capture_replay_on_idle', 'cdc_kafka_mssql_capture', '', 'false'::jsonb,
     'When true: if LSN idle at max, rewind to min_lsn once per recovery (no truncate)')
ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();

-- MariaDB hot-path apply tuning (mirror MSSQL_LOCAL from 024)
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_empty_poll_quiet_threshold', 'cdc_kafka_apply', 'MARIADB_LOCAL', '10'::jsonb,
     'MariaDB catch-up: empty polls before quiet slice exit'),
    ('apply_poll_timeout_ms', 'cdc_kafka_apply', 'MARIADB_LOCAL', '100'::jsonb,
     'MariaDB: librdkafka poll timeout ms'),
    ('apply_max_partition_fetch_bytes', 'cdc_kafka_apply', 'MARIADB_LOCAL', '20971520'::jsonb,
     'MariaDB: max bytes per Kafka partition per fetch (20MB)'),
    ('apply_fetch_max_bytes', 'cdc_kafka_apply', 'MARIADB_LOCAL', '104857600'::jsonb,
     'MariaDB: max total fetch bytes per poll (100MB)'),
    ('apply_batch_size', 'cdc_kafka_apply', 'MARIADB_LOCAL', '50000'::jsonb,
     'MariaDB: PG commit batch size'),
    ('apply_max_seconds', 'cdc_kafka_apply', 'MARIADB_LOCAL', '60'::jsonb,
     'MariaDB catch-up slice max seconds (parity MSSQL_LOCAL; config.json slice overrides)'),
    ('apply_queued_min_messages', 'cdc_kafka_apply', 'MARIADB_LOCAL', '100000'::jsonb,
     'MariaDB: librdkafka queued.min.messages'),
    ('apply_fetch_wait_max_ms', 'cdc_kafka_apply', 'MARIADB_LOCAL', '500'::jsonb,
     'MariaDB: librdkafka fetch.wait.max.ms'),
    ('capture_producer_linger_ms', 'cdc_kafka_capture', 'MARIADB_LOCAL', '5'::jsonb,
     'MariaDB capture producer linger ms'),
    ('capture_producer_batch_size', 'cdc_kafka_capture', 'MARIADB_LOCAL', '10000'::jsonb,
     'MariaDB capture producer batch.num.messages'),
    ('apply_worker_count', 'cdc_kafka_apply', 'MARIADB_LOCAL', '1'::jsonb,
     'MariaDB apply workers per tier override')
ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();

-- MSSQL conn capture keys at conn scope (mirror MariaDB MARIADB_LOCAL)
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('capture_max_seconds', 'cdc_kafka_mssql_capture', 'MSSQL_LOCAL', '60'::jsonb,
     'MSSQL test slice seconds when config.json slice not set'),
    ('capture_producer_linger_ms', 'cdc_kafka_mssql_capture', 'MSSQL_LOCAL', '5'::jsonb,
     'MSSQL capture producer linger ms'),
    ('capture_producer_batch_size', 'cdc_kafka_mssql_capture', 'MSSQL_LOCAL', '10000'::jsonb,
     'MSSQL capture producer batch.num.messages')
ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    updated_at = now();
