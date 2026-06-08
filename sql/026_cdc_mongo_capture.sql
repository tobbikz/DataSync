-- MongoDB change stream resume (per collection), used by cdc_kafka Mongo capture
CREATE TABLE IF NOT EXISTS cdc_catalog.cdc_mongo_resume (
    conn_id         TEXT NOT NULL,
    database        TEXT NOT NULL,
    collection      TEXT NOT NULL,
    resume_token    JSONB,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (conn_id, database, collection)
);

CREATE INDEX IF NOT EXISTS cdc_mongo_resume_updated_idx
    ON cdc_catalog.cdc_mongo_resume (conn_id, updated_at DESC);

COMMENT ON TABLE cdc_catalog.cdc_mongo_resume IS
    'Per-collection MongoDB change stream resume token for cdc_kafka capture';

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('capture_max_seconds', 'cdc_kafka_mongo_capture', '', '60'::jsonb, 'MongoDB capture slice max seconds'),
    ('capture_max_events', 'cdc_kafka_mongo_capture', '', '50000'::jsonb, 'MongoDB capture max events per slice'),
    ('capture_topic_prefix', 'cdc_kafka_mongo_capture', 'MONGO_LOCAL', '"MONGO_LOCAL"'::jsonb, 'Kafka topic prefix for MongoDB capture'),
    ('full_load_batch_size', 'mongo_load', '', '5000'::jsonb, 'MongoDB bootstrap COPY batch size'),
    ('lake_partition_months_ahead', 'mongo_load', '', '3'::jsonb, 'Monthly partitions ahead for Mongo lake tables')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;
