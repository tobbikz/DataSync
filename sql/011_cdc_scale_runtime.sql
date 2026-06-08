-- Scale-ready runtime defaults (10k+ tables, high row volume)

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('kafka_topic_mode', 'cdc_kafka_apply', '', '"bucketed"'::jsonb, 'per_table | bucketed (default bucketed for 10k+ tables)'),
    ('kafka_topic_buckets', 'cdc_kafka_apply', '', '64'::jsonb, 'Number of Kafka topics when mode=bucketed'),
    ('kafka_topic_mode', 'cdc_kafka_capture', '', '"bucketed"'::jsonb, 'Must match apply topic routing'),
    ('kafka_topic_buckets', 'cdc_kafka_capture', '', '64'::jsonb, 'Number of Kafka topics when mode=bucketed'),
    ('applied_events_retention_days', 'cdc_kafka_apply', '', '7'::jsonb, 'Prune cdc_applied_events older than N days each apply slice'),
    ('capture_producer_linger_ms', 'cdc_kafka_capture', '', '5'::jsonb, 'Kafka producer linger.ms for batching publishes'),
    ('capture_producer_batch_size', 'cdc_kafka_capture', '', '10000'::jsonb, 'Kafka producer batch.num.messages hint')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;

CREATE INDEX IF NOT EXISTS cdc_applied_events_applied_at_idx
    ON cdc_catalog.cdc_applied_events (applied_at);

CREATE OR REPLACE FUNCTION cdc_catalog.prune_applied_events(p_retention_days integer DEFAULT 7)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    deleted bigint;
BEGIN
    DELETE FROM cdc_catalog.cdc_applied_events
    WHERE applied_at < now() - make_interval(days => p_retention_days);
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END;
$$;

COMMENT ON FUNCTION cdc_catalog.prune_applied_events IS
    'Delete idempotency audit rows older than retention window (safe with offset-based replay guard).';
