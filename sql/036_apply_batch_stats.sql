-- Per-table apply throughput metadata (batch_id-scoped); remove I/U/D from apply_position.

CREATE TABLE IF NOT EXISTS cdc_catalog.apply_batch_stats (
    stat_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id            TEXT NOT NULL,
    conn_id             TEXT NOT NULL,
    catalog_id          BIGINT,
    source_schema       TEXT NOT NULL,
    source_table        TEXT NOT NULL,
    service_tier        TEXT,
    events_inserts      BIGINT NOT NULL DEFAULT 0,
    events_updates      BIGINT NOT NULL DEFAULT 0,
    events_deletes      BIGINT NOT NULL DEFAULT 0,
    events_total        BIGINT NOT NULL DEFAULT 0,
    duration_ms         BIGINT NOT NULL DEFAULT 0,
    events_per_minute   BIGINT NOT NULL DEFAULT 0,
    kafka_topic         TEXT,
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,
    context             JSONB NOT NULL DEFAULT '{}',
    logged_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS apply_batch_stats_batch_idx
    ON cdc_catalog.apply_batch_stats (batch_id, conn_id);

CREATE INDEX IF NOT EXISTS apply_batch_stats_table_idx
    ON cdc_catalog.apply_batch_stats (conn_id, source_schema, source_table, logged_at DESC);

CREATE INDEX IF NOT EXISTS apply_batch_stats_logged_at_idx
    ON cdc_catalog.apply_batch_stats (logged_at);

COMMENT ON TABLE cdc_catalog.apply_batch_stats IS
    'Per-table apply slice stats: I/U/D counts, duration, events/min — keyed by batch_id';

ALTER TABLE cdc_catalog.apply_position
    DROP COLUMN IF EXISTS events_inserts,
    DROP COLUMN IF EXISTS events_updates,
    DROP COLUMN IF EXISTS events_deletes,
    DROP COLUMN IF EXISTS events_total,
    DROP COLUMN IF EXISTS events_applied;

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description) VALUES
    ('apply_batch_stats_retention_days', 'cdc_kafka_apply', '', '30'::jsonb, 'Prune apply_batch_stats older than N days (0=keep all)')
ON CONFLICT (config_key, component, conn_id) DO NOTHING;

CREATE OR REPLACE FUNCTION cdc_catalog.prune_apply_batch_stats(p_retention_days integer DEFAULT 30)
RETURNS bigint
LANGUAGE sql
AS $$
    WITH deleted AS (
        DELETE FROM cdc_catalog.apply_batch_stats
        WHERE p_retention_days > 0
          AND logged_at < now() - make_interval(days => p_retention_days)
        RETURNING 1
    )
    SELECT count(*)::bigint FROM deleted;
$$;
