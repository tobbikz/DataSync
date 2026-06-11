-- Current-state CDC pipeline health per conn/tier (refreshed by capture/apply/daemon).
CREATE TABLE IF NOT EXISTS cdc_catalog.pipeline_health (
    conn_id text NOT NULL,
    service_tier cdc_catalog.service_tier NOT NULL,
    db_engine cdc_catalog.db_engine NOT NULL DEFAULT 'mariadb',

    capture_lag_seconds integer NOT NULL DEFAULT 0,
    capture_status cdc_catalog.cdc_health_status NOT NULL DEFAULT 'healthy',
    binlog_file text,
    binlog_position bigint,
    last_capture_at timestamp with time zone,

    kafka_lag_total bigint NOT NULL DEFAULT 0,
    kafka_partitions_with_lag integer NOT NULL DEFAULT 0,
    max_kafka_lag bigint NOT NULL DEFAULT 0,
    max_kafka_lag_schema text,
    max_kafka_lag_table text,

    total_tables integer NOT NULL DEFAULT 0,
    cdc_ready integer NOT NULL DEFAULT 0,
    pending_full_load integer NOT NULL DEFAULT 0,
    failed_tables integer NOT NULL DEFAULT 0,
    success_tables integer NOT NULL DEFAULT 0,
    apply_healthy integer NOT NULL DEFAULT 0,
    apply_lagging integer NOT NULL DEFAULT 0,
    apply_quarantined integer NOT NULL DEFAULT 0,
    max_apply_lag_seconds integer NOT NULL DEFAULT 0,

    last_apply_at timestamp with time zone,
    last_slice_events_seen bigint,
    last_slice_events_applied bigint,
    last_slice_errors integer NOT NULL DEFAULT 0,
    last_slice_stop_reason text,
    last_slice_duration_ms bigint,

    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_by text NOT NULL DEFAULT '',

    CONSTRAINT pipeline_health_pkey PRIMARY KEY (conn_id, service_tier, db_engine)
);

COMMENT ON TABLE cdc_catalog.pipeline_health IS
    'Live CDC dashboard row per conn+tier (conn_id=__TOTAL__ for tier aggregate). Refreshed by capture/apply; no Docker needed.';

COMMENT ON COLUMN cdc_catalog.pipeline_health.conn_id IS
    'Source connection id, or __TOTAL__ for tier-wide aggregate.';

COMMENT ON COLUMN cdc_catalog.pipeline_health.kafka_lag_total IS
    'Sum of Kafka consumer lag across distinct topic partitions for this conn+tier.';

CREATE INDEX IF NOT EXISTS pipeline_health_tier_idx
    ON cdc_catalog.pipeline_health (service_tier, db_engine, updated_at DESC);

CREATE OR REPLACE VIEW cdc_catalog.v_pipeline_health AS
SELECT
    ph.conn_id,
    ph.service_tier::text AS service_tier,
    ph.db_engine::text AS db_engine,
    ph.capture_lag_seconds,
    ph.capture_status::text AS capture_status,
    ph.binlog_file,
    ph.binlog_position,
    ph.last_capture_at,
    ph.kafka_lag_total,
    ph.kafka_partitions_with_lag,
    ph.max_kafka_lag,
    ph.max_kafka_lag_schema,
    ph.max_kafka_lag_table,
    ph.total_tables,
    ph.cdc_ready,
    ph.pending_full_load,
    ph.failed_tables,
    ph.success_tables,
    ph.apply_healthy,
    ph.apply_lagging,
    ph.apply_quarantined,
    ph.max_apply_lag_seconds,
    ph.last_apply_at,
    ph.last_slice_events_seen,
    ph.last_slice_events_applied,
    ph.last_slice_errors,
    ph.last_slice_stop_reason,
    ph.last_slice_duration_ms,
    ph.updated_at,
    ph.updated_by,
    CASE
        WHEN ph.conn_id = '__TOTAL__' THEN NULL
        WHEN ph.capture_lag_seconds > 300 OR ph.max_kafka_lag > 50000 THEN 'RED'
        WHEN ph.capture_lag_seconds > 60 OR ph.max_kafka_lag > 10000 OR ph.apply_lagging > 0 THEN 'AMBER'
        ELSE 'GREEN'
    END AS health_rag
FROM cdc_catalog.pipeline_health ph
ORDER BY
    CASE WHEN ph.conn_id = '__TOTAL__' THEN 0 ELSE 1 END,
    ph.service_tier,
    ph.conn_id;

COMMENT ON VIEW cdc_catalog.v_pipeline_health IS
    'Ops dashboard: lag + catalog counts + simple RAG. SELECT * FROM cdc_catalog.v_pipeline_health WHERE service_tier = ''bronze'';';
