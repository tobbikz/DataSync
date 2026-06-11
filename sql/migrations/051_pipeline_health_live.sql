-- Live dashboard columns + refresh function driven by apply_batch_stats.
ALTER TABLE cdc_catalog.pipeline_health
    ADD COLUMN IF NOT EXISTS last_batch_id text,
    ADD COLUMN IF NOT EXISTS active_batch_events_applied bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS recent_errors_1h integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS recent_warnings_1h integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_apply_proof jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN cdc_catalog.pipeline_health.last_batch_id IS
    'Latest apply_batch_stats batch_id driving live metrics';

COMMENT ON COLUMN cdc_catalog.pipeline_health.active_batch_events_applied IS
    'Sum of events_total in last_batch_id (in-progress slice activity)';

COMMENT ON COLUMN cdc_catalog.pipeline_health.last_apply_proof IS
    'Latest apply_batch_stats row as JSON (stat_id, table, events, kafka offset) — verify: SELECT * FROM apply_batch_stats WHERE stat_id = (last_apply_proof->>''stat_id'')::bigint';

CREATE OR REPLACE FUNCTION cdc_catalog.refresh_pipeline_health_live(
    p_conn_id text,
    p_service_tier cdc_catalog.service_tier,
    p_db_engine cdc_catalog.db_engine DEFAULT 'mariadb'
) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_batch_id text;
BEGIN
    IF p_conn_id IS NULL OR p_conn_id = '' OR p_conn_id = '__TOTAL__' THEN
        RETURN;
    END IF;

    INSERT INTO cdc_catalog.pipeline_health (conn_id, service_tier, db_engine)
    VALUES (p_conn_id, p_service_tier, p_db_engine)
    ON CONFLICT (conn_id, service_tier, db_engine) DO NOTHING;

    SELECT abs.batch_id INTO v_batch_id
    FROM cdc_catalog.apply_batch_stats abs
    WHERE abs.conn_id = p_conn_id
      AND abs.service_tier = p_service_tier
      AND abs.logged_at > now() - interval '2 hours'
    ORDER BY abs.logged_at DESC
    LIMIT 1;

    UPDATE cdc_catalog.pipeline_health ph
    SET
        capture_lag_seconds = coalesce(cp.capture_lag_seconds, 0),
        capture_status = coalesce(cp.status, 'healthy'::cdc_catalog.cdc_health_status),
        binlog_file = cp.binlog_file,
        binlog_position = cp.binlog_position,
        last_capture_at = cp.last_event_ts,
        total_tables = cat.total_tables,
        cdc_ready = cat.cdc_ready,
        pending_full_load = cat.pending_full_load,
        failed_tables = cat.failed_tables,
        success_tables = cat.success_tables,
        apply_healthy = cat.apply_healthy,
        apply_lagging = cat.apply_lagging,
        apply_quarantined = cat.apply_quarantined,
        max_apply_lag_seconds = cat.max_apply_lag_seconds,
        kafka_lag_total = coalesce(lag.kafka_lag_total, 0),
        kafka_partitions_with_lag = coalesce(lag.kafka_partitions_with_lag, 0),
        max_kafka_lag = coalesce(lag.max_kafka_lag, 0),
        max_kafka_lag_schema = lag.max_kafka_lag_schema,
        max_kafka_lag_table = lag.max_kafka_lag_table,
        last_apply_at = coalesce(batch.last_apply_at, ph.last_apply_at),
        last_batch_id = v_batch_id,
        active_batch_events_applied = coalesce(batch.active_batch_events_applied, 0),
        recent_errors_1h = coalesce(logs.recent_errors_1h, 0),
        recent_warnings_1h = coalesce(logs.recent_warnings_1h, 0),
        last_apply_proof = coalesce(proof.last_apply_proof, '{}'::jsonb),
        updated_at = now(),
        updated_by = 'live'
    FROM (
        SELECT
            count(*)::integer AS total_tables,
            count(*) FILTER (
                WHERE c.active AND c.cdc_enabled AND NOT c.needs_full_load
            )::integer AS cdc_ready,
            count(*) FILTER (WHERE c.needs_full_load AND c.active)::integer AS pending_full_load,
            count(*) FILTER (WHERE c.status = 'failed'::cdc_catalog.replication_status)::integer AS failed_tables,
            count(*) FILTER (WHERE c.status = 'success'::cdc_catalog.replication_status)::integer AS success_tables,
            count(*) FILTER (
                WHERE ap.status = 'healthy'::cdc_catalog.cdc_health_status
            )::integer AS apply_healthy,
            count(*) FILTER (
                WHERE ap.status = ANY (
                    ARRAY['stale'::cdc_catalog.cdc_health_status, 'lagging'::cdc_catalog.cdc_health_status]
                )
            )::integer AS apply_lagging,
            count(*) FILTER (
                WHERE ap.status = 'quarantined'::cdc_catalog.cdc_health_status
            )::integer AS apply_quarantined,
            coalesce(max(ap.apply_lag_seconds), 0)::integer AS max_apply_lag_seconds
        FROM cdc_catalog.catalog c
        LEFT JOIN cdc_catalog.apply_position ap ON ap.catalog_id = c.catalog_id
        WHERE c.conn_id = p_conn_id
          AND c.service_tier = p_service_tier
          AND c.db_engine = p_db_engine
          AND c.active = true
    ) cat
    LEFT JOIN cdc_catalog.capture_position cp ON cp.conn_id = p_conn_id
    LEFT JOIN LATERAL (
        WITH latest_table_stats AS (
            SELECT DISTINCT ON (abs.source_schema, abs.source_table)
                abs.source_schema,
                abs.source_table,
                abs.kafka_topic,
                abs.kafka_partition,
                abs.kafka_consumer_lag
            FROM cdc_catalog.apply_batch_stats abs
            WHERE abs.conn_id = p_conn_id
              AND abs.service_tier = p_service_tier
              AND abs.logged_at > now() - interval '30 minutes'
              AND abs.kafka_topic IS NOT NULL
              AND abs.kafka_topic <> ''
            ORDER BY abs.source_schema, abs.source_table, abs.logged_at DESC
        ),
        lag_by_partition AS (
            SELECT kafka_topic, kafka_partition, max(kafka_consumer_lag) AS lag
            FROM latest_table_stats
            GROUP BY kafka_topic, kafka_partition
        ),
        hot AS (
            SELECT source_schema, source_table, kafka_consumer_lag
            FROM latest_table_stats
            ORDER BY kafka_consumer_lag DESC NULLS LAST
            LIMIT 1
        )
        SELECT
            (SELECT coalesce(sum(lag), 0) FROM lag_by_partition) AS kafka_lag_total,
            (SELECT count(*)::integer FROM lag_by_partition WHERE lag > 0) AS kafka_partitions_with_lag,
            (SELECT coalesce(max(lag), 0) FROM lag_by_partition) AS max_kafka_lag,
            (SELECT source_schema FROM hot) AS max_kafka_lag_schema,
            (SELECT source_table FROM hot) AS max_kafka_lag_table
    ) lag ON true
    LEFT JOIN LATERAL (
        SELECT
            max(abs.logged_at) AS last_apply_at,
            coalesce(sum(abs.events_total), 0)::bigint AS active_batch_events_applied
        FROM cdc_catalog.apply_batch_stats abs
        WHERE abs.conn_id = p_conn_id
          AND abs.service_tier = p_service_tier
          AND v_batch_id IS NOT NULL
          AND abs.batch_id = v_batch_id
    ) batch ON true
    LEFT JOIN LATERAL (
        SELECT
            count(*) FILTER (WHERE l.level = 'error'::cdc_catalog.log_level)::integer AS recent_errors_1h,
            count(*) FILTER (WHERE l.level = 'warning'::cdc_catalog.log_level)::integer AS recent_warnings_1h
        FROM cdc_catalog.logs l
        WHERE l.conn_id = p_conn_id
          AND l.created_at > now() - interval '1 hour'
          AND l.level IN ('error'::cdc_catalog.log_level, 'warning'::cdc_catalog.log_level)
    ) logs ON true
    LEFT JOIN LATERAL (
        SELECT jsonb_build_object(
            'stat_id', abs.stat_id,
            'batch_id', abs.batch_id,
            'source_schema', abs.source_schema,
            'source_table', abs.source_table,
            'events_inserts', abs.events_inserts,
            'events_updates', abs.events_updates,
            'events_deletes', abs.events_deletes,
            'events_total', abs.events_total,
            'kafka_topic', abs.kafka_topic,
            'kafka_partition', abs.kafka_partition,
            'kafka_offset', abs.kafka_offset,
            'kafka_consumer_lag', abs.kafka_consumer_lag,
            'logged_at', abs.logged_at
        ) AS last_apply_proof
        FROM cdc_catalog.apply_batch_stats abs
        WHERE abs.conn_id = p_conn_id
          AND abs.service_tier = p_service_tier
        ORDER BY abs.logged_at DESC, abs.stat_id DESC
        LIMIT 1
    ) proof ON true
    WHERE ph.conn_id = p_conn_id
      AND ph.service_tier = p_service_tier
      AND ph.db_engine = p_db_engine;
END;
$$;

COMMENT ON FUNCTION cdc_catalog.refresh_pipeline_health_live IS
    'Aggregate apply_batch_stats + catalog + capture + logs into pipeline_health (live dashboard).';

CREATE OR REPLACE FUNCTION cdc_catalog.refresh_pipeline_health_totals(
    p_service_tier cdc_catalog.service_tier,
    p_db_engine cdc_catalog.db_engine DEFAULT 'mariadb'
) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO cdc_catalog.pipeline_health (
        conn_id, service_tier, db_engine,
        capture_lag_seconds, capture_status,
        kafka_lag_total, kafka_partitions_with_lag, max_kafka_lag,
        max_kafka_lag_schema, max_kafka_lag_table,
        total_tables, cdc_ready, pending_full_load, failed_tables, success_tables,
        apply_healthy, apply_lagging, apply_quarantined, max_apply_lag_seconds,
        last_capture_at, last_apply_at,
        last_batch_id, active_batch_events_applied,
        recent_errors_1h, recent_warnings_1h, last_apply_proof,
        last_slice_events_seen, last_slice_events_applied,
        last_slice_errors, last_slice_stop_reason, last_slice_duration_ms,
        updated_at, updated_by
    )
    SELECT
        '__TOTAL__',
        p_service_tier,
        p_db_engine,
        coalesce(max(ph.capture_lag_seconds), 0),
        CASE
            WHEN count(*) FILTER (WHERE ph.capture_status <> 'healthy'::cdc_catalog.cdc_health_status) > 0
                THEN 'lagging'::cdc_catalog.cdc_health_status
            ELSE 'healthy'::cdc_catalog.cdc_health_status
        END,
        coalesce(sum(ph.kafka_lag_total), 0),
        coalesce(sum(ph.kafka_partitions_with_lag), 0),
        coalesce(max(ph.max_kafka_lag), 0),
        (
            SELECT ph2.max_kafka_lag_schema
            FROM cdc_catalog.pipeline_health ph2
            WHERE ph2.service_tier = p_service_tier
              AND ph2.db_engine = p_db_engine
              AND ph2.conn_id <> '__TOTAL__'
            ORDER BY ph2.max_kafka_lag DESC NULLS LAST, ph2.conn_id
            LIMIT 1
        ),
        (
            SELECT ph2.max_kafka_lag_table
            FROM cdc_catalog.pipeline_health ph2
            WHERE ph2.service_tier = p_service_tier
              AND ph2.db_engine = p_db_engine
              AND ph2.conn_id <> '__TOTAL__'
            ORDER BY ph2.max_kafka_lag DESC NULLS LAST, ph2.conn_id
            LIMIT 1
        ),
        coalesce(sum(ph.total_tables), 0),
        coalesce(sum(ph.cdc_ready), 0),
        coalesce(sum(ph.pending_full_load), 0),
        coalesce(sum(ph.failed_tables), 0),
        coalesce(sum(ph.success_tables), 0),
        coalesce(sum(ph.apply_healthy), 0),
        coalesce(sum(ph.apply_lagging), 0),
        coalesce(sum(ph.apply_quarantined), 0),
        coalesce(max(ph.max_apply_lag_seconds), 0),
        max(ph.last_capture_at),
        max(ph.last_apply_at),
        NULL,
        coalesce(sum(ph.active_batch_events_applied), 0),
        coalesce(sum(ph.recent_errors_1h), 0),
        coalesce(sum(ph.recent_warnings_1h), 0),
        (
            SELECT ph2.last_apply_proof
            FROM cdc_catalog.pipeline_health ph2
            WHERE ph2.service_tier = p_service_tier
              AND ph2.db_engine = p_db_engine
              AND ph2.conn_id <> '__TOTAL__'
              AND ph2.last_apply_proof <> '{}'::jsonb
            ORDER BY (ph2.last_apply_proof->>'logged_at')::timestamptz DESC NULLS LAST
            LIMIT 1
        ),
        coalesce(sum(ph.last_slice_events_seen), 0),
        coalesce(sum(ph.last_slice_events_applied), 0),
        coalesce(sum(ph.last_slice_errors), 0),
        'aggregate',
        coalesce(sum(ph.last_slice_duration_ms), 0),
        now(),
        'live'
    FROM cdc_catalog.pipeline_health ph
    WHERE ph.service_tier = p_service_tier
      AND ph.db_engine = p_db_engine
      AND ph.conn_id <> '__TOTAL__'
    ON CONFLICT (conn_id, service_tier, db_engine) DO UPDATE SET
        capture_lag_seconds = EXCLUDED.capture_lag_seconds,
        capture_status = EXCLUDED.capture_status,
        kafka_lag_total = EXCLUDED.kafka_lag_total,
        kafka_partitions_with_lag = EXCLUDED.kafka_partitions_with_lag,
        max_kafka_lag = EXCLUDED.max_kafka_lag,
        max_kafka_lag_schema = EXCLUDED.max_kafka_lag_schema,
        max_kafka_lag_table = EXCLUDED.max_kafka_lag_table,
        total_tables = EXCLUDED.total_tables,
        cdc_ready = EXCLUDED.cdc_ready,
        pending_full_load = EXCLUDED.pending_full_load,
        failed_tables = EXCLUDED.failed_tables,
        success_tables = EXCLUDED.success_tables,
        apply_healthy = EXCLUDED.apply_healthy,
        apply_lagging = EXCLUDED.apply_lagging,
        apply_quarantined = EXCLUDED.apply_quarantined,
        max_apply_lag_seconds = EXCLUDED.max_apply_lag_seconds,
        last_capture_at = EXCLUDED.last_capture_at,
        last_apply_at = EXCLUDED.last_apply_at,
        active_batch_events_applied = EXCLUDED.active_batch_events_applied,
        recent_errors_1h = EXCLUDED.recent_errors_1h,
        recent_warnings_1h = EXCLUDED.recent_warnings_1h,
        last_apply_proof = EXCLUDED.last_apply_proof,
        last_slice_events_seen = EXCLUDED.last_slice_events_seen,
        last_slice_events_applied = EXCLUDED.last_slice_events_applied,
        last_slice_errors = EXCLUDED.last_slice_errors,
        last_slice_stop_reason = EXCLUDED.last_slice_stop_reason,
        last_slice_duration_ms = EXCLUDED.last_slice_duration_ms,
        updated_at = EXCLUDED.updated_at,
        updated_by = EXCLUDED.updated_by;
END;
$$;

DROP VIEW IF EXISTS cdc_catalog.v_pipeline_health;

CREATE VIEW cdc_catalog.v_pipeline_health AS
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
    ph.last_batch_id,
    ph.active_batch_events_applied,
    ph.recent_errors_1h,
    ph.recent_warnings_1h,
    ph.last_apply_proof,
    ph.last_slice_events_seen,
    ph.last_slice_events_applied,
    ph.last_slice_errors,
    ph.last_slice_stop_reason,
    ph.last_slice_duration_ms,
    ph.updated_at,
    ph.updated_by,
    CASE
        WHEN ph.conn_id = '__TOTAL__' THEN NULL
        WHEN ph.recent_errors_1h > 0 OR ph.capture_lag_seconds > 300 OR ph.max_kafka_lag > 50000 THEN 'RED'
        WHEN ph.recent_warnings_1h > 0 OR ph.capture_lag_seconds > 60 OR ph.max_kafka_lag > 10000
            OR ph.apply_lagging > 0 THEN 'AMBER'
        ELSE 'GREEN'
    END AS health_rag
FROM cdc_catalog.pipeline_health ph
ORDER BY
    CASE WHEN ph.conn_id = '__TOTAL__' THEN 0 ELSE 1 END,
    ph.service_tier,
    ph.conn_id;
