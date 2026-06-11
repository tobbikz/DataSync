-- Include brief row sample from apply_batch_stats.context in last_apply_proof.

COMMENT ON COLUMN cdc_catalog.pipeline_health.last_apply_proof IS
    'Latest apply_batch_stats row as JSON (stat_id, table, events, kafka offset, inserted_sample) — verify: SELECT * FROM apply_batch_stats WHERE stat_id = (last_apply_proof->>''stat_id'')::bigint';

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
            'logged_at', abs.logged_at,
            'lake_target', coalesce(abs.context->'lake_target', '{}'::jsonb),
            'inserted_sample', coalesce(abs.context->'sample', '[]'::jsonb)
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
