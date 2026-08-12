#pragma once

#include <string_view>

namespace prod_ops_embedded {
    inline std::string_view datalake_lake() {
        return R"PO_datalake_lak(
-- DataLake helpers — apply to config.json → datalake.database (idempotent via install).
-- Partition DDL for raw/staging tables lives here; control plane stays in cdc_catalog (datasync DB).

CREATE SCHEMA IF NOT EXISTS lake;

CREATE OR REPLACE FUNCTION lake.month_bounds(p_month date)
    RETURNS TABLE(start_date date, end_date date)
    LANGUAGE sql
    IMMUTABLE
    AS $$
    SELECT date_trunc('month', p_month)::date,
           (date_trunc('month', p_month) + interval '1 month')::date;
$$;

CREATE OR REPLACE FUNCTION lake.ensure_monthly_partitions(
    p_schema text,
    p_table text,
    p_months_ahead integer DEFAULT 3
) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    m date;
    end_m date;
    part_name text;
    start_d date;
    end_d date;
    created integer := 0;
BEGIN
    m := date_trunc('month', CURRENT_DATE)::date;
    end_m := (date_trunc('month', CURRENT_DATE) + make_interval(months => p_months_ahead + 1))::date;

    WHILE m < end_m LOOP
        SELECT lb.start_date, lb.end_date INTO start_d, end_d FROM lake.month_bounds(m) lb;
        part_name := format('%s_%s', p_table, to_char(start_d, 'YYYY_MM'));

        -- Date literals (no forced +00) match existing TIMESTAMP/TIMESTAMPTZ partition bounds
        -- and avoid false "would overlap" when older partitions used session-local midnight.
        IF to_regclass(format('%I.%I', p_schema, part_name)) IS NULL THEN
            BEGIN
                EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
                    p_schema, part_name, p_schema, p_table,
                    start_d::text,
                    end_d::text
                );
                created := created + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    -- Race or legacy overlapping bounds: skip month; do not fail apply.
                    IF SQLERRM ILIKE '%would overlap%'
                       OR SQLERRM ILIKE '%already exists%' THEN
                        NULL;
                    ELSE
                        RAISE;
                    END IF;
            END;
        END IF;
        m := (m + interval '1 month')::date;
    END LOOP;
    RETURN created;
END;
$$;

COMMENT ON FUNCTION lake.ensure_monthly_partitions(text, text, integer) IS
    'Create monthly RANGE partitions on _dl_load_timestamp; skips overlap/race instead of failing apply.';
)PO_datalake_lak";
    }
    inline std::string_view datasync_baseline() {
        return R"PO_datasync_bas(
--
-- CDC catalog schema-only backup (no data)
-- Source: local DataLake @ 2025-06-06
-- Regenerate: pg_dump --schema-only --schema=cdc_catalog ...
--
-- PostgreSQL database dump
--


-- Dumped from database version 18.3
-- Dumped by pg_dump version 18.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: cdc_catalog; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA cdc_catalog;


--
-- Name: cdc_health_status; Type: TYPE; Schema: cdc_catalog; Owner: -
--

CREATE TYPE cdc_catalog.cdc_health_status AS ENUM (
    'healthy',
    'lagging',
    'stale',
    'gap_detected',
    'rebootstrap_pending',
    'snapshot_running',
    'quarantined',
    'failed'
);


--
-- Name: db_engine; Type: TYPE; Schema: cdc_catalog; Owner: -
--

CREATE TYPE cdc_catalog.db_engine AS ENUM (
    'mariadb',
    'mssql',
    'mongodb'
);


--
-- Name: log_level; Type: TYPE; Schema: cdc_catalog; Owner: -
--

CREATE TYPE cdc_catalog.log_level AS ENUM (
    'debug',
    'info',
    'warning',
    'error'
);


--
-- Name: replication_status; Type: TYPE; Schema: cdc_catalog; Owner: -
--

CREATE TYPE cdc_catalog.replication_status AS ENUM (
    'pending',
    'success',
    'failed',
    'skipped',
    'disabled',
    'full_load_in_progress',
    'cdc_in_progress'
);


--
-- Name: TYPE replication_status; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TYPE cdc_catalog.replication_status IS 'pending | full_load_in_progress | cdc_in_progress | success | failed | skipped | disabled';


--
-- Name: prune_applied_events(integer); Type: FUNCTION; Schema: cdc_catalog; Owner: -
--
-- Lake partition helpers (lake.month_bounds, lake.ensure_monthly_partitions) live in
-- sql/backup/datalake_lake_schema.sql → config.json datalake.database only.

CREATE FUNCTION cdc_catalog.prune_applied_events(p_retention_days integer DEFAULT 7) RETURNS bigint
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


--
-- Name: FUNCTION prune_applied_events(p_retention_days integer); Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON FUNCTION cdc_catalog.prune_applied_events(p_retention_days integer) IS 'Delete idempotency audit rows older than retention window (safe with offset-based replay guard).';


--
-- Name: prune_apply_batch_stats(integer); Type: FUNCTION; Schema: cdc_catalog; Owner: -
--

CREATE FUNCTION cdc_catalog.prune_apply_batch_stats(p_retention_days integer DEFAULT 30) RETURNS bigint
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


--
-- Name: purge_logs(integer); Type: FUNCTION; Schema: cdc_catalog; Owner: -
--

CREATE FUNCTION cdc_catalog.purge_logs_batched(
    p_retention_days integer DEFAULT 7,
    p_batch_size integer DEFAULT 5000,
    p_max_batches integer DEFAULT 500
) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
    total_deleted bigint := 0;
    batch_deleted bigint;
    batches_done integer := 0;
    cutoff timestamptz;
BEGIN
    IF p_retention_days IS NULL OR p_retention_days < 1
       OR p_batch_size IS NULL OR p_batch_size < 1
       OR p_max_batches IS NULL OR p_max_batches < 1 THEN
        RETURN 0;
    END IF;
    -- One purge at a time; concurrent callers (workers/CLI) skip cleanly.
    IF NOT pg_try_advisory_xact_lock(90420057001) THEN
        RETURN 0;
    END IF;
    cutoff := now() - make_interval(days => p_retention_days);
    LOOP
        WITH doomed AS (
            SELECT log_id
            FROM cdc_catalog.logs
            WHERE logged_at < cutoff
            ORDER BY log_id
            LIMIT p_batch_size
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM cdc_catalog.logs t
        USING doomed d
        WHERE t.log_id = d.log_id;
        GET DIAGNOSTICS batch_deleted = ROW_COUNT;
        total_deleted := total_deleted + batch_deleted;
        batches_done := batches_done + 1;
        EXIT WHEN batch_deleted = 0 OR batches_done >= p_max_batches;
    END LOOP;
    RETURN total_deleted;
END;
$$;

CREATE FUNCTION cdc_catalog.purge_logs(p_retention_days integer DEFAULT 7) RETURNS bigint
    LANGUAGE sql
    AS $$
    SELECT cdc_catalog.purge_logs_batched(p_retention_days, 5000, 500);
$$;


--
-- Name: FUNCTION purge_logs(p_retention_days integer); Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON FUNCTION cdc_catalog.purge_logs_batched(integer, integer, integer) IS
    'Batched delete of cdc_catalog.logs older than retention; advisory-locked; daemon maintenance window.';
COMMENT ON FUNCTION cdc_catalog.purge_logs(p_retention_days integer) IS
    'Compatibility wrapper: batched+locked purge of observability logs (append-only table).';

--
-- Name: touch_updated_at(); Type: FUNCTION; Schema: cdc_catalog; Owner: -
--

CREATE FUNCTION cdc_catalog.touch_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: apply_batch_stats; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.apply_batch_stats (
    stat_id bigint NOT NULL,
    batch_id text NOT NULL,
    conn_id text NOT NULL,
    catalog_id bigint,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    events_inserts bigint DEFAULT 0 NOT NULL,
    events_updates bigint DEFAULT 0 NOT NULL,
    events_deletes bigint DEFAULT 0 NOT NULL,
    events_total bigint DEFAULT 0 NOT NULL,
    duration_ms bigint DEFAULT 0 NOT NULL,
    events_per_minute bigint DEFAULT 0 NOT NULL,
    kafka_topic text,
    kafka_partition integer,
    kafka_offset bigint,
    context jsonb DEFAULT '{}'::jsonb NOT NULL,
    logged_at timestamp with time zone DEFAULT now() NOT NULL,
    is_stale boolean DEFAULT false NOT NULL,
    is_inactive boolean DEFAULT false NOT NULL,
    is_quarantined boolean DEFAULT false NOT NULL,
    apply_health_rag text DEFAULT 'UNKNOWN'::text NOT NULL,
    apply_lag_seconds integer DEFAULT 0 NOT NULL,
    apply_position_status text,
    events_seen_in_slice integer DEFAULT 0 NOT NULL,
    catalog_active boolean,
    cdc_enabled boolean,
    capture_lag_seconds integer DEFAULT 0 NOT NULL,
    kafka_consumer_lag bigint DEFAULT 0 NOT NULL,
    reconcile_row_delta bigint,
    dedup_skipped integer DEFAULT 0 NOT NULL,
    parse_skipped bigint DEFAULT 0 NOT NULL,
    dropped_unrecoverable bigint DEFAULT 0 NOT NULL,
    seconds_since_last_apply integer DEFAULT '-1'::integer NOT NULL,
    kafka_partition_lag bigint,
    lag_scan_complete boolean DEFAULT false NOT NULL,
    slice_kind text DEFAULT 'flush'::text NOT NULL,
    event_loss_status text DEFAULT 'ok'::text NOT NULL,
    health_reason text DEFAULT 'healthy'::text NOT NULL,
    semaphore text GENERATED ALWAYS AS (apply_health_rag) STORED,
    CONSTRAINT apply_batch_stats_apply_health_rag_chk CHECK ((apply_health_rag = ANY (ARRAY['GREEN'::text, 'AMBER'::text, 'RED'::text, 'UNKNOWN'::text])))
);


--
-- Name: TABLE apply_batch_stats; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TABLE cdc_catalog.apply_batch_stats IS 'Per-table apply slice stats: I/U/D counts, duration, events/min — keyed by batch_id';


--
-- Name: COLUMN apply_batch_stats.is_stale; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.is_stale IS 'apply_position lag exceeds apply_max_table_staleness_seconds or status stale/lagging/gap';


--
-- Name: COLUMN apply_batch_stats.is_inactive; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.is_inactive IS 'No CDC events seen in slice (quiet table)';


--
-- Name: COLUMN apply_batch_stats.reconciliation_rag; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.apply_health_rag IS 'Apply health RAG: GREEN/AMBER/RED/UNKNOWN from apply_position + slice metrics';


--
-- Name: COLUMN apply_batch_stats.capture_lag_seconds; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.capture_lag_seconds IS 'Conn-level capture lag from capture_position at slice time';


--
-- Name: COLUMN apply_batch_stats.kafka_consumer_lag; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.kafka_consumer_lag IS 'Exact table backlog in Kafka (messages matching this table ahead of apply offset; not partition watermark)';


--
-- Name: COLUMN apply_batch_stats.reconcile_row_delta; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.reconcile_row_delta IS 'source_row_count - lake_row_count from latest reconciliation_result';


--
-- Name: COLUMN apply_batch_stats.dedup_skipped; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.dedup_skipped IS 'Kafka events skipped because event_id already in cdc_applied_events';


--
-- Name: COLUMN apply_batch_stats.semaphore; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.semaphore IS 'GREEN/AMBER/RED mirror of apply_health_rag';


--
-- Name: apply_batch_stats_stat_id_seq; Type: SEQUENCE; Schema: cdc_catalog; Owner: -
--

ALTER TABLE cdc_catalog.apply_batch_stats ALTER COLUMN stat_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME cdc_catalog.apply_batch_stats_stat_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: apply_position; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.apply_position (
    catalog_id bigint NOT NULL,
    conn_id text NOT NULL,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    kafka_topic text DEFAULT ''::text NOT NULL,
    kafka_partition integer DEFAULT 0 NOT NULL,
    kafka_offset bigint DEFAULT '-1'::integer NOT NULL,
    last_applied_gtid text,
    last_applied_at timestamp with time zone,
    apply_lag_seconds integer DEFAULT 0 NOT NULL,
    status cdc_catalog.cdc_health_status DEFAULT 'healthy'::cdc_catalog.cdc_health_status NOT NULL,
    last_error text,
    quarantined_at timestamp with time zone,
    quarantine_reason text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE apply_position; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TABLE cdc_catalog.apply_position IS 'Per-table Kafka apply cursor; independent lag and quarantine state.';


--
-- Name: capture_position; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.capture_position (
    conn_id text NOT NULL,
    gtid_set text DEFAULT ''::text NOT NULL,
    binlog_file text,
    binlog_position bigint,
    kafka_connect_name text,
    last_event_ts timestamp with time zone,
    capture_lag_seconds integer DEFAULT 0 NOT NULL,
    server_uuid text,
    status cdc_catalog.cdc_health_status DEFAULT 'healthy'::cdc_catalog.cdc_health_status NOT NULL,
    last_error text,
    last_failed_source_schema text,
    last_failed_source_table text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE capture_position; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TABLE cdc_catalog.capture_position IS 'GTID-first capture cursor; source of truth for native binlog capture resume.';


--
-- Name: COLUMN capture_position.kafka_connect_name; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.capture_position.kafka_connect_name IS 'Deprecated; kept for compatibility. Use capture service batch id if needed.';


--
-- Name: catalog; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.catalog (
    catalog_id bigint NOT NULL,
    conn_id text NOT NULL,
    db_engine cdc_catalog.db_engine NOT NULL,
    source_database text DEFAULT ''::text NOT NULL,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    has_pk boolean DEFAULT false NOT NULL,
    pk_columns text,
    discovered_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    active boolean DEFAULT false NOT NULL,
    cdc_enabled boolean DEFAULT false NOT NULL,
    needs_full_load boolean DEFAULT true NOT NULL,
    capture_during_full_load boolean DEFAULT false NOT NULL,
    hot boolean DEFAULT false NOT NULL,
    status cdc_catalog.replication_status DEFAULT 'pending'::cdc_catalog.replication_status NOT NULL,
    last_full_load_at timestamp with time zone,
    last_cdc_at timestamp with time zone,
    last_error_at timestamp with time zone,
    last_error text,
    engine_meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT catalog_engine_meta_is_object CHECK ((jsonb_typeof(engine_meta) = 'object'::text)),
    CONSTRAINT catalog_pk_columns_when_has_pk CHECK (((NOT has_pk) OR ((pk_columns IS NOT NULL) AND (length(TRIM(BOTH FROM pk_columns)) > 0))))
);


--
-- Name: TABLE catalog; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TABLE cdc_catalog.catalog IS 'Replication registry: one row per source object per conn_id. Discover writes identity; operator sets flags; ETL writes runtime state.';


--
-- Name: catalog_catalog_id_seq; Type: SEQUENCE; Schema: cdc_catalog; Owner: -
--

ALTER TABLE cdc_catalog.catalog ALTER COLUMN catalog_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME cdc_catalog.catalog_catalog_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: cdc_applied_events; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.cdc_applied_events (
    event_id text NOT NULL,
    conn_id text NOT NULL,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    op character(1) NOT NULL,
    gtid text,
    kafka_topic text,
    kafka_partition integer,
    kafka_offset bigint,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE cdc_applied_events; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TABLE cdc_catalog.cdc_applied_events IS 'Dedup ledger for at-least-once Kafka apply.';


--
-- Name: cdc_mongo_resume; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.cdc_mongo_resume (
    conn_id text NOT NULL,
    database text NOT NULL,
    collection text NOT NULL,
    resume_token jsonb,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE cdc_mongo_resume; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TABLE cdc_catalog.cdc_mongo_resume IS 'Per-collection MongoDB change stream resume token for cdc_kafka capture';


--
-- Name: cdc_mssql_lsn; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.cdc_mssql_lsn (
    conn_id text NOT NULL,
    database text NOT NULL,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    last_start_lsn bytea NOT NULL,
    last_seqval bytea,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE cdc_mssql_lsn; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TABLE cdc_catalog.cdc_mssql_lsn IS 'Per-table SQL Server CDC LSN checkpoint for cdc_kafka MSSQL capture';


--
-- Name: connections; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.connections (
    alias text NOT NULL,
    db_engine cdc_catalog.db_engine NOT NULL,
    host text DEFAULT 'localhost'::text NOT NULL,
    port integer NOT NULL,
    db_name text DEFAULT ''::text NOT NULL,
    username text DEFAULT ''::text NOT NULL,
    password text DEFAULT ''::text NOT NULL,
    extras jsonb DEFAULT '{}'::jsonb NOT NULL,
    active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT connections_extras_is_object CHECK ((jsonb_typeof(extras) = 'object'::text)),
    CONSTRAINT connections_port_range CHECK (((port > 0) AND (port <= 65535)))
);


--
-- Name: TABLE connections; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TABLE cdc_catalog.connections IS 'Source connection registry (Airflow-style). alias is the operational conn_id used in catalog and runtime_config.';


--
-- Name: COLUMN connections.alias; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.connections.alias IS 'Unique connection alias; maps 1:1 to cdc_catalog.catalog.conn_id';


--
-- Name: COLUMN connections.db_name; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.connections.db_name IS 'Default database on source (MSSQL); empty for MariaDB/Mongo when not applicable';


--
-- Name: COLUMN connections.password; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.connections.password IS 'Source password; never log to observability.logs';


--
-- Name: COLUMN connections.extras; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON COLUMN cdc_catalog.connections.extras IS 'Engine-specific options, e.g. {"replica_set":"rs0"} for MongoDB';


--
-- Name: logs; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.logs (
    log_id bigint NOT NULL,
    logged_at timestamp with time zone DEFAULT now() NOT NULL,
    level cdc_catalog.log_level NOT NULL,
    component text NOT NULL,
    message text NOT NULL,
    context jsonb DEFAULT '{}'::jsonb NOT NULL,
    batch_id text,
    conn_id text,
    source_schema text,
    source_table text,
    CONSTRAINT logs_context_is_object CHECK ((jsonb_typeof(context) = 'object'::text))
);


--
-- Name: TABLE logs; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TABLE cdc_catalog.logs IS 'Append-only application log. All pipeline components MUST persist operational logs here.';


--
-- Name: logs_log_id_seq; Type: SEQUENCE; Schema: cdc_catalog; Owner: -
--

ALTER TABLE cdc_catalog.logs ALTER COLUMN log_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME cdc_catalog.logs_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: reconciliation_result; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.reconciliation_result (
    result_id bigint NOT NULL,
    run_id bigint NOT NULL,
    catalog_id bigint,
    conn_id text NOT NULL,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    source_row_count bigint,
    lake_row_count bigint,
    row_count_delta bigint,
    row_count_status text DEFAULT 'skip'::text NOT NULL,
    apply_lag_seconds integer,
    apply_status text,
    pk_checksum_match boolean,
    status text DEFAULT 'ok'::text NOT NULL,
    checks jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT reconciliation_result_row_count_status_check CHECK ((row_count_status = ANY (ARRAY['ok'::text, 'warn'::text, 'fail'::text, 'skip'::text]))),
    CONSTRAINT reconciliation_result_status_check CHECK ((status = ANY (ARRAY['ok'::text, 'warn'::text, 'fail'::text, 'skip'::text])))
);


--
-- Name: TABLE reconciliation_result; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TABLE cdc_catalog.reconciliation_result IS 'Per-table pipeline reconcile: apply/capture/Kafka lag from cdc_catalog metadata';


--
-- Name: reconciliation_result_result_id_seq; Type: SEQUENCE; Schema: cdc_catalog; Owner: -
--

ALTER TABLE cdc_catalog.reconciliation_result ALTER COLUMN result_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME cdc_catalog.reconciliation_result_result_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: reconciliation_result_daily; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.reconciliation_result_daily (
    snapshot_date date NOT NULL,
    catalog_id bigint NOT NULL,
    conn_id text NOT NULL,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    status text NOT NULL,
    row_count_delta bigint,
    drift_kind text,
    semaphore_reason text,
    recommended_action text,
    source_row_count bigint,
    lake_row_count bigint,
    run_id bigint NOT NULL,
    checks jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: reconciliation_result_daily reconciliation_result_daily_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.reconciliation_result_daily
    ADD CONSTRAINT reconciliation_result_daily_pkey PRIMARY KEY (snapshot_date, catalog_id);


--
-- Name: reconciliation_result_daily_conn_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX reconciliation_result_daily_conn_idx ON cdc_catalog.reconciliation_result_daily USING btree (conn_id, status, snapshot_date DESC);


--
-- Name: reconciliation_run; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.reconciliation_run (
    run_id bigint NOT NULL,
    batch_id text NOT NULL,
    conn_id text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    status text DEFAULT 'running'::text NOT NULL,
    tables_checked integer DEFAULT 0 NOT NULL,
    tables_ok integer DEFAULT 0 NOT NULL,
    tables_warn integer DEFAULT 0 NOT NULL,
    tables_fail integer DEFAULT 0 NOT NULL,
    stale_tables integer DEFAULT 0 NOT NULL,
    reconcile_mode text DEFAULT 'lake'::text NOT NULL,
    context jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT reconciliation_run_status_check CHECK ((status = ANY (ARRAY['running'::text, 'ok'::text, 'warn'::text, 'fail'::text]))),
    CONSTRAINT reconciliation_run_mode_check CHECK ((reconcile_mode = 'lake'::text))
);


COMMENT ON TABLE cdc_catalog.reconciliation_run IS 'One row per reconcile CLI run (lake: pipeline metadata only, no OLTP).';


--
-- Name: reconciliation_run_run_id_seq; Type: SEQUENCE; Schema: cdc_catalog; Owner: -
--

ALTER TABLE cdc_catalog.reconciliation_run ALTER COLUMN run_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME cdc_catalog.reconciliation_run_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: runtime_config; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.runtime_config (
    config_key text NOT NULL,
    component text DEFAULT 'global'::text NOT NULL,
    conn_id text DEFAULT ''::text NOT NULL,
    config_value jsonb NOT NULL,
    description text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT runtime_config_value_is_scalar CHECK ((jsonb_typeof(config_value) = ANY (ARRAY['number'::text, 'string'::text, 'boolean'::text])))
);


--
-- Name: TABLE runtime_config; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON TABLE cdc_catalog.runtime_config IS 'Hot-reload runtime knobs. C++ reads before each run/table; JSON file only holds connection credentials.';


--
-- Name: v_apply_stale; Type: VIEW; Schema: cdc_catalog; Owner: -
--

CREATE VIEW cdc_catalog.v_apply_stale AS
 SELECT ap.catalog_id,
    ap.conn_id,
    ap.source_schema,
    ap.source_table,
    ap.last_applied_at,
    ap.apply_lag_seconds,
    ap.status,
    ap.quarantined_at,
    ap.last_error,
    (now() - ap.last_applied_at) AS lag_interval
   FROM (cdc_catalog.apply_position ap
     JOIN cdc_catalog.catalog c USING (catalog_id))
  WHERE ((c.active = true) AND (c.cdc_enabled = true) AND (c.needs_full_load = false) AND (ap.status = ANY (ARRAY['stale'::cdc_catalog.cdc_health_status, 'lagging'::cdc_catalog.cdc_health_status, 'gap_detected'::cdc_catalog.cdc_health_status, 'quarantined'::cdc_catalog.cdc_health_status, 'failed'::cdc_catalog.cdc_health_status])));


--
-- Name: VIEW v_apply_stale; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON VIEW cdc_catalog.v_apply_stale IS 'Tables with elevated apply lag or unhealthy status.';


--
-- Name: v_capture_health; Type: VIEW; Schema: cdc_catalog; Owner: -
--

CREATE VIEW cdc_catalog.v_capture_health AS
 SELECT conn_id,
    gtid_set,
    binlog_file,
    binlog_position,
    last_event_ts,
    capture_lag_seconds,
    status,
    last_error,
    updated_at
   FROM cdc_catalog.capture_position cp;


--
-- Name: VIEW v_capture_health; Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON VIEW cdc_catalog.v_capture_health IS 'GTID capture cursor health per conn_id.';


--
-- Name: v_cdc_pipeline_summary; Type: VIEW; Schema: cdc_catalog; Owner: -
--

CREATE VIEW cdc_catalog.v_cdc_pipeline_summary AS
 SELECT c.conn_id,
    count(*) FILTER (WHERE (c.active AND c.cdc_enabled AND (NOT c.needs_full_load))) AS cdc_ready,
    count(*) FILTER (WHERE (ap.status = 'healthy'::cdc_catalog.cdc_health_status)) AS apply_healthy,
    count(*) FILTER (WHERE (ap.status = ANY (ARRAY['stale'::cdc_catalog.cdc_health_status, 'lagging'::cdc_catalog.cdc_health_status]))) AS apply_lagging,
    count(*) FILTER (WHERE (ap.status = 'quarantined'::cdc_catalog.cdc_health_status)) AS apply_quarantined,
    max(ap.apply_lag_seconds) AS max_apply_lag_seconds
   FROM (cdc_catalog.catalog c
     LEFT JOIN cdc_catalog.apply_position ap USING (catalog_id))
  WHERE (c.db_engine = 'mariadb'::cdc_catalog.db_engine)
  GROUP BY c.conn_id;


--
-- Name: apply_batch_stats apply_batch_stats_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.apply_batch_stats
    ADD CONSTRAINT apply_batch_stats_pkey PRIMARY KEY (stat_id);


--
-- Name: apply_position apply_position_object_uk; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.apply_position
    ADD CONSTRAINT apply_position_object_uk UNIQUE (conn_id, source_schema, source_table);


--
-- Name: apply_position apply_position_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.apply_position
    ADD CONSTRAINT apply_position_pkey PRIMARY KEY (catalog_id);


--
-- Name: capture_position capture_position_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.capture_position
    ADD CONSTRAINT capture_position_pkey PRIMARY KEY (conn_id);


--
-- Name: catalog catalog_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.catalog
    ADD CONSTRAINT catalog_pkey PRIMARY KEY (catalog_id);


--
-- Name: catalog catalog_source_object_uk; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.catalog
    ADD CONSTRAINT catalog_source_object_uk UNIQUE (conn_id, db_engine, source_database, source_schema, source_table);


--
-- Name: cdc_applied_events cdc_applied_events_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.cdc_applied_events
    ADD CONSTRAINT cdc_applied_events_pkey PRIMARY KEY (event_id);


--
-- Name: cdc_mongo_resume cdc_mongo_resume_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.cdc_mongo_resume
    ADD CONSTRAINT cdc_mongo_resume_pkey PRIMARY KEY (conn_id, database, collection);


--
-- Name: cdc_mssql_lsn cdc_mssql_lsn_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.cdc_mssql_lsn
    ADD CONSTRAINT cdc_mssql_lsn_pkey PRIMARY KEY (conn_id, database, schema_name, table_name);


--
-- Name: connections connections_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.connections
    ADD CONSTRAINT connections_pkey PRIMARY KEY (alias);


--
-- Name: logs logs_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.logs
    ADD CONSTRAINT logs_pkey PRIMARY KEY (log_id);


--
-- Name: reconciliation_result reconciliation_result_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.reconciliation_result
    ADD CONSTRAINT reconciliation_result_pkey PRIMARY KEY (result_id);


--
-- Name: reconciliation_result reconciliation_result_run_conn_table_uk; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.reconciliation_result
    ADD CONSTRAINT reconciliation_result_run_conn_table_uk UNIQUE (run_id, conn_id, source_schema, source_table);


--
-- Name: reconciliation_run reconciliation_run_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.reconciliation_run
    ADD CONSTRAINT reconciliation_run_pkey PRIMARY KEY (run_id);


--
-- Name: runtime_config runtime_config_pk; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.runtime_config
    ADD CONSTRAINT runtime_config_pk PRIMARY KEY (config_key, component, conn_id);


--
-- Name: apply_batch_stats_batch_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX apply_batch_stats_batch_idx ON cdc_catalog.apply_batch_stats USING btree (batch_id, conn_id);


--
-- Name: apply_batch_stats_health_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX apply_batch_stats_health_idx ON cdc_catalog.apply_batch_stats USING btree (conn_id, is_stale, is_inactive);


--
-- Name: apply_batch_stats_logged_at_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX apply_batch_stats_logged_at_idx ON cdc_catalog.apply_batch_stats USING btree (logged_at);


--
-- Name: apply_batch_stats_rag_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX apply_batch_stats_health_rag_idx ON cdc_catalog.apply_batch_stats USING btree (conn_id, apply_health_rag, logged_at DESC);


--
-- Name: apply_batch_stats_semaphore_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX apply_batch_stats_semaphore_idx ON cdc_catalog.apply_batch_stats USING btree (conn_id, semaphore, logged_at DESC);


--
-- Name: apply_batch_stats_table_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX apply_batch_stats_table_idx ON cdc_catalog.apply_batch_stats USING btree (conn_id, source_schema, source_table, logged_at DESC);


--
-- Name: apply_position_stale_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX apply_position_stale_idx ON cdc_catalog.apply_position USING btree (status, apply_lag_seconds DESC) WHERE (status = ANY (ARRAY['stale'::cdc_catalog.cdc_health_status, 'lagging'::cdc_catalog.cdc_health_status, 'gap_detected'::cdc_catalog.cdc_health_status]));


--
-- Name: catalog_active_cdc_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX catalog_active_cdc_idx ON cdc_catalog.catalog USING btree (conn_id) WHERE ((active = true) AND (cdc_enabled = true) AND (needs_full_load = false));


--
-- Name: catalog_engine_meta_gin; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX catalog_engine_meta_gin ON cdc_catalog.catalog USING gin (engine_meta jsonb_path_ops);


--
-- Name: catalog_failed_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX catalog_failed_idx ON cdc_catalog.catalog USING btree (last_error_at DESC) WHERE (status = 'failed'::cdc_catalog.replication_status);


--
-- Name: catalog_needs_full_load_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX catalog_needs_full_load_idx ON cdc_catalog.catalog USING btree (conn_id) WHERE ((active = true) AND (needs_full_load = true));


--
-- Name: cdc_applied_events_applied_at_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX cdc_applied_events_applied_at_idx ON cdc_catalog.cdc_applied_events USING btree (applied_at);


--
-- Name: cdc_applied_events_table_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX cdc_applied_events_table_idx ON cdc_catalog.cdc_applied_events USING btree (conn_id, source_schema, source_table, applied_at DESC);


--
-- Name: cdc_mongo_resume_updated_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX cdc_mongo_resume_updated_idx ON cdc_catalog.cdc_mongo_resume USING btree (conn_id, updated_at DESC);


--
-- Name: cdc_mssql_lsn_updated_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX cdc_mssql_lsn_updated_idx ON cdc_catalog.cdc_mssql_lsn USING btree (conn_id, updated_at DESC);


--
-- Name: connections_active_engine_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX connections_active_engine_idx ON cdc_catalog.connections USING btree (db_engine, alias) WHERE (active = true);


--
-- Name: logs_component_logged_at_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX logs_component_logged_at_idx ON cdc_catalog.logs USING btree (component, logged_at DESC);


--
-- Name: logs_context_gin; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX logs_context_gin ON cdc_catalog.logs USING gin (context jsonb_path_ops);


--
-- Name: logs_errors_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX logs_errors_idx ON cdc_catalog.logs USING btree (logged_at DESC) WHERE (level = ANY (ARRAY['warning'::cdc_catalog.log_level, 'error'::cdc_catalog.log_level]));


--
-- Name: logs_logged_at_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX logs_logged_at_idx ON cdc_catalog.logs USING btree (logged_at DESC);


--
-- Name: reconciliation_result_run_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX reconciliation_result_run_idx ON cdc_catalog.reconciliation_result USING btree (run_id);


--
-- Name: reconciliation_result_status_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX reconciliation_result_status_idx ON cdc_catalog.reconciliation_result USING btree (conn_id, status, source_schema, source_table);


--
-- Name: reconciliation_run_conn_started_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX reconciliation_run_conn_started_idx ON cdc_catalog.reconciliation_run USING btree (conn_id, started_at DESC);


--
-- Name: runtime_config_component_idx; Type: INDEX; Schema: cdc_catalog; Owner: -
--

CREATE INDEX runtime_config_component_idx ON cdc_catalog.runtime_config USING btree (component, conn_id);


--
-- Name: catalog catalog_updated_at_trg; Type: TRIGGER; Schema: cdc_catalog; Owner: -
--

CREATE TRIGGER catalog_updated_at_trg BEFORE UPDATE ON cdc_catalog.catalog FOR EACH ROW EXECUTE FUNCTION cdc_catalog.touch_updated_at();


--
-- Name: connections connections_updated_at_trg; Type: TRIGGER; Schema: cdc_catalog; Owner: -
--

CREATE TRIGGER connections_updated_at_trg BEFORE UPDATE ON cdc_catalog.connections FOR EACH ROW EXECUTE FUNCTION cdc_catalog.touch_updated_at();


--
-- Name: runtime_config runtime_config_updated_at_trg; Type: TRIGGER; Schema: cdc_catalog; Owner: -
--

CREATE TRIGGER runtime_config_updated_at_trg BEFORE UPDATE ON cdc_catalog.runtime_config FOR EACH ROW EXECUTE FUNCTION cdc_catalog.touch_updated_at();


--
-- Name: apply_position apply_position_catalog_id_fkey; Type: FK CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.apply_position
    ADD CONSTRAINT apply_position_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES cdc_catalog.catalog(catalog_id) ON DELETE CASCADE;


--
-- Name: reconciliation_result reconciliation_result_run_id_fkey; Type: FK CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.reconciliation_result
    ADD CONSTRAINT reconciliation_result_run_id_fkey FOREIGN KEY (run_id) REFERENCES cdc_catalog.reconciliation_run(run_id) ON DELETE CASCADE;


--
-- Name: schema_migrations; Type: TABLE; Schema: cdc_catalog; Owner: -
--

CREATE TABLE cdc_catalog.schema_migrations (
    version integer NOT NULL,
    description text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- PostgreSQL database dump complete
--
)PO_datasync_bas";
    }
    inline std::string_view datasync_incremental() {
        return R"PO_datasync_inc(
-- capture_during_full_load: schema_patches migration 059 (versioned one-time DDL).

-- Remove pipeline_health dashboard (unused). Observability: apply_batch_stats + logs.

DROP VIEW IF EXISTS cdc_catalog.v_pipeline_health;

DROP FUNCTION IF EXISTS cdc_catalog.refresh_pipeline_health_totals(
    cdc_catalog.service_tier, cdc_catalog.db_engine);

DROP FUNCTION IF EXISTS cdc_catalog.refresh_pipeline_health_live(
    text, cdc_catalog.service_tier, cdc_catalog.db_engine);

DROP TABLE IF EXISTS cdc_catalog.pipeline_health;

-- Migration 002: one-time reconcile history reset (legacy).
-- runtime_config seeds: schema_patches migrations 038/040/054/055/056/057 (not re-run here).

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 2) THEN
        IF to_regclass('cdc_catalog.reconciliation_run') IS NOT NULL THEN
            TRUNCATE cdc_catalog.reconciliation_run RESTART IDENTITY CASCADE;
        END IF;
        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (2, 'reconciliation_run history reset (legacy)');
        RAISE NOTICE 'migration 002: reconciliation_run truncated (if present)';
    END IF;
END $$;
)PO_datasync_inc";
    }
    inline std::string_view diagnostics() {
        return R"PO_diagnostics(
\echo '=== connections (daemon only captures these conn_id) ==='
SELECT alias AS conn_id, db_engine, host, port, active
FROM cdc_catalog.connections
ORDER BY alias;

\echo '=== per-table capture eligibility (capture_ready must be true) ==='
SELECT
    conn_id,
    source_schema,
    source_table,
    active,
    cdc_enabled,
    needs_full_load,
    capture_during_full_load,
    has_pk,
    status,
    (active
     AND cdc_enabled
     AND (
       (db_engine <> 'mssql' AND (NOT needs_full_load OR capture_during_full_load))
       OR (db_engine = 'mssql' AND NOT needs_full_load)
     )
     AND has_pk
     AND status NOT IN ('skipped', 'disabled')) AS capture_ready,
    CASE
        WHEN NOT active THEN 'active=false'
        WHEN NOT cdc_enabled THEN 'cdc_enabled=false'
        WHEN needs_full_load AND NOT capture_during_full_load THEN 'needs_full_load=true'
        WHEN NOT has_pk THEN 'has_pk=false'
        WHEN status IN ('skipped', 'disabled') THEN 'status=' || status::text
        ELSE 'ok'
    END AS block_reason
FROM cdc_catalog.catalog
ORDER BY conn_id, capture_ready DESC, source_schema, source_table;

\echo '=== summary by conn / engine ==='
SELECT
    conn_id,
    db_engine,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE active) AS active,
    COUNT(*) FILTER (WHERE cdc_enabled) AS cdc_enabled,
    COUNT(*) FILTER (WHERE needs_full_load) AS needs_full_load,
    COUNT(*) FILTER (WHERE capture_during_full_load) AS capture_during_full_load,
    COUNT(*) FILTER (WHERE status = 'success') AS status_success,
    COUNT(*) FILTER (
        WHERE active
          AND cdc_enabled
          AND (
            (db_engine <> 'mssql' AND (NOT needs_full_load OR capture_during_full_load))
            OR (db_engine = 'mssql' AND NOT needs_full_load)
          )
          AND has_pk
          AND status NOT IN ('skipped', 'disabled')
    ) AS capture_ready
FROM cdc_catalog.catalog
GROUP BY conn_id, db_engine
ORDER BY conn_id;

\echo '=== capture_position (binlog cursor — required for MariaDB capture) ==='
SELECT conn_id, binlog_file, binlog_position, status, last_error, updated_at
FROM cdc_catalog.capture_position
ORDER BY conn_id;

\echo '=== recent capture skip / errors ==='
SELECT created_at, level, component, message, conn_id, context
FROM cdc_catalog.logs
WHERE component IN ('cdc_kafka_capture', 'cdc_kafka_daemon', 'cdc_kafka_apply_cpp')
  AND (message ILIKE '%skip%' OR message ILIKE '%capture%')
  AND logged_at > now() - interval '6 hours'
ORDER BY logged_at DESC
LIMIT 20;
)PO_diagnostics";
    }
    inline std::string_view schema_patches() {
        return R"PO_schema_patch(

-- Migration 003 (legacy): reconcile_mode on reconciliation_run — once only.
-- Previously unversioned: every migrate DROP/ADD CONSTRAINT → ACCESS EXCLUSIVE locks vs apply.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 3) THEN
        IF to_regclass('cdc_catalog.reconciliation_run') IS NOT NULL THEN
            ALTER TABLE cdc_catalog.reconciliation_run
                ADD COLUMN IF NOT EXISTS reconcile_mode text NOT NULL DEFAULT 'full';

            ALTER TABLE cdc_catalog.reconciliation_run
                DROP CONSTRAINT IF EXISTS reconciliation_run_mode_check;

            ALTER TABLE cdc_catalog.reconciliation_run
                ADD CONSTRAINT reconciliation_run_mode_check
                    CHECK (reconcile_mode = 'full'::text);

            COMMENT ON TABLE cdc_catalog.reconciliation_run IS
                'Legacy reconcile CLI run header (removed — table dropped by migration 046)';
        END IF;
        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (3, 'reconcile_mode on reconciliation_run (legacy; no-op when table already dropped)');
        RAISE NOTICE 'migration 003: reconcile_mode recorded (DDL applied only if table present)';
    END IF;
END $$;

-- Migration 004: apply observability columns — once only (ADD COLUMN still takes AccessExclusiveLock).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 4) THEN
        ALTER TABLE cdc_catalog.apply_batch_stats
            ADD COLUMN IF NOT EXISTS parse_skipped bigint NOT NULL DEFAULT 0;

        ALTER TABLE cdc_catalog.apply_batch_stats
            ADD COLUMN IF NOT EXISTS dropped_unrecoverable bigint NOT NULL DEFAULT 0;

        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.parse_skipped IS
            'Kafka messages skipped due to JSON/payload parse failure in apply slice (per table batch)';

        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.dropped_unrecoverable IS
            'Events dropped because lake schema/table could not be resolved (per table batch)';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (4, 'apply_batch_stats parse_skipped + dropped_unrecoverable columns');
        RAISE NOTICE 'migration 004: apply_batch_stats observability columns';
    END IF;
END $$;

-- Migration 035: drop service_tier (tier routing removed from C++ daemon).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 35) THEN
        DROP VIEW IF EXISTS cdc_catalog.v_kafka_consumer;
        DROP VIEW IF EXISTS cdc_catalog.v_apply_latest;
        DROP VIEW IF EXISTS cdc_catalog.v_apply_stale;
        DROP VIEW IF EXISTS cdc_catalog.v_cdc_pipeline_summary;
        DROP INDEX IF EXISTS cdc_catalog.catalog_active_cdc_idx;
        DROP INDEX IF EXISTS cdc_catalog.catalog_needs_full_load_idx;
        ALTER TABLE cdc_catalog.apply_batch_stats DROP COLUMN IF EXISTS service_tier;
        IF to_regclass('cdc_catalog.reconciliation_run') IS NOT NULL THEN
            ALTER TABLE cdc_catalog.reconciliation_run DROP COLUMN IF EXISTS service_tier;
        END IF;
        IF to_regclass('cdc_catalog.reconciliation_result') IS NOT NULL THEN
            ALTER TABLE cdc_catalog.reconciliation_result DROP COLUMN IF EXISTS service_tier;
        END IF;
        ALTER TABLE cdc_catalog.cdc_run_fairness_metrics DROP COLUMN IF EXISTS service_tier;
        ALTER TABLE cdc_catalog.catalog DROP COLUMN IF EXISTS service_tier;
        CREATE INDEX IF NOT EXISTS catalog_active_cdc_idx
            ON cdc_catalog.catalog USING btree (conn_id)
            WHERE active = true AND cdc_enabled = true AND needs_full_load = false;
        CREATE INDEX IF NOT EXISTS catalog_needs_full_load_idx
            ON cdc_catalog.catalog USING btree (conn_id)
            WHERE active = true AND needs_full_load = true;
        CREATE OR REPLACE VIEW cdc_catalog.v_apply_stale AS
         SELECT ap.catalog_id, ap.conn_id, ap.source_schema, ap.source_table,
                ap.last_applied_at, ap.apply_lag_seconds, ap.status, ap.quarantined_at,
                ap.last_error, (now() - ap.last_applied_at) AS lag_interval
           FROM cdc_catalog.apply_position ap
           JOIN cdc_catalog.catalog c USING (catalog_id)
          WHERE c.active = true AND c.cdc_enabled = true AND c.needs_full_load = false
            AND ap.status = ANY (ARRAY['stale','lagging','gap_detected','quarantined','failed']::cdc_catalog.cdc_health_status[]);
        CREATE OR REPLACE VIEW cdc_catalog.v_cdc_pipeline_summary AS
         SELECT c.conn_id,
                count(*) FILTER (WHERE c.active AND c.cdc_enabled AND NOT c.needs_full_load) AS cdc_ready,
                count(*) FILTER (WHERE ap.status = 'healthy'::cdc_catalog.cdc_health_status) AS apply_healthy,
                count(*) FILTER (WHERE ap.status = ANY (ARRAY['stale','lagging']::cdc_catalog.cdc_health_status[])) AS apply_lagging,
                count(*) FILTER (WHERE ap.status = 'quarantined'::cdc_catalog.cdc_health_status) AS apply_quarantined,
                max(ap.apply_lag_seconds) AS max_apply_lag_seconds
           FROM cdc_catalog.catalog c
           LEFT JOIN cdc_catalog.apply_position ap USING (catalog_id)
          WHERE c.db_engine = 'mariadb'::cdc_catalog.db_engine
          GROUP BY c.conn_id;
        CREATE OR REPLACE VIEW cdc_catalog.v_apply_latest AS
         SELECT DISTINCT ON (conn_id, source_schema, source_table)
                stat_id, batch_id, conn_id, catalog_id, source_schema, source_table,
                events_total, events_updates, events_inserts, events_deletes,
                duration_ms, events_per_minute, kafka_topic, kafka_partition, kafka_offset,
                kafka_consumer_lag, apply_lag_seconds, is_stale, is_inactive,
                reconciliation_rag, dedup_skipped, parse_skipped, dropped_unrecoverable,
                logged_at
           FROM cdc_catalog.apply_batch_stats
          ORDER BY conn_id, source_schema, source_table, logged_at DESC;
        CREATE OR REPLACE VIEW cdc_catalog.v_kafka_consumer AS
         SELECT ap.conn_id, ap.source_schema, ap.source_table, ap.kafka_topic, ap.kafka_partition,
                ap.kafka_offset AS consumed_offset, ap.last_applied_at, ap.apply_lag_seconds,
                ap.status AS apply_position_status, latest.kafka_consumer_lag,
                latest.events_total AS last_slice_events, latest.logged_at AS last_apply_at,
                latest.is_inactive, latest.is_stale, latest.reconciliation_rag
           FROM cdc_catalog.apply_position ap
           JOIN cdc_catalog.catalog c ON c.catalog_id = ap.catalog_id
           LEFT JOIN cdc_catalog.v_apply_latest latest
             ON latest.conn_id = ap.conn_id AND latest.source_schema = ap.source_schema
            AND latest.source_table = ap.source_table;
        DROP TYPE IF EXISTS cdc_catalog.service_tier;
        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (35, 'drop service_tier column and enum from cdc_catalog');
        RAISE NOTICE 'migration 035: service_tier dropped';
    END IF;
END $$;

-- Migration 036 (legacy): reconcile full only — noop when reconcile tables absent.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 36) THEN
        IF to_regclass('cdc_catalog.reconciliation_run') IS NOT NULL THEN
            ALTER TABLE cdc_catalog.reconciliation_run
                DROP CONSTRAINT IF EXISTS reconciliation_run_mode_check;
            ALTER TABLE cdc_catalog.reconciliation_run
                ADD CONSTRAINT reconciliation_run_mode_check
                CHECK (reconcile_mode = 'full'::text);
            UPDATE cdc_catalog.reconciliation_run
            SET reconcile_mode = 'full'
            WHERE reconcile_mode IS DISTINCT FROM 'full';
        END IF;
        DELETE FROM cdc_catalog.runtime_config
        WHERE component = 'cdc_kafka_reconcile'
          AND config_key IN ('reconcile_mode', 'reconcile_full_interval_hours');
        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (36, 'reconcile full only (legacy; noop after 046)');
        RAISE NOTICE 'migration 036: reconcile full only (legacy)';
    END IF;
END $$;

-- Migration 037: drop legacy apply_batch_stats columns, fairness table, dead runtime keys.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 37) THEN
        DROP VIEW IF EXISTS cdc_catalog.v_kafka_consumer;
        DROP VIEW IF EXISTS cdc_catalog.v_apply_latest;

        DROP INDEX IF EXISTS cdc_catalog.apply_batch_stats_health_idx;

        ALTER TABLE cdc_catalog.apply_batch_stats
            DROP COLUMN IF EXISTS catchup_triggered,
            DROP COLUMN IF EXISTS fk_deferred_retries,
            DROP COLUMN IF EXISTS is_starving,
            DROP COLUMN IF EXISTS host_cpu_percent,
            DROP COLUMN IF EXISTS host_mem_used_mb,
            DROP COLUMN IF EXISTS host_mem_percent,
            DROP COLUMN IF EXISTS host_net_rx_mb,
            DROP COLUMN IF EXISTS host_net_tx_mb,
            DROP COLUMN IF EXISTS process_rss_mb;

        DROP TABLE IF EXISTS cdc_catalog.cdc_run_fairness_metrics;

        CREATE INDEX IF NOT EXISTS apply_batch_stats_health_idx
            ON cdc_catalog.apply_batch_stats USING btree (conn_id, is_stale, is_inactive);

        CREATE OR REPLACE VIEW cdc_catalog.v_apply_latest AS
         SELECT DISTINCT ON (conn_id, source_schema, source_table)
                stat_id, batch_id, conn_id, catalog_id, source_schema, source_table,
                events_total, events_updates, events_inserts, events_deletes,
                duration_ms, events_per_minute, kafka_topic, kafka_partition, kafka_offset,
                kafka_consumer_lag, apply_lag_seconds, is_stale, is_inactive,
                reconciliation_rag, dedup_skipped, parse_skipped, dropped_unrecoverable,
                logged_at
           FROM cdc_catalog.apply_batch_stats
          ORDER BY conn_id, source_schema, source_table, logged_at DESC;

        CREATE OR REPLACE VIEW cdc_catalog.v_kafka_consumer AS
         SELECT ap.conn_id, ap.source_schema, ap.source_table, ap.kafka_topic, ap.kafka_partition,
                ap.kafka_offset AS consumed_offset, ap.last_applied_at, ap.apply_lag_seconds,
                ap.status AS apply_position_status, latest.kafka_consumer_lag,
                latest.events_total AS last_slice_events, latest.logged_at AS last_apply_at,
                latest.is_inactive, latest.is_stale, latest.reconciliation_rag
           FROM cdc_catalog.apply_position ap
           JOIN cdc_catalog.catalog c ON c.catalog_id = ap.catalog_id
           LEFT JOIN cdc_catalog.v_apply_latest latest
             ON latest.conn_id = ap.conn_id AND latest.source_schema = ap.source_schema
            AND latest.source_table = ap.source_table;

        DELETE FROM cdc_catalog.runtime_config
        WHERE config_key IN (
            'apply_worker_count',
            'full_load_parallel_tables',
            'apply_process_rss_cap_mb',
            'apply_append_only',
            'apply_catchup_enabled',
            'apply_catchup_kafka_messages',
            'apply_catchup_lag_seconds',
            'apply_catchup_max_tables',
            'apply_catchup_min_kafka_messages',
            'apply_exit_on_targets_met',
            'kafka_purge_consumed_enabled',
            'kafka_purge_consumed_interval_seconds',
            'kafka_purge_consumed_max_lag',
            'kafka_purge_consumed_min_deletable_offsets'
        );

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (37, 'drop legacy apply_batch_stats columns, cdc_run_fairness_metrics, dead runtime keys');
        RAISE NOTICE 'migration 037: apply_batch_stats cleanup';
    END IF;
END $$;

-- Migration 038: runtime_config canonical 5 keys only; rest in pipeline_defaults.hpp.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 38) THEN
        DELETE FROM cdc_catalog.runtime_config
        WHERE config_key = 'apply_target_events_per_table'
           OR (config_key, component, COALESCE(conn_id, '')) NOT IN (
                ('full_load_batch_size',          'mariadb_load',        ''),
                ('apply_batch_size',              'cdc_kafka_apply',     ''),
                ('logs_retention_days',           'global',              ''),
                ('applied_events_retention_days', 'cdc_kafka_apply',     '')
           );

        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES
            ('full_load_batch_size',          'mariadb_load',        '', '50000'::jsonb, 'MariaDB full-load COPY batch size'),
            ('apply_batch_size',              'cdc_kafka_apply',     '', '20000'::jsonb, 'Apply batch before flush'),
            ('logs_retention_days',           'global',              '', '7'::jsonb, 'Purge cdc_catalog.logs retention'),
            ('applied_events_retention_days', 'cdc_kafka_apply',     '', '7'::jsonb, 'Dedup audit retention')
        ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
            config_value = EXCLUDED.config_value,
            description  = EXCLUDED.description,
            updated_at   = now();

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (38, 'runtime_config 5 keys only; pipeline_defaults.hpp for rest');
        RAISE NOTICE 'migration 038: runtime_config reduced to 5 canonical keys';
    END IF;
END $$;

-- Migration 039: catalog.hot — per_table Kafka topic + dedicated apply consumer pool.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 39) THEN
        ALTER TABLE cdc_catalog.catalog
            ADD COLUMN IF NOT EXISTS hot boolean NOT NULL DEFAULT false;

        COMMENT ON COLUMN cdc_catalog.catalog.hot IS
            'Operator flag: dedicated per_table Kafka topic and hot apply workers (see pipeline_defaults kHotApplyConsumerCount).';

        CREATE INDEX IF NOT EXISTS idx_catalog_hot
            ON cdc_catalog.catalog (conn_id)
            WHERE hot = true AND active = true AND cdc_enabled = true;

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (39, 'catalog hot flag for per_table topics and dedicated apply path');
        RAISE NOTICE 'migration 039: catalog.hot column added';
    END IF;
END $$;

-- Migration 040: apply throughput runtime keys (mirror apply, kafka sharding, lake commit tuning).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 40) THEN
        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES
            ('apply_worker_count', 'cdc_kafka_apply', '', '8'::jsonb,
             'Cold-path apply worker threads per connection (must divide kafka_topic_partitions evenly)'),
            ('kafka_topic_partitions', 'cdc_kafka_capture', '', '24'::jsonb,
             'Kafka topic partition count for CDC topics (capture producer routing)'),
            ('kafka_topic_partitions', 'cdc_kafka_apply', '', '24'::jsonb,
             'Kafka topic partition count for apply consumer shard assignment'),
            ('apply_lake_synchronous_commit_off', 'cdc_kafka_apply', '', 'true'::jsonb,
             'SET synchronous_commit=off on lake apply connections (cold path; hot always off)')
        ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
            config_value = EXCLUDED.config_value,
            description  = EXCLUDED.description,
            updated_at   = now();

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (40, 'apply throughput: workers, kafka partitions, synchronous_commit');
        RAISE NOTICE 'migration 040: apply throughput runtime keys';
    END IF;
END $$;

-- Migration 041: exact table lag scan timeout + column comment refresh.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 41) THEN
        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES
            ('table_lag_scan_timeout_ms', 'cdc_kafka_apply', '', '120000'::jsonb,
             'Max milliseconds for end-of-slice exact Kafka table lag scan per catalog table')
        ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
            config_value = EXCLUDED.config_value,
            description  = EXCLUDED.description,
            updated_at   = now();

        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.kafka_consumer_lag IS
            'Exact table backlog in Kafka (messages matching this table ahead of apply offset; not partition watermark)';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (41, 'exact table kafka lag: scan timeout runtime key, column comment');
        RAISE NOTICE 'migration 041: exact table kafka lag';
    END IF;
END $$;

-- Migration 042: mirror apply PK indexes (dl_mir_*_pk) + chunked CDC deletes (C++ apply).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 42) THEN
        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (42, 'mirror apply PK index backfill on apply worker 0; chunked CDC deletes (5000 PKs)');
        RAISE NOTICE 'migration 042: mirror apply PK indexes + chunked deletes';
    END IF;
END $$;

-- Migration 043 (legacy): reconcile enrichments — noop when reconcile tables absent.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 43) THEN
        IF to_regclass('cdc_catalog.reconciliation_result') IS NOT NULL THEN
            ALTER TABLE cdc_catalog.reconciliation_result
                DROP CONSTRAINT IF EXISTS reconciliation_result_run_table_uk;
            ALTER TABLE cdc_catalog.reconciliation_result
                ADD CONSTRAINT reconciliation_result_run_conn_table_uk
                UNIQUE (run_id, conn_id, source_schema, source_table);
        END IF;

        IF to_regclass('cdc_catalog.reconciliation_result_daily') IS NULL
           AND to_regclass('cdc_catalog.reconciliation_result') IS NOT NULL THEN
            CREATE TABLE cdc_catalog.reconciliation_result_daily (
                snapshot_date date NOT NULL,
                catalog_id bigint NOT NULL,
                conn_id text NOT NULL,
                source_schema text NOT NULL,
                source_table text NOT NULL,
                status text NOT NULL,
                row_count_delta bigint,
                drift_kind text,
                semaphore_reason text,
                recommended_action text,
                source_row_count bigint,
                lake_row_count bigint,
                run_id bigint NOT NULL,
                checks jsonb DEFAULT '{}'::jsonb NOT NULL,
                updated_at timestamp with time zone DEFAULT now() NOT NULL,
                PRIMARY KEY (snapshot_date, catalog_id)
            );

            CREATE INDEX reconciliation_result_daily_conn_idx
                ON cdc_catalog.reconciliation_result_daily (conn_id, status, snapshot_date DESC);

            COMMENT ON TABLE cdc_catalog.reconciliation_result_daily IS
                'Legacy daily reconcile snapshot (removed by migration 046)';
        END IF;

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (43, 'reconcile enrichments (legacy; noop after 046)');
        RAISE NOTICE 'migration 043: reconcile enrichments (legacy)';
    END IF;
END $$;

-- Migration 044 (legacy): reconcile lake mode — noop when reconcile tables absent.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 44) THEN
        IF to_regclass('cdc_catalog.reconciliation_run') IS NOT NULL THEN
            ALTER TABLE cdc_catalog.reconciliation_run
                DROP CONSTRAINT IF EXISTS reconciliation_run_mode_check;
            ALTER TABLE cdc_catalog.reconciliation_run
                ADD CONSTRAINT reconciliation_run_mode_check
                CHECK (reconcile_mode = ANY (ARRAY['full'::text, 'lake'::text]));
        END IF;

        DELETE FROM cdc_catalog.runtime_config
        WHERE component = 'cdc_kafka_reconcile'
          AND config_key = 'reconcile_cycle_mode';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (44, 'reconcile lake mode (legacy; noop after 046)');
        RAISE NOTICE 'migration 044: reconcile lake mode (legacy)';
    END IF;
END $$;

-- Migration 045 (legacy): reconcile lake-only — noop when reconcile tables absent.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 45) THEN
        IF to_regclass('cdc_catalog.reconciliation_run') IS NOT NULL THEN
            ALTER TABLE cdc_catalog.reconciliation_run
                DROP CONSTRAINT IF EXISTS reconciliation_run_mode_check;

            UPDATE cdc_catalog.reconciliation_run
            SET reconcile_mode = 'lake'
            WHERE reconcile_mode IS DISTINCT FROM 'lake';

            ALTER TABLE cdc_catalog.reconciliation_run
                ADD CONSTRAINT reconciliation_run_mode_check
                CHECK (reconcile_mode = 'lake'::text);

            COMMENT ON TABLE cdc_catalog.reconciliation_run IS
                'Legacy reconcile CLI run header (removed by migration 046)';
        END IF;

        IF to_regclass('cdc_catalog.reconciliation_result') IS NOT NULL THEN
            COMMENT ON TABLE cdc_catalog.reconciliation_result IS
                'Legacy per-table reconcile result (removed by migration 046)';
        END IF;

        DELETE FROM cdc_catalog.runtime_config
        WHERE component = 'cdc_kafka_reconcile';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (45, 'reconcile lake-only (legacy; noop after 046)');
        RAISE NOTICE 'migration 045: reconcile lake-only (legacy)';
    END IF;
END $$;

-- Migration 046: drop reconcile pipeline tables (C++ reconcile job removed).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 46) THEN
        DROP TABLE IF EXISTS cdc_catalog.reconciliation_result CASCADE;
        DROP TABLE IF EXISTS cdc_catalog.reconciliation_result_daily CASCADE;
        DROP TABLE IF EXISTS cdc_catalog.reconciliation_run CASCADE;

        DELETE FROM cdc_catalog.runtime_config
        WHERE component = 'cdc_kafka_reconcile'
           OR config_key LIKE 'reconcile%';

        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.reconciliation_rag IS
            'Apply health RAG from apply_position status (GREEN/AMBER/RED/UNKNOWN)';

        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.reconcile_row_delta IS
            'Legacy column; always 0 after reconcile removal';

        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.semaphore IS
            'GREEN/AMBER/RED mirror of reconciliation_rag (apply health)';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (46, 'drop reconcile tables; apply-health-only CDC pipeline');
        RAISE NOTICE 'migration 046: reconcile tables dropped';
    END IF;
END $$;

-- Migration 047: apply_batch_stats post-reconcile health columns.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 47) THEN
        ALTER TABLE cdc_catalog.apply_batch_stats
            ADD COLUMN IF NOT EXISTS seconds_since_last_apply integer NOT NULL DEFAULT -1,
            ADD COLUMN IF NOT EXISTS kafka_partition_lag bigint,
            ADD COLUMN IF NOT EXISTS lag_scan_complete boolean NOT NULL DEFAULT false,
            ADD COLUMN IF NOT EXISTS slice_kind text NOT NULL DEFAULT 'flush',
            ADD COLUMN IF NOT EXISTS event_loss_status text NOT NULL DEFAULT 'ok',
            ADD COLUMN IF NOT EXISTS health_reason text NOT NULL DEFAULT 'healthy';

        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.seconds_since_last_apply IS
            'Seconds since apply_position.last_applied_at at slice time (-1 if unknown)';
        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.kafka_partition_lag IS
            'Partition high watermark minus consumed offset (not table-exact)';
        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.lag_scan_complete IS
            'True when exact table lag scan finished without message cap truncation';
        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.slice_kind IS
            'flush = per-table lake write; slice_finalize = end-of-slice lag heartbeat';
        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.event_loss_status IS
            'ok | warn (parse_skipped) | fail (dropped_unrecoverable)';
        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.health_reason IS
            'Primary apply-health signal: healthy, kafka_backlog (>=1k msgs), kafka_backlog_critical (>=50k), parse_skipped, apply_stale, etc.';
        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.reconciliation_rag IS
            'Apply health RAG: GREEN/AMBER/RED/UNKNOWN (derived from apply_position + slice metrics)';
        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.reconcile_row_delta IS
            'Deprecated — always 0 after reconcile removal';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (47, 'apply_batch_stats post-reconcile health columns');
        RAISE NOTICE 'migration 047: apply_batch_stats health columns';
    END IF;
END $$;

-- Migration 048: rename reconciliation_rag → apply_health_rag (post-reconcile semantics).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 48) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'cdc_catalog'
              AND table_name = 'apply_batch_stats'
              AND column_name = 'reconciliation_rag'
        ) THEN
            ALTER TABLE cdc_catalog.apply_batch_stats DROP COLUMN IF EXISTS semaphore;
            ALTER TABLE cdc_catalog.apply_batch_stats
                RENAME COLUMN reconciliation_rag TO apply_health_rag;
            ALTER TABLE cdc_catalog.apply_batch_stats
                DROP CONSTRAINT IF EXISTS apply_batch_stats_reconciliation_rag_chk;
            ALTER TABLE cdc_catalog.apply_batch_stats
                ADD CONSTRAINT apply_batch_stats_apply_health_rag_chk
                CHECK (apply_health_rag = ANY (ARRAY['GREEN','AMBER','RED','UNKNOWN']::text[]));
            ALTER TABLE cdc_catalog.apply_batch_stats
                ADD COLUMN semaphore text GENERATED ALWAYS AS (apply_health_rag) STORED;
            DROP INDEX IF EXISTS cdc_catalog.apply_batch_stats_rag_idx;
            CREATE INDEX IF NOT EXISTS apply_batch_stats_health_rag_idx
                ON cdc_catalog.apply_batch_stats (conn_id, apply_health_rag, logged_at DESC);
        END IF;

        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.apply_health_rag IS
            'Apply health RAG: GREEN/AMBER/RED/UNKNOWN from apply_position + slice metrics';
        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.semaphore IS
            'GREEN/AMBER/RED mirror of apply_health_rag';
        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.slice_kind IS
            'slice = unified per-table row (flush metrics + lag scan at slice end)';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (48, 'rename reconciliation_rag to apply_health_rag');
        RAISE NOTICE 'migration 048: apply_health_rag rename';
    END IF;
END $$;

-- Migration 050: full_load_checkpoint for resumable COPY + progress tracking.
-- (Version 49 was capture_during_full_load on legacy DBs — do not reuse.)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 50) THEN
        CREATE TABLE IF NOT EXISTS cdc_catalog.full_load_checkpoint (
            catalog_id bigint NOT NULL REFERENCES cdc_catalog.catalog(catalog_id) ON DELETE CASCADE,
            worker_id integer NOT NULL DEFAULT 0,
            batch_id text NOT NULL,
            phase text NOT NULL,
            last_pk jsonb,
            rows_loaded bigint NOT NULL DEFAULT 0,
            source_rows bigint,
            updated_at timestamp with time zone NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_id, worker_id),
            CONSTRAINT full_load_checkpoint_phase_chk
                CHECK (phase = ANY (ARRAY['truncate'::text, 'ddl'::text, 'copy'::text]))
        );

        CREATE INDEX IF NOT EXISTS full_load_checkpoint_updated_idx
            ON cdc_catalog.full_load_checkpoint (updated_at DESC);

        COMMENT ON TABLE cdc_catalog.full_load_checkpoint IS
            'Per-table (and per parallel worker) full-load resume checkpoint';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (50, 'full_load_checkpoint resumable COPY');
        RAISE NOTICE 'migration 050: full_load_checkpoint';
    END IF;
END $$;

-- Migration 051: apply_outbox — lake-first commit; audit retry without re-applying lake.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 51) THEN
        CREATE TABLE IF NOT EXISTS cdc_catalog.apply_outbox (
            event_id text PRIMARY KEY,
            conn_id text NOT NULL,
            catalog_id bigint NOT NULL,
            batch_id text NOT NULL,
            payload jsonb NOT NULL,
            lake_committed_at timestamptz NOT NULL DEFAULT now(),
            audit_committed_at timestamptz
        );

        CREATE INDEX IF NOT EXISTS apply_outbox_pending_idx
            ON cdc_catalog.apply_outbox (conn_id, lake_committed_at)
            WHERE audit_committed_at IS NULL;

        COMMENT ON TABLE cdc_catalog.apply_outbox IS
            'Pending catalog audit after lake COMMIT; drained before apply slices';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (51, 'apply_outbox lake-first audit retry');
        RAISE NOTICE 'migration 051: apply_outbox';
    END IF;
END $$;

-- Migration 052: reconcile-lite — single append-only reconciliation table + latest view.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 52) THEN
        CREATE TABLE IF NOT EXISTS cdc_catalog.reconciliation (
            reconciliation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            batch_id text NOT NULL,
            conn_id text NOT NULL,
            catalog_id bigint NOT NULL REFERENCES cdc_catalog.catalog(catalog_id) ON DELETE CASCADE,
            db_engine cdc_catalog.db_engine NOT NULL,
            source_schema text NOT NULL,
            source_table text NOT NULL,
            checked_at timestamptz NOT NULL DEFAULT now(),
            status text NOT NULL DEFAULT 'ok',
            row_count_source bigint,
            row_count_lake bigint,
            row_count_delta bigint,
            row_count_status text NOT NULL DEFAULT 'skip',
            max_pk_source text,
            max_pk_lake text,
            max_pk_status text NOT NULL DEFAULT 'skip',
            ts_column text,
            max_ts_source timestamptz,
            max_ts_lake timestamptz,
            max_ts_lag_seconds integer,
            max_ts_status text NOT NULL DEFAULT 'skip',
            pipeline_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
            checks jsonb NOT NULL DEFAULT '{}'::jsonb,
            duration_ms bigint NOT NULL DEFAULT 0,
            error text,
            CONSTRAINT reconciliation_status_chk
                CHECK (status = ANY (ARRAY['ok','warn','fail','skip']::text[])),
            CONSTRAINT reconciliation_row_count_status_chk
                CHECK (row_count_status = ANY (ARRAY['ok','warn','fail','skip']::text[])),
            CONSTRAINT reconciliation_max_pk_status_chk
                CHECK (max_pk_status = ANY (ARRAY['ok','warn','fail','skip']::text[])),
            CONSTRAINT reconciliation_max_ts_status_chk
                CHECK (max_ts_status = ANY (ARRAY['ok','warn','fail','skip']::text[]))
        );

        CREATE INDEX IF NOT EXISTS reconciliation_conn_checked_idx
            ON cdc_catalog.reconciliation (conn_id, checked_at DESC);
        CREATE INDEX IF NOT EXISTS reconciliation_catalog_checked_idx
            ON cdc_catalog.reconciliation (catalog_id, checked_at DESC);
        CREATE INDEX IF NOT EXISTS reconciliation_status_checked_idx
            ON cdc_catalog.reconciliation (status, checked_at DESC)
            WHERE status IN ('fail', 'warn');

        COMMENT ON TABLE cdc_catalog.reconciliation IS
            'Append-only reconcile-lite results: COUNT, MAX(PK), MAX(ts) per table per run';

        CREATE OR REPLACE VIEW cdc_catalog.v_reconciliation_latest AS
        SELECT DISTINCT ON (catalog_id) *
        FROM cdc_catalog.reconciliation
        ORDER BY catalog_id, checked_at DESC;

        COMMENT ON VIEW cdc_catalog.v_reconciliation_latest IS
            'Latest reconcile-lite row per catalog_id';

        CREATE OR REPLACE FUNCTION cdc_catalog.prune_reconciliation(p_retention_days integer DEFAULT 30)
        RETURNS bigint
        LANGUAGE sql
        AS $prune_reconciliation$
            WITH deleted AS (
                DELETE FROM cdc_catalog.reconciliation
                WHERE p_retention_days > 0
                  AND checked_at < now() - make_interval(days => p_retention_days)
                RETURNING 1
            )
            SELECT count(*)::bigint FROM deleted;
        $prune_reconciliation$;

        COMMENT ON FUNCTION cdc_catalog.prune_reconciliation(integer) IS
            'Delete reconcile-lite rows older than retention window';

        COMMENT ON COLUMN cdc_catalog.apply_batch_stats.reconcile_row_delta IS
            'source_row_count - lake_row_count from v_reconciliation_latest';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (52, 'reconcile-lite single table + v_reconciliation_latest');
        RAISE NOTICE 'migration 052: reconciliation table';
    END IF;
END $$;

-- Migration 053: repair 050/051/052 objects when schema_migrations rows exist without DDL.
DO $migration053$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 53) THEN
        CREATE TABLE IF NOT EXISTS cdc_catalog.full_load_checkpoint (
            catalog_id bigint NOT NULL REFERENCES cdc_catalog.catalog(catalog_id) ON DELETE CASCADE,
            worker_id integer NOT NULL DEFAULT 0,
            batch_id text NOT NULL,
            phase text NOT NULL,
            last_pk jsonb,
            rows_loaded bigint NOT NULL DEFAULT 0,
            source_rows bigint,
            updated_at timestamp with time zone NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_id, worker_id),
            CONSTRAINT full_load_checkpoint_phase_chk
                CHECK (phase = ANY (ARRAY['truncate'::text, 'ddl'::text, 'copy'::text]))
        );
        CREATE INDEX IF NOT EXISTS full_load_checkpoint_updated_idx
            ON cdc_catalog.full_load_checkpoint (updated_at DESC);

        CREATE TABLE IF NOT EXISTS cdc_catalog.reconciliation (
            reconciliation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            batch_id text NOT NULL,
            conn_id text NOT NULL,
            catalog_id bigint NOT NULL REFERENCES cdc_catalog.catalog(catalog_id) ON DELETE CASCADE,
            db_engine cdc_catalog.db_engine NOT NULL,
            source_schema text NOT NULL,
            source_table text NOT NULL,
            checked_at timestamptz NOT NULL DEFAULT now(),
            status text NOT NULL DEFAULT 'ok',
            row_count_source bigint,
            row_count_lake bigint,
            row_count_delta bigint,
            row_count_status text NOT NULL DEFAULT 'skip',
            max_pk_source text,
            max_pk_lake text,
            max_pk_status text NOT NULL DEFAULT 'skip',
            ts_column text,
            max_ts_source timestamptz,
            max_ts_lake timestamptz,
            max_ts_lag_seconds integer,
            max_ts_status text NOT NULL DEFAULT 'skip',
            pipeline_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
            checks jsonb NOT NULL DEFAULT '{}'::jsonb,
            duration_ms bigint NOT NULL DEFAULT 0,
            error text,
            CONSTRAINT reconciliation_status_chk
                CHECK (status = ANY (ARRAY['ok','warn','fail','skip']::text[])),
            CONSTRAINT reconciliation_row_count_status_chk
                CHECK (row_count_status = ANY (ARRAY['ok','warn','fail','skip']::text[])),
            CONSTRAINT reconciliation_max_pk_status_chk
                CHECK (max_pk_status = ANY (ARRAY['ok','warn','fail','skip']::text[])),
            CONSTRAINT reconciliation_max_ts_status_chk
                CHECK (max_ts_status = ANY (ARRAY['ok','warn','fail','skip']::text[]))
        );
        CREATE INDEX IF NOT EXISTS reconciliation_conn_checked_idx
            ON cdc_catalog.reconciliation (conn_id, checked_at DESC);
        CREATE INDEX IF NOT EXISTS reconciliation_catalog_checked_idx
            ON cdc_catalog.reconciliation (catalog_id, checked_at DESC);
        CREATE INDEX IF NOT EXISTS reconciliation_status_checked_idx
            ON cdc_catalog.reconciliation (status, checked_at DESC)
            WHERE status IN ('fail', 'warn');

        CREATE OR REPLACE VIEW cdc_catalog.v_reconciliation_latest AS
        SELECT DISTINCT ON (catalog_id) *
        FROM cdc_catalog.reconciliation
        ORDER BY catalog_id, checked_at DESC;

        CREATE OR REPLACE FUNCTION cdc_catalog.prune_reconciliation(p_retention_days integer DEFAULT 30)
        RETURNS bigint
        LANGUAGE sql
        AS $prune_reconciliation$
            WITH deleted AS (
                DELETE FROM cdc_catalog.reconciliation
                WHERE p_retention_days > 0
                  AND checked_at < now() - make_interval(days => p_retention_days)
                RETURNING 1
            )
            SELECT count(*)::bigint FROM deleted;
        $prune_reconciliation$;

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (53, 'repair catalog objects 050-052 if missing');
        RAISE NOTICE 'migration 053: catalog schema repair';
    END IF;
END $migration053$;

-- Migration 054: cold-path apply workers 8 -> 12 (24 kafka partitions / 12 workers / 3 hot).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 54) THEN
        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES
            ('apply_worker_count', 'cdc_kafka_apply', '', '12'::jsonb,
             'Cold-path apply worker threads per connection (must divide kafka_topic_partitions evenly)')
        ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
            config_value = EXCLUDED.config_value,
            description  = EXCLUDED.description,
            updated_at   = now();

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (54, 'apply throughput: cold-path apply_worker_count 8 -> 12');
        RAISE NOTICE 'migration 054: apply_worker_count -> 12';
    END IF;
END $$;

-- Migration 055: batched retention prune (daemon 03:00 CST); workers no longer run monolithic DELETE at startup.
DO $migration055$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 55) THEN
        CREATE OR REPLACE FUNCTION cdc_catalog.prune_apply_batch_stats_batched(
            p_retention_days integer DEFAULT 30,
            p_batch_size integer DEFAULT 5000,
            p_max_batches integer DEFAULT 500
        ) RETURNS bigint
        LANGUAGE plpgsql
        AS $prune_abs_batched$
        DECLARE
            total_deleted bigint := 0;
            batch_deleted bigint;
            batches_done integer := 0;
            cutoff timestamptz;
        BEGIN
            IF p_retention_days IS NULL OR p_retention_days < 1
               OR p_batch_size IS NULL OR p_batch_size < 1
               OR p_max_batches IS NULL OR p_max_batches < 1 THEN
                RETURN 0;
            END IF;
            cutoff := now() - make_interval(days => p_retention_days);
            LOOP
                WITH doomed AS (
                    SELECT stat_id
                    FROM cdc_catalog.apply_batch_stats
                    WHERE logged_at < cutoff
                    ORDER BY stat_id
                    LIMIT p_batch_size
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM cdc_catalog.apply_batch_stats t
                USING doomed d
                WHERE t.stat_id = d.stat_id;
                GET DIAGNOSTICS batch_deleted = ROW_COUNT;
                total_deleted := total_deleted + batch_deleted;
                batches_done := batches_done + 1;
                EXIT WHEN batch_deleted = 0 OR batches_done >= p_max_batches;
            END LOOP;
            RETURN total_deleted;
        END;
        $prune_abs_batched$;

        CREATE OR REPLACE FUNCTION cdc_catalog.prune_applied_events_batched(
            p_retention_days integer DEFAULT 7,
            p_batch_size integer DEFAULT 5000,
            p_max_batches integer DEFAULT 200
        ) RETURNS bigint
        LANGUAGE plpgsql
        AS $prune_ae_batched$
        DECLARE
            total_deleted bigint := 0;
            batch_deleted bigint;
            batches_done integer := 0;
            cutoff timestamptz;
        BEGIN
            IF p_retention_days IS NULL OR p_retention_days < 1
               OR p_batch_size IS NULL OR p_batch_size < 1
               OR p_max_batches IS NULL OR p_max_batches < 1 THEN
                RETURN 0;
            END IF;
            cutoff := now() - make_interval(days => p_retention_days);
            LOOP
                WITH doomed AS (
                    SELECT event_id
                    FROM cdc_catalog.cdc_applied_events
                    WHERE applied_at < cutoff
                    ORDER BY event_id
                    LIMIT p_batch_size
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM cdc_catalog.cdc_applied_events t
                USING doomed d
                WHERE t.event_id = d.event_id;
                GET DIAGNOSTICS batch_deleted = ROW_COUNT;
                total_deleted := total_deleted + batch_deleted;
                batches_done := batches_done + 1;
                EXIT WHEN batch_deleted = 0 OR batches_done >= p_max_batches;
            END LOOP;
            RETURN total_deleted;
        END;
        $prune_ae_batched$;

        COMMENT ON FUNCTION cdc_catalog.prune_apply_batch_stats_batched(integer, integer, integer) IS
            'Batched delete of apply_batch_stats older than retention; used by daemon maintenance window only.';
        COMMENT ON FUNCTION cdc_catalog.prune_applied_events_batched(integer, integer, integer) IS
            'Batched delete of cdc_applied_events older than retention; used by daemon maintenance window only.';

        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES
            ('apply_batch_stats_retention_days', 'global', '', '30'::jsonb,
             'Retention days for apply_batch_stats batched prune'),
            ('apply_batch_stats_prune_batch_size', 'global', '', '5000'::jsonb,
             'Rows per DELETE batch for apply_batch_stats prune'),
            ('apply_batch_stats_prune_max_batches', 'global', '', '500'::jsonb,
             'Max DELETE batches per nightly apply_batch_stats prune run'),
            ('applied_events_prune_batch_size', 'global', '', '5000'::jsonb,
             'Rows per DELETE batch for cdc_applied_events prune'),
            ('applied_events_prune_max_batches', 'global', '', '200'::jsonb,
             'Max DELETE batches per nightly cdc_applied_events prune run'),
            ('retention_maintenance_last_run_date', 'global', '', '""'::jsonb,
             'Last America/Costa_Rica date batched retention prune completed (YYYY-MM-DD)')
        ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
            config_value = EXCLUDED.config_value,
            description  = EXCLUDED.description,
            updated_at   = now();

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (55, 'batched retention prune functions; remove worker startup DELETE storm');
        RAISE NOTICE 'migration 055: batched retention prune';
    END IF;
END $migration055$;

-- Migration 056: stop per-restart runtime_config wipe; seed tunable keys once (preserve operator overrides).
DO $migration056$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 56) THEN
        DELETE FROM cdc_catalog.runtime_config
        WHERE component = 'cdc_kafka_health'
           OR (config_key, component) IN (
                ('debezium_topic_prefix', 'cdc_kafka_apply'),
                ('debezium_connect_url', 'cdc_kafka_health'),
                ('debezium_connector_name', 'cdc_kafka_health'),
                ('capture_topic_prefix', 'cdc_kafka_capture'),
                ('kafka_topic_prefix', 'cdc_kafka_apply'),
                ('capture_binlog_mode', 'cdc_kafka_capture')
            )
           OR (config_key = 'cdc_apply_batch_size' AND component = 'mariadb_cdc')
           OR (config_key, component) IN (
                ('apply_max_seconds', 'cdc_kafka_apply'),
                ('capture_max_seconds', 'cdc_kafka_capture'),
                ('apply_append_only', 'cdc_kafka_apply'),
                ('apply_exit_on_targets_met', 'cdc_kafka_apply'),
                ('apply_empty_poll_quiet_threshold', 'cdc_kafka_apply'),
                ('apply_catchup_enabled', 'cdc_kafka_apply'),
                ('apply_catchup_kafka_messages', 'cdc_kafka_apply'),
                ('apply_catchup_lag_seconds', 'cdc_kafka_apply'),
                ('apply_catchup_max_tables', 'cdc_kafka_apply'),
                ('apply_catchup_min_kafka_messages', 'cdc_kafka_apply'),
                ('apply_process_rss_cap_mb', 'cdc_kafka_apply'),
                ('full_load_parallel_tables', 'mariadb_load'),
                ('kafka_purge_consumed_enabled', 'cdc_kafka_apply'),
                ('kafka_purge_consumed_interval_seconds', 'cdc_kafka_apply'),
                ('kafka_purge_consumed_max_lag', 'cdc_kafka_apply'),
                ('kafka_purge_consumed_min_deletable_offsets', 'cdc_kafka_apply'),
                ('kafka_topic_partitions', 'cdc_kafka_capture'),
                ('kafka_topic_partitions', 'cdc_kafka_apply')
            );

        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES
            ('full_load_batch_size', 'mariadb_load', '', '50000'::jsonb,
             'MariaDB full-load COPY batch size'),
            ('apply_batch_size', 'cdc_kafka_apply', '', '20000'::jsonb,
             'Apply batch before flush'),
            ('logs_retention_days', 'global', '', '7'::jsonb,
             'Purge cdc_catalog.logs retention'),
            ('applied_events_retention_days', 'cdc_kafka_apply', '', '7'::jsonb,
             'Dedup audit retention'),
            ('apply_worker_count', 'cdc_kafka_apply', '', '12'::jsonb,
             'Cold-path apply worker threads per connection (must divide kafka_topic_partitions evenly)'),
            ('apply_batch_stats_retention_days', 'global', '', '30'::jsonb,
             'Retention days for apply_batch_stats batched prune'),
            ('apply_batch_stats_prune_batch_size', 'global', '', '5000'::jsonb,
             'Rows per DELETE batch for apply_batch_stats prune'),
            ('apply_batch_stats_prune_max_batches', 'global', '', '500'::jsonb,
             'Max DELETE batches per nightly apply_batch_stats prune run'),
            ('applied_events_prune_batch_size', 'global', '', '5000'::jsonb,
             'Rows per DELETE batch for cdc_applied_events prune'),
            ('applied_events_prune_max_batches', 'global', '', '200'::jsonb,
             'Max DELETE batches per nightly cdc_applied_events prune run'),
            ('retention_maintenance_last_run_date', 'global', '', '""'::jsonb,
             'Last America/Costa_Rica date batched retention prune completed (YYYY-MM-DD)')
        ON CONFLICT (config_key, component, conn_id) DO NOTHING;

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (56, 'runtime_config seeds one-time; apply_worker_count + retention keys persist across restarts');
        RAISE NOTICE 'migration 056: runtime_config repair (apply_worker_count, retention keys)';
    END IF;
END $migration056$;

-- Migration 057: serialize + batch cdc_catalog.logs purge (stop worker-startup DELETE stampede).
DO $migration057$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 57) THEN
        CREATE OR REPLACE FUNCTION cdc_catalog.purge_logs_batched(
            p_retention_days integer DEFAULT 7,
            p_batch_size integer DEFAULT 5000,
            p_max_batches integer DEFAULT 500
        ) RETURNS bigint
        LANGUAGE plpgsql
        AS $purge_logs_batched$
        DECLARE
            total_deleted bigint := 0;
            batch_deleted bigint;
            batches_done integer := 0;
            cutoff timestamptz;
        BEGIN
            IF p_retention_days IS NULL OR p_retention_days < 1
               OR p_batch_size IS NULL OR p_batch_size < 1
               OR p_max_batches IS NULL OR p_max_batches < 1 THEN
                RETURN 0;
            END IF;
            IF NOT pg_try_advisory_xact_lock(90420057001) THEN
                RETURN 0;
            END IF;
            cutoff := now() - make_interval(days => p_retention_days);
            LOOP
                WITH doomed AS (
                    SELECT log_id
                    FROM cdc_catalog.logs
                    WHERE logged_at < cutoff
                    ORDER BY log_id
                    LIMIT p_batch_size
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM cdc_catalog.logs t
                USING doomed d
                WHERE t.log_id = d.log_id;
                GET DIAGNOSTICS batch_deleted = ROW_COUNT;
                total_deleted := total_deleted + batch_deleted;
                batches_done := batches_done + 1;
                EXIT WHEN batch_deleted = 0 OR batches_done >= p_max_batches;
            END LOOP;
            RETURN total_deleted;
        END;
        $purge_logs_batched$;

        CREATE OR REPLACE FUNCTION cdc_catalog.purge_logs(p_retention_days integer DEFAULT 7)
        RETURNS bigint
        LANGUAGE sql
        AS $purge_logs$
            SELECT cdc_catalog.purge_logs_batched(p_retention_days, 5000, 500);
        $purge_logs$;

        COMMENT ON FUNCTION cdc_catalog.purge_logs_batched(integer, integer, integer) IS
            'Batched delete of cdc_catalog.logs older than retention; advisory-locked; daemon maintenance window.';
        COMMENT ON FUNCTION cdc_catalog.purge_logs(integer) IS
            'Compatibility wrapper: batched+locked purge of observability logs (append-only table).';

        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES
            ('logs_purge_batch_size', 'global', '', '5000'::jsonb,
             'Rows per DELETE batch for cdc_catalog.logs purge'),
            ('logs_purge_max_batches', 'global', '', '500'::jsonb,
             'Max DELETE batches per nightly cdc_catalog.logs purge run')
        ON CONFLICT (config_key, component, conn_id) DO NOTHING;

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (57, 'batched+locked purge_logs; stop worker startup logs DELETE stampede');
        RAISE NOTICE 'migration 057: batched locked purge_logs';
    END IF;
END $migration057$;

-- Migration 058: capture_position last_failed_source_* for alerting and ops triage.
DO $migration058$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 58) THEN
        ALTER TABLE cdc_catalog.capture_position
            ADD COLUMN IF NOT EXISTS last_failed_source_schema text,
            ADD COLUMN IF NOT EXISTS last_failed_source_table text;

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (58, 'capture_position last_failed_source_schema/table for capture failure triage');
        RAISE NOTICE 'migration 058: capture_position last_failed_source_* columns';
    END IF;
END $migration058$;

-- Migration 059: capture_during_full_load (snapshot + concurrent stream during full load).
DO $migration059$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 59) THEN
        ALTER TABLE cdc_catalog.catalog
            ADD COLUMN IF NOT EXISTS capture_during_full_load boolean NOT NULL DEFAULT false;

        COMMENT ON COLUMN cdc_catalog.catalog.capture_during_full_load IS
            'When true with needs_full_load: capture publishes to Kafka during COPY; apply waits then replays from stream bookmark. When false: capture pauses during COPY; on FL complete apply offsets jump to Kafka high watermark (skip pre-FL backlog).';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (59, 'catalog.capture_during_full_load for concurrent CDC during full load');
        RAISE NOTICE 'migration 059: capture_during_full_load column';
    END IF;
END $migration059$;

-- Migration 060: smaller retention prune batches (daemon commits one batch per call).
DO $migration060$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 60) THEN
        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES
            ('apply_batch_stats_prune_batch_size', 'global', '', '1000'::jsonb,
             'Rows per DELETE batch for apply_batch_stats prune (daemon commits per batch)'),
            ('apply_batch_stats_prune_max_batches', 'global', '', '500'::jsonb,
             'Max DELETE batches per nightly apply_batch_stats prune run'),
            ('applied_events_prune_batch_size', 'global', '', '1000'::jsonb,
             'Rows per DELETE batch for cdc_applied_events prune (daemon commits per batch)'),
            ('applied_events_prune_max_batches', 'global', '', '500'::jsonb,
             'Max DELETE batches per nightly cdc_applied_events prune run'),
            ('logs_purge_batch_size', 'global', '', '1000'::jsonb,
             'Rows per DELETE batch for cdc_catalog.logs purge (daemon commits per batch)'),
            ('logs_purge_max_batches', 'global', '', '500'::jsonb,
             'Max DELETE batches per nightly logs purge run')
        ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
            config_value = EXCLUDED.config_value,
            description  = EXCLUDED.description,
            updated_at   = now();

        COMMENT ON FUNCTION cdc_catalog.prune_applied_events_batched(integer, integer, integer) IS
            'Batched delete of cdc_applied_events; call with p_max_batches=1 from daemon so each batch commits separately.';

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (60, 'smaller retention prune batch defaults; commit-per-batch from daemon');
        RAISE NOTICE 'migration 060: retention prune smaller batches';
    END IF;
END $migration060$;

-- Migration 061: hourly apply_batch_stats view for BI (avoids full-table SUM scans in Superset).
DO $migration061$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 61) THEN
        CREATE OR REPLACE VIEW cdc_catalog.v_apply_batch_stats_hourly AS
        SELECT
            date_trunc('hour', logged_at) AS logged_at,
            COALESCE(SUM(events_total), 0)::bigint AS events_total,
            COALESCE(SUM(events_inserts), 0)::bigint AS events_inserts,
            COALESCE(SUM(events_updates), 0)::bigint AS events_updates,
            COALESCE(SUM(events_deletes), 0)::bigint AS events_deletes,
            COALESCE(SUM(parse_skipped), 0)::bigint AS parse_skipped,
            COALESCE(SUM(dropped_unrecoverable), 0)::bigint AS dropped_unrecoverable,
            COALESCE(SUM(dedup_skipped), 0)::bigint AS dedup_skipped,
            COUNT(*)::bigint AS slice_rows
        FROM cdc_catalog.apply_batch_stats
        GROUP BY 1;

        COMMENT ON VIEW cdc_catalog.v_apply_batch_stats_hourly IS
            'Hourly rollup of apply_batch_stats for dashboards; prefer over raw SUM(date_trunc(...)) on the base table.';

        CREATE INDEX IF NOT EXISTS apply_batch_stats_logged_at_stat_id_idx
            ON cdc_catalog.apply_batch_stats USING btree (logged_at, stat_id);

        -- Prefer logged_at range + SKIP LOCKED over sorting by stat_id alone on large heaps.
        CREATE OR REPLACE FUNCTION cdc_catalog.prune_apply_batch_stats_batched(
            p_retention_days integer DEFAULT 30,
            p_batch_size integer DEFAULT 5000,
            p_max_batches integer DEFAULT 500
        ) RETURNS bigint
        LANGUAGE plpgsql
        AS $prune_abs_batched$
        DECLARE
            total_deleted bigint := 0;
            batch_deleted bigint;
            batches_done integer := 0;
            cutoff timestamptz;
        BEGIN
            IF p_retention_days IS NULL OR p_retention_days < 1
               OR p_batch_size IS NULL OR p_batch_size < 1
               OR p_max_batches IS NULL OR p_max_batches < 1 THEN
                RETURN 0;
            END IF;
            cutoff := now() - make_interval(days => p_retention_days);
            LOOP
                WITH doomed AS (
                    SELECT stat_id
                    FROM cdc_catalog.apply_batch_stats
                    WHERE logged_at < cutoff
                    ORDER BY logged_at, stat_id
                    LIMIT p_batch_size
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM cdc_catalog.apply_batch_stats t
                USING doomed d
                WHERE t.stat_id = d.stat_id;
                GET DIAGNOSTICS batch_deleted = ROW_COUNT;
                total_deleted := total_deleted + batch_deleted;
                batches_done := batches_done + 1;
                EXIT WHEN batch_deleted = 0 OR batches_done >= p_max_batches;
            END LOOP;
            RETURN total_deleted;
        END;
        $prune_abs_batched$;

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (61, 'v_apply_batch_stats_hourly + (logged_at,stat_id) index + prune ORDER BY logged_at');
        RAISE NOTICE 'migration 061: hourly apply_batch_stats view + prune index';
    END IF;
END $migration061$;

-- Migration 062: retention 3d everywhere + small batches + prune/catalog indexes.
DO $migration062$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 62) THEN
        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES
            ('apply_batch_stats_retention_days', 'global', '', '3'::jsonb,
             'Retention days for apply_batch_stats (daemon prune 03:00 CST)'),
            ('applied_events_retention_days', 'cdc_kafka_apply', '', '3'::jsonb,
             'Retention days for cdc_applied_events dedup ledger'),
            ('logs_retention_days', 'global', '', '3'::jsonb,
             'Retention days for cdc_catalog.logs'),
            ('apply_batch_stats_prune_batch_size', 'global', '', '500'::jsonb,
             'Rows per DELETE batch for apply_batch_stats prune'),
            ('apply_batch_stats_prune_max_batches', 'global', '', '10000'::jsonb,
             'Max DELETE batches per nightly apply_batch_stats prune run'),
            ('applied_events_prune_batch_size', 'global', '', '500'::jsonb,
             'Rows per DELETE batch for cdc_applied_events prune'),
            ('applied_events_prune_max_batches', 'global', '', '10000'::jsonb,
             'Max DELETE batches per nightly cdc_applied_events prune run'),
            ('logs_purge_batch_size', 'global', '', '500'::jsonb,
             'Rows per DELETE batch for cdc_catalog.logs purge'),
            ('logs_purge_max_batches', 'global', '', '10000'::jsonb,
             'Max DELETE batches per nightly logs purge run')
        ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
            config_value = EXCLUDED.config_value,
            description  = EXCLUDED.description,
            updated_at   = now();

        CREATE INDEX IF NOT EXISTS apply_batch_stats_logged_at_brin
            ON cdc_catalog.apply_batch_stats USING brin (logged_at);
        CREATE INDEX IF NOT EXISTS cdc_applied_events_applied_at_brin
            ON cdc_catalog.cdc_applied_events USING brin (applied_at);
        CREATE INDEX IF NOT EXISTS logs_logged_at_brin
            ON cdc_catalog.logs USING brin (logged_at);

        CREATE INDEX IF NOT EXISTS catalog_conn_engine_active_idx
            ON cdc_catalog.catalog (conn_id, db_engine)
            WHERE active = true;
        CREATE INDEX IF NOT EXISTS catalog_capture_during_full_load_idx
            ON cdc_catalog.catalog (conn_id, db_engine)
            WHERE capture_during_full_load = true
              AND needs_full_load = false
              AND cdc_enabled = false;
        CREATE INDEX IF NOT EXISTS catalog_status_updated_idx
            ON cdc_catalog.catalog (status, updated_at DESC);

        CREATE INDEX IF NOT EXISTS apply_position_conn_status_idx
            ON cdc_catalog.apply_position (conn_id, status);

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (62, 'retention 3d + batch 500/max 10k; BRIN + catalog/apply_position indexes');
        RAISE NOTICE 'migration 062: retention 3d + indexes';
    END IF;
END $migration062$;

-- Migration 063: drop unused apply_outbox (lake-first audit retry never used operationally).
DO $migration063$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 63) THEN
        DROP FUNCTION IF EXISTS cdc_catalog.prune_apply_outbox_batched(integer, integer, integer);
        DROP TABLE IF EXISTS cdc_catalog.apply_outbox CASCADE;

        DELETE FROM cdc_catalog.runtime_config
        WHERE config_key IN (
            'apply_outbox_retention_days',
            'apply_outbox_prune_batch_size',
            'apply_outbox_prune_max_batches'
        );

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (63, 'drop unused apply_outbox table and related prune/runtime keys');
        RAISE NOTICE 'migration 063: apply_outbox dropped';
    END IF;
END $migration063$;

-- Migration 064: fix applied_events prune ORDER BY time; raise prune throughput; FILLFACTOR
-- on hot UPDATE tables. btree (applied_at, event_id) is created CONCURRENTLY via ops SQL
-- (not here — 388GB+ table must not take ACCESS EXCLUSIVE in migrate).
DO $migration064$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 64) THEN
        CREATE OR REPLACE FUNCTION cdc_catalog.prune_applied_events_batched(
            p_retention_days integer DEFAULT 3,
            p_batch_size integer DEFAULT 5000,
            p_max_batches integer DEFAULT 50000
        ) RETURNS bigint
        LANGUAGE plpgsql
        AS $prune_ae_batched$
        DECLARE
            total_deleted bigint := 0;
            batch_deleted bigint;
            batches_done integer := 0;
            cutoff timestamptz;
        BEGIN
            IF p_retention_days IS NULL OR p_retention_days < 1
               OR p_batch_size IS NULL OR p_batch_size < 1
               OR p_max_batches IS NULL OR p_max_batches < 1 THEN
                RETURN 0;
            END IF;
            cutoff := now() - make_interval(days => p_retention_days);
            LOOP
                WITH doomed AS (
                    SELECT event_id
                    FROM cdc_catalog.cdc_applied_events
                    WHERE applied_at < cutoff
                    ORDER BY applied_at, event_id
                    LIMIT p_batch_size
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM cdc_catalog.cdc_applied_events t
                USING doomed d
                WHERE t.event_id = d.event_id;
                GET DIAGNOSTICS batch_deleted = ROW_COUNT;
                total_deleted := total_deleted + batch_deleted;
                batches_done := batches_done + 1;
                EXIT WHEN batch_deleted = 0 OR batches_done >= p_max_batches;
            END LOOP;
            RETURN total_deleted;
        END;
        $prune_ae_batched$;

        COMMENT ON FUNCTION cdc_catalog.prune_applied_events_batched(integer, integer, integer) IS
            'Batched delete by applied_at (not event_id text PK); daemon calls with p_max_batches=1 per commit.';

        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES
            ('applied_events_prune_batch_size', 'global', '', '5000'::jsonb,
             'Rows per DELETE batch for cdc_applied_events prune (daemon commits per batch)'),
            ('applied_events_prune_max_batches', 'global', '', '50000'::jsonb,
             'Max DELETE batches per nightly cdc_applied_events prune run (~250M rows/night max)')
        ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
            config_value = EXCLUDED.config_value,
            description  = EXCLUDED.description,
            updated_at   = now();

        -- Future page splits leave free space for HOT UPDATEs (existing pages need REINDEX).
        ALTER TABLE cdc_catalog.apply_position SET (fillfactor = 70);
        ALTER TABLE cdc_catalog.catalog SET (fillfactor = 80);

        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (64, 'prune applied_events by applied_at; raise prune throughput; fillfactor apply_position/catalog');
        RAISE NOTICE 'migration 064: applied_events prune fix + fillfactor';
    END IF;
END $migration064$;
)PO_schema_patch";
    }
    /** Idempotent DDL for 050/051/052 — safe when schema_migrations version rows exist without objects. */
    inline std::string_view catalog_schema_repair() {
        return R"PO_cat_repair(
CREATE TABLE IF NOT EXISTS cdc_catalog.full_load_checkpoint (
    catalog_id bigint NOT NULL REFERENCES cdc_catalog.catalog(catalog_id) ON DELETE CASCADE,
    worker_id integer NOT NULL DEFAULT 0,
    batch_id text NOT NULL,
    phase text NOT NULL,
    last_pk jsonb,
    rows_loaded bigint NOT NULL DEFAULT 0,
    source_rows bigint,
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (catalog_id, worker_id),
    CONSTRAINT full_load_checkpoint_phase_chk
        CHECK (phase = ANY (ARRAY['truncate'::text, 'ddl'::text, 'copy'::text]))
);
CREATE INDEX IF NOT EXISTS full_load_checkpoint_updated_idx
    ON cdc_catalog.full_load_checkpoint (updated_at DESC);

CREATE TABLE IF NOT EXISTS cdc_catalog.reconciliation (
    reconciliation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id text NOT NULL,
    conn_id text NOT NULL,
    catalog_id bigint NOT NULL REFERENCES cdc_catalog.catalog(catalog_id) ON DELETE CASCADE,
    db_engine cdc_catalog.db_engine NOT NULL,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now(),
    status text NOT NULL DEFAULT 'ok',
    row_count_source bigint,
    row_count_lake bigint,
    row_count_delta bigint,
    row_count_status text NOT NULL DEFAULT 'skip',
    max_pk_source text,
    max_pk_lake text,
    max_pk_status text NOT NULL DEFAULT 'skip',
    ts_column text,
    max_ts_source timestamptz,
    max_ts_lake timestamptz,
    max_ts_lag_seconds integer,
    max_ts_status text NOT NULL DEFAULT 'skip',
    pipeline_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    checks jsonb NOT NULL DEFAULT '{}'::jsonb,
    duration_ms bigint NOT NULL DEFAULT 0,
    error text,
    CONSTRAINT reconciliation_status_chk
        CHECK (status = ANY (ARRAY['ok','warn','fail','skip']::text[])),
    CONSTRAINT reconciliation_row_count_status_chk
        CHECK (row_count_status = ANY (ARRAY['ok','warn','fail','skip']::text[])),
    CONSTRAINT reconciliation_max_pk_status_chk
        CHECK (max_pk_status = ANY (ARRAY['ok','warn','fail','skip']::text[])),
    CONSTRAINT reconciliation_max_ts_status_chk
        CHECK (max_ts_status = ANY (ARRAY['ok','warn','fail','skip']::text[]))
);
CREATE INDEX IF NOT EXISTS reconciliation_conn_checked_idx
    ON cdc_catalog.reconciliation (conn_id, checked_at DESC);
CREATE INDEX IF NOT EXISTS reconciliation_catalog_checked_idx
    ON cdc_catalog.reconciliation (catalog_id, checked_at DESC);
CREATE INDEX IF NOT EXISTS reconciliation_status_checked_idx
    ON cdc_catalog.reconciliation (status, checked_at DESC)
    WHERE status IN ('fail', 'warn');

CREATE OR REPLACE VIEW cdc_catalog.v_reconciliation_latest AS
SELECT DISTINCT ON (catalog_id) *
FROM cdc_catalog.reconciliation
ORDER BY catalog_id, checked_at DESC;

CREATE OR REPLACE FUNCTION cdc_catalog.prune_reconciliation(p_retention_days integer DEFAULT 30)
RETURNS bigint
LANGUAGE sql
AS $prune_reconciliation$
    WITH deleted AS (
        DELETE FROM cdc_catalog.reconciliation
        WHERE p_retention_days > 0
          AND checked_at < now() - make_interval(days => p_retention_days)
        RETURNING 1
    )
    SELECT count(*)::bigint FROM deleted;
$prune_reconciliation$;
)PO_cat_repair";
    }
    inline std::string_view monitoring_views() {
        return R"PO_monitoring_v(
-- Ensure apply_health_rag column exists before (re)creating views.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'cdc_catalog'
          AND table_name = 'apply_batch_stats'
          AND column_name = 'reconciliation_rag'
    ) THEN
        ALTER TABLE cdc_catalog.apply_batch_stats DROP COLUMN IF EXISTS semaphore;
        ALTER TABLE cdc_catalog.apply_batch_stats
            RENAME COLUMN reconciliation_rag TO apply_health_rag;
        ALTER TABLE cdc_catalog.apply_batch_stats
            DROP CONSTRAINT IF EXISTS apply_batch_stats_reconciliation_rag_chk;
        ALTER TABLE cdc_catalog.apply_batch_stats
            ADD CONSTRAINT apply_batch_stats_apply_health_rag_chk
            CHECK (apply_health_rag = ANY (ARRAY['GREEN','AMBER','RED','UNKNOWN']::text[]));
        ALTER TABLE cdc_catalog.apply_batch_stats
            ADD COLUMN IF NOT EXISTS semaphore text GENERATED ALWAYS AS (apply_health_rag) STORED;
        DROP INDEX IF EXISTS cdc_catalog.apply_batch_stats_rag_idx;
        CREATE INDEX IF NOT EXISTS apply_batch_stats_health_rag_idx
            ON cdc_catalog.apply_batch_stats (conn_id, apply_health_rag, logged_at DESC);
    END IF;
END $$;

-- Drop dependent views first (leaf views before v_apply_latest).
DROP VIEW IF EXISTS cdc_catalog.v_kafka_consumer;
DROP VIEW IF EXISTS cdc_catalog.v_cdc_pipeline_summary;
DROP VIEW IF EXISTS cdc_catalog.v_apply_stale;
DROP VIEW IF EXISTS cdc_catalog.v_apply_latest;
DROP VIEW IF EXISTS cdc_catalog.v_full_load_progress;

CREATE VIEW cdc_catalog.v_apply_latest AS
SELECT DISTINCT ON (conn_id, source_schema, source_table)
    stat_id,
    batch_id,
    conn_id,
    catalog_id,
    source_schema,
    source_table,
    events_total,
    events_updates,
    events_inserts,
    events_deletes,
    duration_ms,
    events_per_minute,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_consumer_lag,
    kafka_partition_lag,
    lag_scan_complete,
    apply_lag_seconds,
    seconds_since_last_apply,
    is_stale,
    is_inactive,
    apply_health_rag,
    health_reason,
    event_loss_status,
    slice_kind,
    dedup_skipped,
    parse_skipped,
    dropped_unrecoverable,
    capture_lag_seconds,
    logged_at
FROM cdc_catalog.apply_batch_stats
ORDER BY conn_id, source_schema, source_table, logged_at DESC;

CREATE VIEW cdc_catalog.v_apply_stale AS
SELECT
    ap.catalog_id,
    ap.conn_id,
    ap.source_schema,
    ap.source_table,
    ap.last_applied_at,
    ap.apply_lag_seconds,
    ap.status,
    ap.quarantined_at,
    ap.last_error,
    (now() - ap.last_applied_at) AS lag_interval,
    latest.is_stale,
    latest.seconds_since_last_apply,
    latest.apply_health_rag,
    latest.health_reason,
    latest.kafka_consumer_lag
FROM cdc_catalog.apply_position ap
JOIN cdc_catalog.catalog c USING (catalog_id)
LEFT JOIN cdc_catalog.v_apply_latest latest
    ON latest.conn_id = ap.conn_id
   AND latest.source_schema = ap.source_schema
   AND latest.source_table = ap.source_table
WHERE c.active = true
  AND c.cdc_enabled = true
  AND c.needs_full_load = false
  AND (
      latest.is_stale = true
      OR latest.apply_health_rag IN ('AMBER', 'RED')
      OR latest.event_loss_status = 'fail'
      OR ap.status = ANY (ARRAY['stale','lagging','gap_detected','quarantined','failed']::cdc_catalog.cdc_health_status[])
  );

CREATE VIEW cdc_catalog.v_cdc_pipeline_summary AS
SELECT
    c.conn_id,
    c.db_engine,
    count(*) FILTER (WHERE c.active AND c.cdc_enabled AND NOT c.needs_full_load) AS cdc_ready,
    count(*) FILTER (WHERE latest.apply_health_rag = 'GREEN') AS apply_green,
    count(*) FILTER (WHERE latest.apply_health_rag = 'AMBER') AS apply_amber,
    count(*) FILTER (WHERE latest.apply_health_rag = 'RED') AS apply_red,
    count(*) FILTER (WHERE latest.apply_health_rag = 'UNKNOWN') AS apply_unknown,
    count(*) FILTER (WHERE latest.is_stale) AS apply_stale,
    count(*) FILTER (WHERE latest.event_loss_status = 'fail') AS event_loss_fail,
    max(latest.kafka_consumer_lag) AS max_kafka_consumer_lag,
    max(latest.capture_lag_seconds) AS max_capture_lag_seconds
FROM cdc_catalog.catalog c
LEFT JOIN cdc_catalog.v_apply_latest latest
    ON latest.conn_id = c.conn_id
   AND latest.source_schema = c.source_schema
   AND latest.source_table = c.source_table
WHERE c.active = true
GROUP BY c.conn_id, c.db_engine;

CREATE VIEW cdc_catalog.v_full_load_progress AS
SELECT
    c.catalog_id,
    c.conn_id,
    c.db_engine,
    c.source_schema,
    c.source_table,
    c.status,
    c.needs_full_load,
    cp.worker_id,
    cp.batch_id AS checkpoint_batch_id,
    cp.phase AS checkpoint_phase,
    cp.rows_loaded AS checkpoint_rows_loaded,
    cp.source_rows AS checkpoint_source_rows,
    cp.last_pk AS checkpoint_last_pk,
    cp.updated_at AS checkpoint_updated_at
FROM cdc_catalog.catalog c
LEFT JOIN cdc_catalog.full_load_checkpoint cp ON cp.catalog_id = c.catalog_id
WHERE c.active = true
  AND (c.needs_full_load = true OR c.status = 'full_load_in_progress' OR cp.catalog_id IS NOT NULL);

CREATE VIEW cdc_catalog.v_kafka_consumer AS
SELECT
    ap.conn_id,
    ap.source_schema,
    ap.source_table,
    ap.kafka_topic,
    ap.kafka_partition,
    ap.kafka_offset AS consumed_offset,
    ap.last_applied_at,
    ap.apply_lag_seconds,
    ap.status AS apply_position_status,
    latest.kafka_consumer_lag,
    latest.kafka_partition_lag,
    latest.events_total AS last_slice_events,
    latest.logged_at AS last_apply_at,
    latest.is_inactive,
    latest.is_stale,
    latest.apply_health_rag,
    latest.health_reason,
    latest.event_loss_status,
    latest.slice_kind
FROM cdc_catalog.apply_position ap
JOIN cdc_catalog.catalog c ON c.catalog_id = ap.catalog_id
LEFT JOIN cdc_catalog.v_apply_latest latest
    ON latest.conn_id = ap.conn_id
   AND latest.source_schema = ap.source_schema
   AND latest.source_table = ap.source_table;

COMMENT ON VIEW cdc_catalog.v_apply_latest IS 'Latest apply_batch_stats row per table (replaces manual apply_batch_stats ORDER BY logged_at DESC)';
COMMENT ON VIEW cdc_catalog.v_apply_stale IS 'Tables with elevated apply lag, stale flag, or AMBER/RED health from apply_batch_stats';
COMMENT ON VIEW cdc_catalog.v_cdc_pipeline_summary IS 'Per conn_id + db_engine: RAG counts from v_apply_latest (all engines)';
COMMENT ON VIEW cdc_catalog.v_kafka_consumer IS 'Per-table apply offset + last-known Kafka consumer lag (no docker exec)';
)PO_monitoring_v";
    }
}  // namespace prod_ops_embedded
