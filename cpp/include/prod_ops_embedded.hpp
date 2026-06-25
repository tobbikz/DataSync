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

        IF to_regclass(format('%I.%I', p_schema, part_name)) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
                p_schema, part_name, p_schema, p_table,
                start_d::text || ' 00:00:00+00',
                end_d::text || ' 00:00:00+00'
            );
            created := created + 1;
        END IF;
        m := (m + interval '1 month')::date;
    END LOOP;
    RETURN created;
END;
$$;

COMMENT ON FUNCTION lake.ensure_monthly_partitions(text, text, integer) IS
    'Create monthly RANGE partitions on _dl_load_timestamp for a partitioned lake table.';
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

CREATE FUNCTION cdc_catalog.purge_logs(p_retention_days integer DEFAULT 7) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
    deleted bigint;
BEGIN
    IF p_retention_days IS NULL OR p_retention_days < 1 THEN
        RETURN 0;
    END IF;
    DELETE FROM cdc_catalog.logs
    WHERE logged_at < now() - make_interval(days => p_retention_days);
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END;
$$;


--
-- Name: FUNCTION purge_logs(p_retention_days integer); Type: COMMENT; Schema: cdc_catalog; Owner: -
--

COMMENT ON FUNCTION cdc_catalog.purge_logs(p_retention_days integer) IS 'Delete observability log rows older than retention window (append-only table).';


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
    reconciliation_rag text DEFAULT 'UNKNOWN'::text NOT NULL,
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
    semaphore text GENERATED ALWAYS AS (reconciliation_rag) STORED,
    CONSTRAINT apply_batch_stats_reconciliation_rag_chk CHECK ((reconciliation_rag = ANY (ARRAY['GREEN'::text, 'AMBER'::text, 'RED'::text, 'UNKNOWN'::text])))
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

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.reconciliation_rag IS 'Latest reconcile: ok→GREEN, warn→AMBER, fail→RED';


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

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.semaphore IS 'GREEN/AMBER/RED mirror of reconciliation_rag (latest reconcile)';


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

COMMENT ON TABLE cdc_catalog.reconciliation_result IS 'Per-table reconciliation: row counts (full mode), apply/capture/Kafka pipeline lag, optional PK sample checksum in checks JSON';


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
    reconcile_mode text DEFAULT 'full'::text NOT NULL,
    context jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT reconciliation_run_status_check CHECK ((status = ANY (ARRAY['running'::text, 'ok'::text, 'warn'::text, 'fail'::text]))),
    CONSTRAINT reconciliation_run_mode_check CHECK ((reconcile_mode = 'full'::text))
);


COMMENT ON TABLE cdc_catalog.reconciliation_run IS 'One row per reconcile CLI run (full: row count + PK checksum + lag).';


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
-- Name: reconciliation_result reconciliation_result_run_table_uk; Type: CONSTRAINT; Schema: cdc_catalog; Owner: -
--

ALTER TABLE ONLY cdc_catalog.reconciliation_result
    ADD CONSTRAINT reconciliation_result_run_table_uk UNIQUE (run_id, source_schema, source_table);


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

CREATE INDEX apply_batch_stats_rag_idx ON cdc_catalog.apply_batch_stats USING btree (conn_id, reconciliation_rag, logged_at DESC);


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
-- Snapshot + concurrent stream: capture to Kafka while full load runs.
ALTER TABLE cdc_catalog.catalog
    ADD COLUMN IF NOT EXISTS capture_during_full_load boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN cdc_catalog.catalog.capture_during_full_load IS
    'When true with needs_full_load: capture publishes to Kafka; apply waits until full load completes then replays from stream bookmark in engine_meta.';

-- Remove pipeline_health dashboard (unused). Observability: apply_batch_stats + logs.

DROP VIEW IF EXISTS cdc_catalog.v_pipeline_health;

DROP FUNCTION IF EXISTS cdc_catalog.refresh_pipeline_health_totals(
    cdc_catalog.service_tier, cdc_catalog.db_engine);

DROP FUNCTION IF EXISTS cdc_catalog.refresh_pipeline_health_live(
    text, cdc_catalog.service_tier, cdc_catalog.db_engine);

DROP TABLE IF EXISTS cdc_catalog.pipeline_health;

-- Migration 002: canonical runtime_config (5 global rows) + one-time reconcile history reset.
-- All other tuning: cpp/include/pipeline_defaults.hpp. Kafka bootstrap: KAFKA_BOOTSTRAP env.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 2) THEN
        TRUNCATE cdc_catalog.reconciliation_run RESTART IDENTITY CASCADE;
        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (2, 'canonical runtime_config; reconciliation_run history reset');
        RAISE NOTICE 'migration 002: reconciliation_run truncated';
    END IF;
END $$;

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
        ('apply_worker_count', 'cdc_kafka_apply'),
        ('apply_process_rss_cap_mb', 'cdc_kafka_apply'),
        ('full_load_parallel_tables', 'mariadb_load'),
        ('kafka_purge_consumed_enabled', 'cdc_kafka_apply'),
        ('kafka_purge_consumed_interval_seconds', 'cdc_kafka_apply'),
        ('kafka_purge_consumed_max_lag', 'cdc_kafka_apply'),
        ('kafka_purge_consumed_min_deletable_offsets', 'cdc_kafka_apply'),
        ('kafka_topic_partitions', 'cdc_kafka_capture'),
        ('kafka_topic_partitions', 'cdc_kafka_apply')
    );

DELETE FROM cdc_catalog.runtime_config
WHERE (config_key, component, COALESCE(conn_id, '')) NOT IN (
    ('full_load_batch_size',          'mariadb_load',        ''),
    ('apply_batch_size',              'cdc_kafka_apply',     ''),
    ('reconcile_interval_hours',      'cdc_kafka_reconcile', ''),
    ('logs_retention_days',           'global',              ''),
    ('applied_events_retention_days', 'cdc_kafka_apply',     '')
);

INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
VALUES
    ('full_load_batch_size',          'mariadb_load',        '', '50000'::jsonb, 'MariaDB full-load COPY batch size'),
    ('apply_batch_size',              'cdc_kafka_apply',     '', '20000'::jsonb, 'Apply batch before flush'),
    ('reconcile_interval_hours',      'cdc_kafka_reconcile', '', '4'::jsonb, 'Hours between reconcile-loop runs'),
    ('logs_retention_days',           'global',              '', '7'::jsonb, 'Purge cdc_catalog.logs retention'),
    ('applied_events_retention_days', 'cdc_kafka_apply',     '', '7'::jsonb, 'Dedup audit retention')
ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description  = EXCLUDED.description,
    updated_at   = now();

\echo '=== runtime_config canonical (expect 5 rows) ==='
SELECT COUNT(*) AS runtime_config_rows FROM cdc_catalog.runtime_config;
SELECT config_key, component, config_value
FROM cdc_catalog.runtime_config
ORDER BY component, config_key;
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
  AND created_at > now() - interval '6 hours'
ORDER BY created_at DESC
LIMIT 20;

-- Migration 003: Add reconcile_mode column to reconciliation_run for full vs light visibility.
ALTER TABLE cdc_catalog.reconciliation_run
    ADD COLUMN IF NOT EXISTS reconcile_mode text NOT NULL DEFAULT 'full';

ALTER TABLE cdc_catalog.reconciliation_run
    DROP CONSTRAINT IF EXISTS reconciliation_run_mode_check;

ALTER TABLE cdc_catalog.reconciliation_run
    ADD CONSTRAINT reconciliation_run_mode_check
        CHECK (reconcile_mode = 'full'::text);

COMMENT ON TABLE cdc_catalog.reconciliation_run IS 'One row per reconcile CLI run (full: row count + PK checksum + lag).';

-- Migration 004: apply observability columns on existing deployments.
ALTER TABLE cdc_catalog.apply_batch_stats
    ADD COLUMN IF NOT EXISTS parse_skipped bigint NOT NULL DEFAULT 0;

ALTER TABLE cdc_catalog.apply_batch_stats
    ADD COLUMN IF NOT EXISTS dropped_unrecoverable bigint NOT NULL DEFAULT 0;

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.parse_skipped IS
    'Kafka messages skipped due to JSON/payload parse failure in apply slice (per table batch)';

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.dropped_unrecoverable IS
    'Events dropped because lake schema/table could not be resolved (per table batch)';

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
        ALTER TABLE cdc_catalog.reconciliation_run DROP COLUMN IF EXISTS service_tier;
        ALTER TABLE cdc_catalog.reconciliation_result DROP COLUMN IF EXISTS service_tier;
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

-- Migration 036: reconcile full only (also applied via DataSync migrate + sql/036_*.sql).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = 36) THEN
        ALTER TABLE cdc_catalog.reconciliation_run
            DROP CONSTRAINT IF EXISTS reconciliation_run_mode_check;
        ALTER TABLE cdc_catalog.reconciliation_run
            ADD CONSTRAINT reconciliation_run_mode_check
            CHECK (reconcile_mode = 'full'::text);
        DELETE FROM cdc_catalog.runtime_config
        WHERE component = 'cdc_kafka_reconcile'
          AND config_key IN ('reconcile_mode', 'reconcile_full_interval_hours');
        UPDATE cdc_catalog.reconciliation_run
        SET reconcile_mode = 'full'
        WHERE reconcile_mode IS DISTINCT FROM 'full';
        INSERT INTO cdc_catalog.schema_migrations (version, description)
        VALUES (36, 'reconcile full only; drop light/auto runtime keys');
        RAISE NOTICE 'migration 036: reconcile full only';
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
                ('reconcile_interval_hours',      'cdc_kafka_reconcile', ''),
                ('logs_retention_days',           'global',              ''),
                ('applied_events_retention_days', 'cdc_kafka_apply',     '')
           );

        INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
        VALUES
            ('full_load_batch_size',          'mariadb_load',        '', '50000'::jsonb, 'MariaDB full-load COPY batch size'),
            ('apply_batch_size',              'cdc_kafka_apply',     '', '20000'::jsonb, 'Apply batch before flush'),
            ('reconcile_interval_hours',      'cdc_kafka_reconcile', '', '4'::jsonb, 'Hours between reconcile-loop runs'),
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
)PO_diagnostics";
    }
    inline std::string_view monitoring_views() {
        return R"PO_monitoring_v(
-- Drop dependent views first (CREATE OR REPLACE cannot rename columns e.g. service_tier -> events_total).
DROP VIEW IF EXISTS cdc_catalog.v_kafka_consumer;
DROP VIEW IF EXISTS cdc_catalog.v_apply_latest;
DROP VIEW IF EXISTS cdc_catalog.v_apply_stale;
DROP VIEW IF EXISTS cdc_catalog.v_cdc_pipeline_summary;

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
    apply_lag_seconds,
    is_stale,
    is_inactive,
    reconciliation_rag,
    dedup_skipped,
    parse_skipped,
    dropped_unrecoverable,
    logged_at
FROM cdc_catalog.apply_batch_stats
ORDER BY conn_id, source_schema, source_table, logged_at DESC;

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
    latest.events_total AS last_slice_events,
    latest.logged_at AS last_apply_at,
    latest.is_inactive,
    latest.is_stale,
    latest.reconciliation_rag
FROM cdc_catalog.apply_position ap
JOIN cdc_catalog.catalog c ON c.catalog_id = ap.catalog_id
LEFT JOIN cdc_catalog.v_apply_latest latest
    ON latest.conn_id = ap.conn_id
   AND latest.source_schema = ap.source_schema
   AND latest.source_table = ap.source_table;

COMMENT ON VIEW cdc_catalog.v_apply_latest IS 'Latest apply_batch_stats row per table (replaces manual apply_batch_stats ORDER BY logged_at)';
COMMENT ON VIEW cdc_catalog.v_kafka_consumer IS 'Per-table apply offset + last-known Kafka consumer lag (no docker exec)';
)PO_monitoring_v";
    }
}  // namespace prod_ops_embedded
