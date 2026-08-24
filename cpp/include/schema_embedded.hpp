#pragma once

#include <string_view>
#include <vector>

namespace schema_embedded {

struct Script {
    std::string_view filename;
    std::string_view sql;
    bool destructive{false};
};

inline std::string_view k000_reset() {
    return R"__cdc_k000_reset__(
-- Destructive reset: only run when DATASYNC_SCHEMA_RESET=1 or applied manually.
DROP SCHEMA IF EXISTS cdc_catalog CASCADE;
CREATE SCHEMA cdc_catalog;

)__cdc_k000_reset__";
}

inline std::string_view k010_enums() {
    return R"__cdc_k010_enums__(
-- cdc_catalog enum types (idempotent).

DO $$ BEGIN
  CREATE TYPE cdc_catalog.db_engine AS ENUM ('mariadb', 'mssql', 'mongodb');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE cdc_catalog.log_level AS ENUM ('debug', 'info', 'warning', 'error');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE cdc_catalog.replication_status AS ENUM (
    'pending', 'success', 'syncing', 'error', 'failed', 'needs_full_load',
    'full_load_in_progress', 'cdc_in_progress', 'quarantined',
    'skipped', 'disabled'
  );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE cdc_catalog.cdc_health_status AS ENUM (
    'healthy', 'degraded', 'failed', 'unknown', 'quarantined',
    'stale', 'lagging', 'gap_detected'
  );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Add enum values that may be missing on older bootstrap schemas.
ALTER TYPE cdc_catalog.replication_status ADD VALUE IF NOT EXISTS 'cdc_in_progress';
ALTER TYPE cdc_catalog.replication_status ADD VALUE IF NOT EXISTS 'failed';
ALTER TYPE cdc_catalog.replication_status ADD VALUE IF NOT EXISTS 'skipped';
ALTER TYPE cdc_catalog.replication_status ADD VALUE IF NOT EXISTS 'disabled';
ALTER TYPE cdc_catalog.cdc_health_status ADD VALUE IF NOT EXISTS 'quarantined';
ALTER TYPE cdc_catalog.cdc_health_status ADD VALUE IF NOT EXISTS 'stale';
ALTER TYPE cdc_catalog.cdc_health_status ADD VALUE IF NOT EXISTS 'lagging';
ALTER TYPE cdc_catalog.cdc_health_status ADD VALUE IF NOT EXISTS 'gap_detected';

)__cdc_k010_enums__";
}

inline std::string_view k020_tables() {
    return R"__cdc_k020_tables__(
-- Core cdc_catalog tables (production shape).

CREATE TABLE IF NOT EXISTS cdc_catalog.connections (
  alias text NOT NULL PRIMARY KEY,
  db_engine cdc_catalog.db_engine NOT NULL,
  host text NOT NULL DEFAULT 'localhost',
  port integer NOT NULL CHECK (port > 0 AND port <= 65535),
  db_name text NOT NULL DEFAULT '',
  username text NOT NULL DEFAULT '',
  password text NOT NULL DEFAULT '',
  extras jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(extras) = 'object'),
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cdc_catalog.catalog (
  catalog_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  conn_id text NOT NULL,
  db_engine cdc_catalog.db_engine NOT NULL,
  source_database text NOT NULL DEFAULT '',
  source_schema text NOT NULL,
  source_table text NOT NULL,
  has_pk boolean NOT NULL DEFAULT false,
  pk_columns text,
  discovered_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  active boolean NOT NULL DEFAULT false,
  cdc_enabled boolean NOT NULL DEFAULT false,
  needs_full_load boolean NOT NULL DEFAULT true,
  status cdc_catalog.replication_status NOT NULL DEFAULT 'pending',
  last_full_load_at timestamptz,
  last_cdc_at timestamptz,
  last_error_at timestamptz,
  last_error text,
  engine_meta jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(engine_meta) = 'object'),
  capture_during_full_load boolean NOT NULL DEFAULT false,
  hot boolean NOT NULL DEFAULT false,
  CONSTRAINT catalog_source_object_uk
    UNIQUE (conn_id, db_engine, source_database, source_schema, source_table),
  CONSTRAINT catalog_pk_columns_when_has_pk
    CHECK ((NOT has_pk) OR (pk_columns IS NOT NULL AND length(trim(both from pk_columns)) > 0))
) WITH (fillfactor = 80);

CREATE TABLE IF NOT EXISTS cdc_catalog.apply_position (
  catalog_id bigint NOT NULL PRIMARY KEY REFERENCES cdc_catalog.catalog (catalog_id) ON DELETE CASCADE,
  conn_id text NOT NULL,
  source_schema text NOT NULL,
  source_table text NOT NULL,
  kafka_topic text NOT NULL DEFAULT '',
  kafka_partition integer NOT NULL DEFAULT 0,
  kafka_offset bigint NOT NULL DEFAULT -1,
  last_applied_gtid text,
  last_applied_at timestamptz,
  apply_lag_seconds integer NOT NULL DEFAULT 0,
  status cdc_catalog.cdc_health_status NOT NULL DEFAULT 'healthy',
  last_error text,
  quarantined_at timestamptz,
  quarantine_reason text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT apply_position_object_uk UNIQUE (conn_id, source_schema, source_table)
) WITH (fillfactor = 70);

CREATE TABLE IF NOT EXISTS cdc_catalog.apply_batch_stats (
  stat_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  batch_id text NOT NULL,
  conn_id text NOT NULL,
  catalog_id bigint,
  source_schema text NOT NULL,
  source_table text NOT NULL,
  events_inserts bigint NOT NULL DEFAULT 0,
  events_updates bigint NOT NULL DEFAULT 0,
  events_deletes bigint NOT NULL DEFAULT 0,
  events_total bigint NOT NULL DEFAULT 0,
  duration_ms bigint NOT NULL DEFAULT 0,
  events_per_minute bigint NOT NULL DEFAULT 0,
  kafka_topic text,
  kafka_partition integer,
  kafka_offset bigint,
  context jsonb NOT NULL DEFAULT '{}'::jsonb,
  logged_at timestamptz NOT NULL DEFAULT now(),
  is_stale boolean NOT NULL DEFAULT false,
  is_inactive boolean NOT NULL DEFAULT false,
  is_quarantined boolean NOT NULL DEFAULT false,
  apply_health_rag text NOT NULL DEFAULT 'UNKNOWN'
    CHECK (apply_health_rag = ANY (ARRAY ['GREEN', 'AMBER', 'RED', 'UNKNOWN'])),
  apply_lag_seconds integer NOT NULL DEFAULT 0,
  apply_position_status text,
  events_seen_in_slice integer NOT NULL DEFAULT 0,
  catalog_active boolean,
  cdc_enabled boolean,
  capture_lag_seconds integer NOT NULL DEFAULT 0,
  kafka_consumer_lag bigint NOT NULL DEFAULT 0,
  reconcile_row_delta bigint,
  dedup_skipped integer NOT NULL DEFAULT 0,
  parse_skipped bigint NOT NULL DEFAULT 0,
  dropped_unrecoverable bigint NOT NULL DEFAULT 0,
  seconds_since_last_apply integer NOT NULL DEFAULT -1,
  kafka_partition_lag bigint,
  lag_scan_complete boolean NOT NULL DEFAULT false,
  slice_kind text NOT NULL DEFAULT 'flush',
  event_loss_status text NOT NULL DEFAULT 'ok',
  health_reason text NOT NULL DEFAULT 'healthy'
);

CREATE TABLE IF NOT EXISTS cdc_catalog.capture_position (
  conn_id text NOT NULL PRIMARY KEY,
  gtid_set text NOT NULL DEFAULT '',
  binlog_file text,
  binlog_position bigint,
  kafka_connect_name text,
  last_event_ts timestamptz,
  capture_lag_seconds integer NOT NULL DEFAULT 0,
  server_uuid text,
  status cdc_catalog.cdc_health_status NOT NULL DEFAULT 'healthy',
  last_error text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_failed_source_schema text,
  last_failed_source_table text
);

CREATE TABLE IF NOT EXISTS cdc_catalog.cdc_mongo_resume (
  conn_id text NOT NULL,
  database text NOT NULL,
  collection text NOT NULL,
  resume_token jsonb,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (conn_id, database, collection)
);

CREATE TABLE IF NOT EXISTS cdc_catalog.cdc_mssql_lsn (
  conn_id text NOT NULL,
  database text NOT NULL,
  schema_name text NOT NULL,
  table_name text NOT NULL,
  last_start_lsn bytea NOT NULL,
  last_seqval bytea,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (conn_id, database, schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS cdc_catalog.logs (
  log_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  logged_at timestamptz NOT NULL DEFAULT now(),
  level cdc_catalog.log_level NOT NULL,
  component text NOT NULL,
  message text NOT NULL,
  context jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(context) = 'object'),
  batch_id text,
  conn_id text,
  source_schema text,
  source_table text
);

CREATE TABLE IF NOT EXISTS cdc_catalog.full_load_checkpoint (
  catalog_id bigint NOT NULL REFERENCES cdc_catalog.catalog (catalog_id) ON DELETE CASCADE,
  worker_id integer NOT NULL DEFAULT 0,
  batch_id text NOT NULL,
  phase text NOT NULL CHECK (phase = ANY (ARRAY ['truncate', 'ddl', 'copy'])),
  last_pk jsonb,
  rows_loaded bigint NOT NULL DEFAULT 0,
  source_rows bigint,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (catalog_id, worker_id)
);

-- Upgrade path from minimal bootstrap schema.
ALTER TABLE cdc_catalog.catalog ADD COLUMN IF NOT EXISTS source_database text NOT NULL DEFAULT '';
ALTER TABLE cdc_catalog.catalog ADD COLUMN IF NOT EXISTS discovered_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE cdc_catalog.catalog ADD COLUMN IF NOT EXISTS last_cdc_at timestamptz;
ALTER TABLE cdc_catalog.catalog ADD COLUMN IF NOT EXISTS last_error_at timestamptz;
ALTER TABLE cdc_catalog.catalog ADD COLUMN IF NOT EXISTS last_error text;
ALTER TABLE cdc_catalog.catalog ADD COLUMN IF NOT EXISTS capture_during_full_load boolean NOT NULL DEFAULT false;

ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS events_inserts bigint NOT NULL DEFAULT 0;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS events_updates bigint NOT NULL DEFAULT 0;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS events_deletes bigint NOT NULL DEFAULT 0;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS duration_ms bigint NOT NULL DEFAULT 0;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS events_per_minute bigint NOT NULL DEFAULT 0;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS kafka_topic text;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS kafka_partition integer;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS kafka_offset bigint;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS is_stale boolean NOT NULL DEFAULT false;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS is_inactive boolean NOT NULL DEFAULT false;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS apply_position_status text;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS events_seen_in_slice integer NOT NULL DEFAULT 0;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS catalog_active boolean;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS cdc_enabled boolean;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS reconcile_row_delta bigint;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS dedup_skipped integer NOT NULL DEFAULT 0;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS parse_skipped bigint NOT NULL DEFAULT 0;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS dropped_unrecoverable bigint NOT NULL DEFAULT 0;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS seconds_since_last_apply integer NOT NULL DEFAULT -1;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS kafka_partition_lag bigint;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS lag_scan_complete boolean NOT NULL DEFAULT false;
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS slice_kind text NOT NULL DEFAULT 'flush';
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS event_loss_status text NOT NULL DEFAULT 'ok';
ALTER TABLE cdc_catalog.apply_batch_stats ADD COLUMN IF NOT EXISTS health_reason text NOT NULL DEFAULT 'healthy';

ALTER TABLE cdc_catalog.apply_position ADD COLUMN IF NOT EXISTS last_applied_gtid text;
ALTER TABLE cdc_catalog.apply_position ADD COLUMN IF NOT EXISTS quarantined_at timestamptz;

ALTER TABLE cdc_catalog.capture_position ADD COLUMN IF NOT EXISTS last_failed_source_schema text;
ALTER TABLE cdc_catalog.capture_position ADD COLUMN IF NOT EXISTS last_failed_source_table text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = 'cdc_catalog.apply_batch_stats'::regclass
      AND attname = 'semaphore'
      AND NOT attisdropped
  ) THEN
    ALTER TABLE cdc_catalog.apply_batch_stats
      ADD COLUMN semaphore text GENERATED ALWAYS AS (apply_health_rag) STORED;
  END IF;
EXCEPTION WHEN others THEN
  NULL;
END $$;

COMMENT ON TABLE cdc_catalog.apply_batch_stats IS
  'Per-table apply slice stats: I/U/D counts, duration, events/min — keyed by batch_id';
COMMENT ON TABLE cdc_catalog.capture_position IS
  'GTID-first capture cursor; source of truth for native binlog capture resume.';
COMMENT ON TABLE cdc_catalog.catalog IS
  'Replication registry: one row per source object per conn_id.';
COMMENT ON TABLE cdc_catalog.apply_position IS
  'Per-table Kafka apply cursor; independent lag and quarantine state.';
COMMENT ON TABLE cdc_catalog.connections IS
  'Source connection registry. alias maps 1:1 to catalog.conn_id.';
COMMENT ON TABLE cdc_catalog.logs IS
  'Append-only application log for all pipeline components.';

)__cdc_k020_tables__";
}

inline std::string_view k030_indexes() {
    return R"__cdc_k030_indexes__(
-- Indexes (idempotent).

CREATE INDEX IF NOT EXISTS apply_batch_stats_table_idx
  ON cdc_catalog.apply_batch_stats (conn_id, source_schema, source_table, logged_at DESC);
CREATE INDEX IF NOT EXISTS ix_apply_batch_logged_at_desc
  ON cdc_catalog.apply_batch_stats (logged_at DESC);
CREATE INDEX IF NOT EXISTS ix_apply_batch_batch_logged
  ON cdc_catalog.apply_batch_stats (batch_id, logged_at DESC);
CREATE INDEX IF NOT EXISTS apply_batch_stats_lag_recent_idx
  ON cdc_catalog.apply_batch_stats (logged_at DESC, kafka_consumer_lag DESC)
  WHERE kafka_consumer_lag > 0;
CREATE INDEX IF NOT EXISTS apply_batch_stats_logged_at_brin
  ON cdc_catalog.apply_batch_stats USING brin (logged_at);
CREATE INDEX IF NOT EXISTS apply_batch_stats_conn_events_window_idx
  ON cdc_catalog.apply_batch_stats (conn_id, logged_at DESC)
  INCLUDE (source_schema, source_table, events_total, events_inserts, events_updates, events_deletes, batch_id)
  WHERE COALESCE(events_total, 0) > 0 OR COALESCE(events_seen_in_slice, 0) > 0;
CREATE INDEX IF NOT EXISTS apply_batch_stats_health_rag_idx
  ON cdc_catalog.apply_batch_stats (conn_id, apply_health_rag, logged_at DESC);
CREATE INDEX IF NOT EXISTS apply_batch_stats_logged_at_stat_id_idx
  ON cdc_catalog.apply_batch_stats (logged_at, stat_id);
CREATE INDEX IF NOT EXISTS apply_batch_stats_idle_zero_idx
  ON cdc_catalog.apply_batch_stats (logged_at, stat_id)
  WHERE COALESCE(is_inactive, false)
     OR (COALESCE(events_total, 0) = 0 AND COALESCE(events_seen_in_slice, 0) = 0);
CREATE INDEX IF NOT EXISTS apply_batch_stats_catalog_id_idx
  ON cdc_catalog.apply_batch_stats (catalog_id, logged_at DESC);

CREATE INDEX IF NOT EXISTS apply_position_stale_idx
  ON cdc_catalog.apply_position (status, apply_lag_seconds DESC)
  WHERE status = ANY (ARRAY ['stale'::cdc_catalog.cdc_health_status, 'lagging'::cdc_catalog.cdc_health_status, 'gap_detected'::cdc_catalog.cdc_health_status]);
CREATE INDEX IF NOT EXISTS apply_position_conn_status_idx
  ON cdc_catalog.apply_position (conn_id, status);

CREATE INDEX IF NOT EXISTS catalog_engine_meta_gin
  ON cdc_catalog.catalog USING gin (engine_meta jsonb_path_ops);
CREATE INDEX IF NOT EXISTS catalog_failed_idx
  ON cdc_catalog.catalog (last_error_at DESC)
  WHERE status IN ('failed'::cdc_catalog.replication_status, 'error'::cdc_catalog.replication_status);
CREATE INDEX IF NOT EXISTS ix_catalog_db_engine ON cdc_catalog.catalog (db_engine);
CREATE INDEX IF NOT EXISTS ix_catalog_schema_table_conn_active
  ON cdc_catalog.catalog (source_schema, source_table, conn_id, active DESC, cdc_enabled DESC, updated_at DESC);
CREATE INDEX IF NOT EXISTS catalog_conn_table_idx
  ON cdc_catalog.catalog (conn_id, source_schema, source_table);
CREATE INDEX IF NOT EXISTS idx_catalog_hot
  ON cdc_catalog.catalog (conn_id)
  WHERE hot = true AND active = true AND cdc_enabled = true;
CREATE INDEX IF NOT EXISTS catalog_active_cdc_idx
  ON cdc_catalog.catalog (conn_id)
  WHERE active = true AND cdc_enabled = true AND needs_full_load = false;
CREATE INDEX IF NOT EXISTS catalog_needs_full_load_idx
  ON cdc_catalog.catalog (conn_id)
  WHERE active = true AND needs_full_load = true;
CREATE INDEX IF NOT EXISTS ix_catalog_list_sort
  ON cdc_catalog.catalog (active DESC, needs_full_load DESC, updated_at DESC)
  INCLUDE (source_schema, source_table, conn_id, db_engine, cdc_enabled, status);
CREATE INDEX IF NOT EXISTS ix_catalog_conn_list_sort
  ON cdc_catalog.catalog (conn_id, active DESC, needs_full_load DESC, updated_at DESC)
  INCLUDE (source_schema, source_table, db_engine);
CREATE INDEX IF NOT EXISTS catalog_conn_engine_active_idx
  ON cdc_catalog.catalog (conn_id, db_engine)
  WHERE active = true;
CREATE INDEX IF NOT EXISTS catalog_capture_during_full_load_idx
  ON cdc_catalog.catalog (conn_id, db_engine)
  WHERE capture_during_full_load = true AND needs_full_load = false AND cdc_enabled = false;
CREATE INDEX IF NOT EXISTS catalog_status_updated_idx
  ON cdc_catalog.catalog (status, updated_at DESC);

CREATE INDEX IF NOT EXISTS connections_active_engine_idx
  ON cdc_catalog.connections (db_engine, alias)
  WHERE active = true;

CREATE INDEX IF NOT EXISTS logs_component_logged_at_idx
  ON cdc_catalog.logs (component, logged_at DESC);
CREATE INDEX IF NOT EXISTS logs_context_gin
  ON cdc_catalog.logs USING gin (context jsonb_path_ops);
CREATE INDEX IF NOT EXISTS logs_errors_idx
  ON cdc_catalog.logs (logged_at DESC)
  WHERE level = ANY (ARRAY ['warning'::cdc_catalog.log_level, 'error'::cdc_catalog.log_level]);
CREATE INDEX IF NOT EXISTS logs_logged_at_idx ON cdc_catalog.logs (logged_at DESC);
CREATE INDEX IF NOT EXISTS logs_level_logged_at_idx ON cdc_catalog.logs (level, logged_at DESC);
CREATE INDEX IF NOT EXISTS logs_conn_id_logged_at_idx
  ON cdc_catalog.logs (conn_id, logged_at DESC)
  WHERE conn_id IS NOT NULL AND conn_id <> '';
CREATE INDEX IF NOT EXISTS logs_logged_at_brin_idx ON cdc_catalog.logs USING brin (logged_at);

CREATE INDEX IF NOT EXISTS cdc_mongo_resume_updated_idx
  ON cdc_catalog.cdc_mongo_resume (conn_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS cdc_mssql_lsn_updated_idx
  ON cdc_catalog.cdc_mssql_lsn (conn_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS full_load_checkpoint_updated_idx
  ON cdc_catalog.full_load_checkpoint (updated_at DESC);

)__cdc_k030_indexes__";
}

inline std::string_view k040_functions() {
    return R"__cdc_k040_functions__(
-- Retention / purge helpers used by the C++ daemon.

CREATE OR REPLACE FUNCTION cdc_catalog.purge_logs(retention_days integer)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  deleted bigint;
BEGIN
  DELETE FROM cdc_catalog.logs
  WHERE logged_at < now() - make_interval(days => GREATEST(retention_days, 1));
  GET DIAGNOSTICS deleted = ROW_COUNT;
  RETURN deleted;
END;
$$;

CREATE OR REPLACE FUNCTION cdc_catalog.purge_logs_batched(
  retention_days integer,
  batch_size integer,
  max_batches integer
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  total bigint := 0;
  batch bigint;
  i integer := 0;
  lim integer := GREATEST(batch_size, 1);
  maxb integer := GREATEST(max_batches, 1);
BEGIN
  WHILE i < maxb LOOP
    DELETE FROM cdc_catalog.logs
    WHERE ctid IN (
      SELECT ctid FROM cdc_catalog.logs
      WHERE logged_at < now() - make_interval(days => GREATEST(retention_days, 1))
      ORDER BY logged_at
      LIMIT lim
    );
    GET DIAGNOSTICS batch = ROW_COUNT;
    total := total + batch;
    EXIT WHEN batch = 0;
    i := i + 1;
  END LOOP;
  RETURN total;
END;
$$;

CREATE OR REPLACE FUNCTION cdc_catalog.prune_apply_batch_stats_batched(
  retention_days integer,
  batch_size integer,
  max_batches integer
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  total bigint := 0;
  batch bigint;
  i integer := 0;
  lim integer := GREATEST(batch_size, 1);
  maxb integer := GREATEST(max_batches, 1);
BEGIN
  WHILE i < maxb LOOP
    DELETE FROM cdc_catalog.apply_batch_stats
    WHERE ctid IN (
      SELECT ctid FROM cdc_catalog.apply_batch_stats
      WHERE logged_at < now() - make_interval(days => GREATEST(retention_days, 1))
      ORDER BY logged_at, stat_id
      LIMIT lim
    );
    GET DIAGNOSTICS batch = ROW_COUNT;
    total := total + batch;
    EXIT WHEN batch = 0;
    i := i + 1;
  END LOOP;
  RETURN total;
END;
$$;

CREATE OR REPLACE FUNCTION cdc_catalog.prune_apply_batch_stats_idle_batched(
  batch_size integer,
  max_batches integer
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  total bigint := 0;
  batch bigint;
  i integer := 0;
  lim integer := GREATEST(batch_size, 1);
  maxb integer := GREATEST(max_batches, 1);
BEGIN
  WHILE i < maxb LOOP
    DELETE FROM cdc_catalog.apply_batch_stats
    WHERE ctid IN (
      SELECT ctid FROM cdc_catalog.apply_batch_stats
      WHERE COALESCE(is_inactive, false)
         OR (COALESCE(events_total, 0) = 0 AND COALESCE(events_seen_in_slice, 0) = 0)
      ORDER BY logged_at, stat_id
      LIMIT lim
    );
    GET DIAGNOSTICS batch = ROW_COUNT;
    total := total + batch;
    EXIT WHEN batch = 0;
    i := i + 1;
  END LOOP;
  RETURN total;
END;
$$;

)__cdc_k040_functions__";
}

inline std::string_view k050_views() {
    return R"__cdc_k050_views__(
-- Superset Ops dashboard materialized views (DataSync Ops v10).

DROP MATERIALIZED VIEW IF EXISTS cdc_catalog.mv_tab_health_latest_3d CASCADE;
CREATE MATERIALIZED VIEW cdc_catalog.mv_tab_health_latest_3d AS
SELECT DISTINCT ON (s.catalog_id)
  s.stat_id,
  s.logged_at,
  s.logged_at AS event_ts,
  s.batch_id,
  s.conn_id,
  c.db_engine::text AS db_engine,
  s.source_schema,
  s.source_table,
  (s.source_schema || '.' || s.source_table) AS table_fqn,
  s.catalog_id,
  s.events_inserts,
  s.events_updates,
  s.events_deletes,
  s.events_total,
  s.duration_ms,
  s.events_per_minute,
  s.kafka_consumer_lag,
  s.kafka_partition_lag,
  s.capture_lag_seconds,
  s.apply_lag_seconds,
  s.seconds_since_last_apply,
  s.apply_health_rag,
  s.health_reason,
  s.event_loss_status,
  s.is_stale,
  s.is_inactive,
  s.is_quarantined,
  s.lag_scan_complete,
  s.events_seen_in_slice,
  s.reconcile_row_delta,
  s.dedup_skipped,
  s.parse_skipped,
  s.dropped_unrecoverable,
  CASE WHEN s.apply_health_rag = 'GREEN' THEN 1 ELSE 0 END AS is_green,
  CASE WHEN s.apply_health_rag = 'AMBER' THEN 1 ELSE 0 END AS is_amber,
  CASE WHEN s.apply_health_rag = 'RED' THEN 1 ELSE 0 END AS is_red,
  CASE WHEN s.is_stale THEN 1 ELSE 0 END AS is_stale_i,
  CASE WHEN s.is_inactive THEN 1 ELSE 0 END AS is_inactive_i,
  CASE WHEN s.event_loss_status <> 'ok' THEN 1 ELSE 0 END AS is_event_loss,
  CASE WHEN s.is_quarantined THEN 1 ELSE 0 END AS is_apply_quarantined,
  0 AS is_reconcile_gap,
  CASE WHEN COALESCE(s.kafka_consumer_lag, 0) > 0 THEN 1 ELSE 0 END AS lag_active
FROM cdc_catalog.apply_batch_stats s
JOIN cdc_catalog.catalog c ON c.catalog_id = s.catalog_id
WHERE s.logged_at >= now() - interval '3 days'
ORDER BY s.catalog_id, s.logged_at DESC;

CREATE UNIQUE INDEX IF NOT EXISTS mv_tab_health_latest_3d_catalog_id_idx
  ON cdc_catalog.mv_tab_health_latest_3d (catalog_id);

DROP MATERIALIZED VIEW IF EXISTS cdc_catalog.mv_tab_catalog CASCADE;
CREATE MATERIALIZED VIEW cdc_catalog.mv_tab_catalog AS
SELECT
  c.catalog_id,
  c.conn_id,
  c.db_engine::text AS db_engine,
  c.source_database,
  c.source_schema,
  c.source_table,
  (c.source_schema || '.' || c.source_table) AS table_fqn,
  c.active,
  c.cdc_enabled,
  c.needs_full_load,
  c.status::text AS status,
  c.hot,
  c.discovered_at,
  c.updated_at,
  c.last_full_load_at,
  c.last_cdc_at,
  c.last_error_at,
  COALESCE(c.updated_at, c.discovered_at) AS event_ts,
  ap.status::text AS apply_position_status,
  ap.apply_lag_seconds,
  ap.last_applied_at,
  ap.kafka_topic,
  ap.kafka_partition,
  ap.kafka_offset,
  ap.quarantine_reason,
  LEFT(ap.last_error, 120) AS last_error_short,
  CASE WHEN c.status = 'full_load_in_progress' THEN 1 ELSE 0 END AS is_full_load_in_progress,
  CASE WHEN c.needs_full_load THEN 1 ELSE 0 END AS is_needs_full_load,
  CASE WHEN c.status = 'cdc_in_progress' THEN 1 ELSE 0 END AS is_cdc_in_progress,
  CASE WHEN c.active AND c.cdc_enabled AND NOT c.needs_full_load THEN 1 ELSE 0 END AS is_cdc_ready,
  CASE WHEN c.status = 'quarantined' OR ap.status = 'quarantined' THEN 1 ELSE 0 END AS is_quarantined,
  CASE WHEN c.status = 'success' THEN 1 ELSE 0 END AS is_success,
  CASE WHEN c.status = 'pending' THEN 1 ELSE 0 END AS is_pending
FROM cdc_catalog.catalog c
LEFT JOIN cdc_catalog.apply_position ap ON ap.catalog_id = c.catalog_id;

CREATE UNIQUE INDEX IF NOT EXISTS mv_tab_catalog_catalog_id_idx
  ON cdc_catalog.mv_tab_catalog (catalog_id);

DROP MATERIALIZED VIEW IF EXISTS cdc_catalog.mv_tab_capture_latest CASCADE;
CREATE MATERIALIZED VIEW cdc_catalog.mv_tab_capture_latest AS
SELECT
  cp.conn_id,
  cp.status::text AS capture_status,
  cp.capture_lag_seconds,
  LEFT(COALESCE(cp.last_error, ''), 120) AS last_error_short,
  cp.updated_at AS event_ts,
  CASE WHEN cp.status <> 'healthy'::cdc_catalog.cdc_health_status THEN 1 ELSE 0 END AS is_unhealthy
FROM cdc_catalog.capture_position cp;

CREATE UNIQUE INDEX IF NOT EXISTS mv_tab_capture_latest_conn_id_idx
  ON cdc_catalog.mv_tab_capture_latest (conn_id);

DROP MATERIALIZED VIEW IF EXISTS cdc_catalog.mv_tab_events_hourly_3d CASCADE;
CREATE MATERIALIZED VIEW cdc_catalog.mv_tab_events_hourly_3d AS
SELECT
  date_trunc('hour', s.logged_at) AS event_ts,
  date_trunc('hour', s.logged_at) AS bucket,
  s.conn_id,
  c.db_engine::text AS db_engine,
  SUM(s.events_total) AS events_total,
  SUM(s.events_inserts) AS events_inserts,
  SUM(s.events_updates) AS events_updates,
  SUM(s.events_deletes) AS events_deletes,
  AVG(s.duration_ms)::bigint AS duration_ms_avg,
  MAX(s.kafka_consumer_lag) AS kafka_consumer_lag_max,
  SUM(s.events_seen_in_slice) AS events_seen_in_slice
FROM cdc_catalog.apply_batch_stats s
JOIN cdc_catalog.catalog c ON c.catalog_id = s.catalog_id
WHERE s.logged_at >= now() - interval '3 days'
GROUP BY 1, 2, 3, 4;

CREATE INDEX IF NOT EXISTS mv_tab_events_hourly_3d_conn_idx
  ON cdc_catalog.mv_tab_events_hourly_3d (conn_id, event_ts DESC);

DROP MATERIALIZED VIEW IF EXISTS cdc_catalog.mv_tab_kafka_hourly_3d CASCADE;
CREATE MATERIALIZED VIEW cdc_catalog.mv_tab_kafka_hourly_3d AS
SELECT
  date_trunc('hour', s.logged_at) AS event_ts,
  date_trunc('hour', s.logged_at) AS bucket,
  s.conn_id,
  MAX(s.kafka_consumer_lag) AS kafka_consumer_lag_max,
  MAX(s.kafka_partition_lag) AS kafka_partition_lag_max,
  AVG(s.apply_lag_seconds)::integer AS apply_lag_seconds_avg,
  AVG(s.capture_lag_seconds)::integer AS capture_lag_seconds_avg
FROM cdc_catalog.apply_batch_stats s
WHERE s.logged_at >= now() - interval '3 days'
GROUP BY 1, 2, 3;

CREATE INDEX IF NOT EXISTS mv_tab_kafka_hourly_3d_conn_idx
  ON cdc_catalog.mv_tab_kafka_hourly_3d (conn_id, event_ts DESC);

DROP MATERIALIZED VIEW IF EXISTS cdc_catalog.mv_tab_logs_hourly_3d CASCADE;
CREATE MATERIALIZED VIEW cdc_catalog.mv_tab_logs_hourly_3d AS
SELECT
  date_trunc('hour', l.logged_at) AS event_ts,
  date_trunc('hour', l.logged_at) AS bucket,
  l.component,
  l.level::text AS level,
  COUNT(*) AS row_count,
  COUNT(*) FILTER (WHERE l.level = 'error') AS error_count,
  COUNT(*) FILTER (WHERE l.level = 'warning') AS warning_count,
  COUNT(*) FILTER (WHERE l.level = 'info') AS info_count,
  MAX(l.logged_at) AS last_logged_at
FROM cdc_catalog.logs l
WHERE l.logged_at >= now() - interval '3 days'
GROUP BY 1, 2, 3, 4;

CREATE INDEX IF NOT EXISTS mv_tab_logs_hourly_3d_component_idx
  ON cdc_catalog.mv_tab_logs_hourly_3d (component, event_ts DESC);

)__cdc_k050_views__";
}

inline std::vector<Script> cdc_catalog_scripts() {
    return {
        {"000_reset.sql", k000_reset(), true},
        {"010_enums.sql", k010_enums(), false},
        {"020_tables.sql", k020_tables(), false},
        {"030_indexes.sql", k030_indexes(), false},
        {"040_functions.sql", k040_functions(), false},
        {"050_views.sql", k050_views(), false},
    };
}

inline std::string_view k_lake_helpers() {
    return R"__cdc_k_lake_helpers__(
-- Lake helper schema for datalake DB (partition management).
-- Applied via install.sh → apply_lake_schema (config.json → datalake).

CREATE SCHEMA IF NOT EXISTS lake;

CREATE OR REPLACE FUNCTION lake.month_bounds(p_month date)
RETURNS TABLE(month_start timestamptz, month_end timestamptz)
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT date_trunc('month', p_month::timestamptz)::timestamptz,
         (date_trunc('month', p_month::timestamptz) + interval '1 month')::timestamptz;
$$;

CREATE OR REPLACE FUNCTION lake.ensure_monthly_partitions(
    p_schema text,
    p_table text,
    p_months_ahead integer DEFAULT 3
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    parent_oid oid;
    parent_reg regclass;
    months integer := GREATEST(1, COALESCE(p_months_ahead, 3));
    anchor date := date_trunc('month', CURRENT_DATE)::date;
    month_cursor date;
    part_name text;
    bounds record;
BEGIN
    IF p_schema IS NULL OR btrim(p_schema) = '' OR p_table IS NULL OR btrim(p_table) = '' THEN
        RAISE EXCEPTION 'ensure_monthly_partitions: schema and table are required';
    END IF;

    SELECT c.oid, format('%I.%I', n.nspname, c.relname)::regclass
      INTO parent_oid, parent_reg
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = p_schema
       AND c.relname = p_table
       AND c.relkind = 'p';

    IF parent_oid IS NULL THEN
        RAISE EXCEPTION 'ensure_monthly_partitions: partitioned parent %.% not found', p_schema, p_table;
    END IF;

    -- Current month plus future months (months_ahead inclusive span).
    FOR i IN 0..months LOOP
        month_cursor := (anchor + (i || ' months')::interval)::date;
        SELECT * INTO bounds FROM lake.month_bounds(month_cursor);
        part_name := p_table || '_' || to_char(month_cursor, 'YYYY_MM');

        BEGIN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
                p_schema,
                part_name,
                parent_reg,
                bounds.month_start,
                bounds.month_end
            );
        EXCEPTION
            WHEN duplicate_table THEN
                NULL;
            WHEN invalid_table_definition THEN
                IF SQLERRM LIKE '%overlap%' OR SQLERRM LIKE '%already exists%' THEN
                    NULL;
                ELSE
                    RAISE;
                END IF;
        END;
    END LOOP;
END;
$$;

)__cdc_k_lake_helpers__";
}

inline std::string_view lake_helpers_sql() { return k_lake_helpers(); }

}  // namespace schema_embedded
