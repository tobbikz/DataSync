-- Local smoke: prove TEMP staging drift + DROP/recreate fix (no DataSync binary needed).
\set ON_ERROR_STOP on

DROP SCHEMA IF EXISTS qmetrics CASCADE;
CREATE SCHEMA qmetrics;
CREATE TABLE qmetrics.calls_logs (
  id bigint PRIMARY KEY,
  agent text,
  _dl_load_timestamp timestamptz,
  _dl_load_date date,
  _dl_source_system varchar,
  _dl_snapshot_id text
);

CREATE TEMP TABLE cdc_stg_qmetrics_calls_logs (LIKE qmetrics.calls_logs INCLUDING DEFAULTS);

ALTER TABLE qmetrics.calls_logs ADD COLUMN agent_account text NULL;
TRUNCATE cdc_stg_qmetrics_calls_logs;

-- Same probe as cpp temp_staging_matches_lake_columns()
SELECT
  (SELECT count(*)::int
     FROM pg_attribute a
     JOIN pg_class c ON c.oid = a.attrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'cdc_stg_qmetrics_calls_logs'
      AND n.nspname LIKE 'pg_temp_%'
      AND a.attnum > 0 AND NOT a.attisdropped
      AND a.attname <> '_cdc_stg_kafka_offset') AS stg_cols,
  (SELECT count(*)::int
     FROM information_schema.columns
    WHERE table_schema = 'qmetrics' AND table_name = 'calls_logs') AS lake_cols;

-- Stale TEMP must not have agent_account
SELECT NOT EXISTS (
  SELECT 1
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relname = 'cdc_stg_qmetrics_calls_logs'
    AND n.nspname LIKE 'pg_temp_%'
    AND a.attname = 'agent_account'
    AND a.attnum > 0 AND NOT a.attisdropped
) AS stale_missing_agent_account;

-- NEW fix path: DROP + recreate LIKE (what ensure_temp_table_like does on mismatch)
DROP TABLE IF EXISTS cdc_stg_qmetrics_calls_logs;
CREATE TEMP TABLE cdc_stg_qmetrics_calls_logs (LIKE qmetrics.calls_logs INCLUDING DEFAULTS);

SELECT
  (SELECT count(*)::int
     FROM pg_attribute a
     JOIN pg_class c ON c.oid = a.attrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'cdc_stg_qmetrics_calls_logs'
      AND n.nspname LIKE 'pg_temp_%'
      AND a.attnum > 0 AND NOT a.attisdropped
      AND a.attname <> '_cdc_stg_kafka_offset') AS stg_cols_after_fix,
  (SELECT count(*)::int
     FROM information_schema.columns
    WHERE table_schema = 'qmetrics' AND table_name = 'calls_logs') AS lake_cols_after_fix;

COPY cdc_stg_qmetrics_calls_logs
  (id, agent, agent_account, _dl_load_timestamp, _dl_load_date, _dl_source_system, _dl_snapshot_id)
FROM STDIN WITH (FORMAT csv);
1,bob,acc1,2026-10-28 12:00:00+00,2026-10-28,MariaDB,batch1
\.

SELECT id, agent, agent_account FROM cdc_stg_qmetrics_calls_logs;
SELECT 'temp_staging_drift_smoke_ok' AS status;
