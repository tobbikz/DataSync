-- Consolidate MariaDB capture cursor into capture_position only.

INSERT INTO cdc_catalog.capture_position (
    conn_id, gtid_set, binlog_file, binlog_position, status, updated_at
)
SELECT
    b.conn_id,
    '',
    b.binlog_file,
    b.binlog_position,
    'healthy'::cdc_catalog.cdc_health_status,
    b.updated_at
FROM cdc_catalog.cdc_binlog_position b
WHERE NOT EXISTS (
    SELECT 1 FROM cdc_catalog.capture_position c
    WHERE c.conn_id = b.conn_id
)
ON CONFLICT (conn_id) DO NOTHING;

UPDATE cdc_catalog.capture_position c
SET binlog_file = b.binlog_file,
    binlog_position = b.binlog_position,
    updated_at = GREATEST(c.updated_at, b.updated_at)
FROM cdc_catalog.cdc_binlog_position b
WHERE c.conn_id = b.conn_id
  AND (
    c.binlog_file IS NULL
    OR length(trim(c.binlog_file)) = 0
    OR c.binlog_position IS NULL
  );

DROP TABLE IF EXISTS cdc_catalog.cdc_binlog_position;
