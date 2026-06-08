-- Host / process metrics on apply_batch_stats (Linux /proc sampler).

ALTER TABLE cdc_catalog.apply_batch_stats
    ADD COLUMN IF NOT EXISTS host_cpu_percent DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS host_mem_used_mb BIGINT,
    ADD COLUMN IF NOT EXISTS host_mem_percent INTEGER,
    ADD COLUMN IF NOT EXISTS host_net_rx_mb BIGINT,
    ADD COLUMN IF NOT EXISTS host_net_tx_mb BIGINT,
    ADD COLUMN IF NOT EXISTS process_rss_mb BIGINT;

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.host_cpu_percent IS
    'Host CPU busy % during apply slice (from /proc/stat delta)';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.host_mem_used_mb IS
    'Host memory used MB at slice sample time';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.host_mem_percent IS
    'Host memory used % at slice sample time';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.host_net_rx_mb IS
    'Host network RX MB during apply slice (excludes lo)';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.host_net_tx_mb IS
    'Host network TX MB during apply slice (excludes lo)';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.process_rss_mb IS
    'DataSync process RSS MB at sample time';
