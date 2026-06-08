-- Clarify mem columns: process RSS (DataSync), not host-wide used RAM.

COMMENT ON COLUMN cdc_catalog.apply_batch_stats.host_mem_used_mb IS
    'DataSync process RSS MB at slice sample time (/proc/self/status VmRSS)';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.host_mem_percent IS
    'DataSync process RSS as % of host MemTotal (not host-wide used %)';
COMMENT ON COLUMN cdc_catalog.apply_batch_stats.process_rss_mb IS
    'Same as host_mem_used_mb: DataSync RSS MB at sample time';
