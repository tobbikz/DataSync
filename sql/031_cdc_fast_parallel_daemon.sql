-- Legacy slice bump for installs still on runtime_config fallbacks.
-- Primary source: config.json cdc.slice_max_events / slice_max_seconds.

UPDATE cdc_catalog.runtime_config
SET config_value = '10000000'::jsonb, updated_at = now()
WHERE config_key = 'capture_max_events'
  AND component IN ('cdc_kafka_capture', 'cdc_kafka_mssql_capture', 'cdc_kafka_mongo_capture')
  AND conn_id = '';

UPDATE cdc_catalog.runtime_config
SET config_value = '10000000'::jsonb, updated_at = now()
WHERE config_key = 'apply_max_events'
  AND component = 'cdc_kafka_apply'
  AND conn_id = '';
