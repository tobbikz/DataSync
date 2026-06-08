-- Drop unused Debezium/DDL tables; apply_position I/U/D totals; tier rename hot→platinum.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_enum e
        JOIN pg_type t ON e.enumtypid = t.oid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname = 'cdc_catalog' AND t.typname = 'service_tier' AND e.enumlabel = 'platinum'
    ) THEN
        ALTER TYPE cdc_catalog.service_tier ADD VALUE 'platinum';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_enum e
        JOIN pg_type t ON e.enumtypid = t.oid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname = 'cdc_catalog' AND t.typname = 'service_tier' AND e.enumlabel = 'trash'
    ) THEN
        ALTER TYPE cdc_catalog.service_tier ADD VALUE 'trash';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_enum e
        JOIN pg_type t ON e.enumtypid = t.oid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname = 'cdc_catalog' AND t.typname = 'service_tier' AND e.enumlabel = 'firehose'
    ) THEN
        ALTER TYPE cdc_catalog.service_tier ADD VALUE 'firehose';
    END IF;
END $$;

DROP TABLE IF EXISTS cdc_catalog.signal_audit;
DROP TABLE IF EXISTS cdc_catalog.schema_history;
DROP TABLE IF EXISTS cdc_catalog.service_tiers;

ALTER TABLE cdc_catalog.apply_position
    ADD COLUMN IF NOT EXISTS events_inserts BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS events_updates BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS events_deletes BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS events_total BIGINT NOT NULL DEFAULT 0;

-- Backfill total from legacy counter where new columns are zero
UPDATE cdc_catalog.apply_position
SET events_total = events_applied
WHERE events_total = 0 AND events_applied > 0;

UPDATE cdc_catalog.catalog
SET service_tier = 'platinum', updated_at = now()
WHERE lower(service_tier::text) = 'hot';

DELETE FROM cdc_catalog.runtime_config
WHERE config_key IN (
    'ddl_replay_enabled',
    'ddl_capture_enabled',
    'daemon_idle_seconds_hot',
    'daemon_idle_seconds_gold',
    'daemon_idle_seconds_silver',
    'daemon_idle_seconds_bronze',
    'daemon_round_idle_seconds'
);

COMMENT ON COLUMN cdc_catalog.apply_position.events_inserts IS 'Lifetime INSERT/c snapshot ops applied to lake';
COMMENT ON COLUMN cdc_catalog.apply_position.events_updates IS 'Lifetime UPDATE ops applied';
COMMENT ON COLUMN cdc_catalog.apply_position.events_deletes IS 'Lifetime DELETE ops applied';
COMMENT ON COLUMN cdc_catalog.apply_position.events_total IS 'Lifetime I+U+D; mirrors events_applied';
