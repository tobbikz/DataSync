-- Tier daemon tuning moved to config.json "cdc" block (round_idle_seconds, slice_*, tiers[]).
-- Per-table tier assignment stays on cdc_catalog.catalog.service_tier.

-- Idempotent: catalog.service_tier TEXT (was enum in sql/001)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'cdc_catalog'
          AND t.typname = 'service_tier'
    ) THEN
        ALTER TABLE cdc_catalog.catalog
            ALTER COLUMN service_tier TYPE TEXT USING service_tier::text;
        DROP TYPE cdc_catalog.service_tier;
    END IF;
EXCEPTION
    WHEN others THEN
        NULL;
END $$;

DROP TABLE IF EXISTS cdc_catalog.service_tiers;

DELETE FROM cdc_catalog.runtime_config
WHERE config_key IN (
    'daemon_round_idle_seconds',
    'daemon_idle_seconds_hot',
    'daemon_idle_seconds_gold',
    'daemon_idle_seconds_silver',
    'daemon_idle_seconds_bronze'
);
