-- Add in-progress replication_status values for full load and CDC visibility
-- Database: DataLake

DO $$
BEGIN
    ALTER TYPE cdc_catalog.replication_status ADD VALUE 'full_load_in_progress';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TYPE cdc_catalog.replication_status ADD VALUE 'cdc_in_progress';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

COMMENT ON TYPE cdc_catalog.replication_status IS
    'pending | full_load_in_progress | cdc_in_progress | success | failed | skipped | disabled';
