# CDC catalog — schema-only backup

Snapshot of **`cdc_catalog`** from local `DataLake` (PostgreSQL 18.3). **No row data** — DDL only.

Generated: 2025-06-06

## Contents

| Kind | Objects |
|------|---------|
| Schema | `cdc_catalog` |
| ENUMs | `cdc_health_status`, `db_engine`, `log_level`, `replication_status`, `service_tier` |
| Tables | `apply_batch_stats`, `apply_position`, `capture_position`, `catalog`, `cdc_applied_events`, `cdc_mongo_resume`, `cdc_mssql_lsn`, `cdc_run_fairness_metrics`, `connections`, `logs`, `reconciliation_result`, `reconciliation_run`, `runtime_config` |
| Views | `v_apply_stale`, `v_capture_health`, `v_cdc_pipeline_summary` |
| Functions | `ensure_monthly_partitions`, `month_bounds`, `prune_applied_events`, `prune_apply_batch_stats`, `purge_logs`, `touch_updated_at` |
| Triggers | `catalog_updated_at_trg`, `connections_updated_at_trg`, `runtime_config_updated_at_trg` |

File: `cdc_catalog_schema_structure.sql` (~1280 lines)

## Apply in prod (empty DB or new schema)

```bash
psql -h <host> -U <user> -d DataLake -f sql/backup/cdc_catalog_schema_structure.sql
```

If `cdc_catalog` already exists, drop it first (prod only when intentional):

```sql
DROP SCHEMA cdc_catalog CASCADE;
```

Then run the dump file.

## After DDL

1. Seed `cdc_catalog.connections` and `cdc_catalog.runtime_config` (migrations `sql/004_*.sql`, `sql/011_*.sql`, etc. have INSERT seeds if needed).
2. Run catalog discover / full-load — tables start empty.
3. Regenerate this backup after major schema changes:

```bash
pg_dump -h localhost -U tomy.berrios -d DataLake \
  --schema-only --no-owner --no-privileges --schema=cdc_catalog \
  -f sql/backup/cdc_catalog_schema_structure.sql
```
