# DataSync

CDC Data Lake pipeline — native C++ ingest from MariaDB, MSSQL, and MongoDB into PostgreSQL.

## Quick start

```bash
cp config.json.example config.json   # edit credentials
./start.sh
```

## Layout

| Path | Purpose |
|------|---------|
| `cpp/` | DataSync binary (catalog, full-load, Kafka capture/apply) |
| `sql/` | PostgreSQL migrations + `backup/cdc_catalog_schema_structure.sql` |
| `scripts/` | Docker, Kafka, engine setup |
| `deploy/` | systemd units |

## Docs

- Runtime config: `cdc_catalog.runtime_config` (see `sql/004_*.sql`)
- Schema backup (prod DDL): `sql/backup/`
