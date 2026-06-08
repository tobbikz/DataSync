# DataSync

CDC Data Lake pipeline — C++ ingest from MariaDB, MSSQL, and MongoDB into PostgreSQL.

## Install (Docker only)

Requires **Docker Engine** + **Compose v2** on any Linux distro. No host `psql`, no systemd, no native build.

Docker **does not** create PostgreSQL, MariaDB, MSSQL, or MongoDB — only Kafka + the DataSync daemon. Point `config.json` at your existing databases.

```bash
./install.sh
```

On first run, `./install.sh` copies `config.json.example` → `config.json` if missing (edit password, then re-run).

`install.sh` runs the full bootstrap:

1. Builds the **DataSync** image
2. Starts **Zookeeper + Kafka + Kafka UI**
3. Applies **`sql/backup/cdc_catalog_schema_structure.sql`** via container (`schema-only`)
4. Starts the **DataSync daemon**
5. Runs **`discover`**
6. Prints recent **daemon logs**

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8080 |
| Kafka | `localhost:9092` |
| PostgreSQL | external — from `config.json` |

The DataSync container uses **`network_mode: host`** (Linux) so it reaches host PostgreSQL and Kafka (`localhost:9092`) without extra PG configuration.

## SQL

Only one DDL file is kept:

```
sql/backup/cdc_catalog_schema_structure.sql
```

Structure only (no seed data). After apply, insert `cdc_catalog.connections` and `cdc_catalog.runtime_config` rows as needed.

## CLI (optional)

```bash
docker compose run --rm datasync full-load --tier bronze --conn-id MARIADB_LOCAL
docker compose run --rm datasync daemon --once
docker compose logs -f datasync
```
