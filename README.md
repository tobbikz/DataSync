# DataSync

CDC Data Lake pipeline — C++ ingest from MariaDB, MSSQL, and MongoDB into PostgreSQL.

## Install (Docker only)

Requires **Docker Engine** + **Compose v2** on any Linux distro. Optional **systemd** units for `enable`/`restart` with automatic rebuild.

Docker **does not** create PostgreSQL, MariaDB, MSSQL, or MongoDB — only Kafka + the DataSync daemon. Point `config.json` at your existing databases.

```bash
./install.sh
```

On first run, `./install.sh` copies `config.json.example` → `config.json` if missing (edit password, then re-run).

`install.sh` runs the full bootstrap:

1. Builds the **DataSync** image
2. Starts **Zookeeper + Kafka**
3. Applies **`sql/backup/cdc_catalog_schema_structure.sql`** via container (`schema-only`)
4. Starts the **DataSync daemon**
5. Runs **`discover`**
6. Prints recent **daemon logs**
7. Installs **systemd** units when run with sudo (`DataSync.service`, reconcile timer)

| Service | URL |
|---------|-----|
| Kafka | `localhost:9092` |
| PostgreSQL | external — from `config.json` |

The DataSync container uses **`network_mode: host`** (Linux) so it reaches host PostgreSQL and Kafka (`localhost:9092`) without extra PG configuration.

## systemd (optional)

Every **`systemctl restart DataSync`** runs **`ExecStartPre`** first → rebuild (`docker compose build datasync` by default), then recreates the daemon container.

```bash
sudo systemctl enable --now DataSync
sudo systemctl restart DataSync
sudo systemctl enable --now DataSync-reconcile.timer

deploy/systemd/datasync-cli.sh discover
docker compose logs -f datasync
```

Native binary (no Docker daemon): `sudo deploy/systemd/install-systemd.sh --native`

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
