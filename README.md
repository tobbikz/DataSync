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
2. Ensures **`cdc_catalog`** in `config.json` → `datasync.database` and **`lake`** helpers in `datalake.database` (creates DBs if missing; idempotent)
3. Starts **Zookeeper + Kafka**
4. Starts the **DataSync daemon**
5. Runs **`discover`**
6. Prints recent **daemon logs**
7. Installs and **enables** **systemd** (`DataSync` + `DataSync-reconcile.timer`) when sudo is available

| Service | URL |
|---------|-----|
| Kafka | `localhost:9092` |
| PostgreSQL | external — from `config.json` |

The DataSync container uses **`network_mode: host`** (Linux) so it reaches host PostgreSQL and Kafka (`localhost:9092`) without extra PG configuration.

## systemd

`./install.sh` runs `systemctl enable --now DataSync` and `DataSync-reconcile.timer` (needs sudo).

Every **`systemctl restart DataSync`** runs **`ExecStartPre`** first → rebuild, then recreates the daemon container.

```bash
systemctl status DataSync
docker compose ps
docker compose logs -f datasync
```

## SQL

Bootstrap idempotente (`./install.sh`):

| File | Target DB | Contenido |
|------|-----------|-----------|
| `sql/backup/cdc_catalog_schema_structure.sql` | `config.json` → **datasync** | Catálogo, logs, runtime_config, dedup, offsets |
| `sql/backup/datalake_lake_schema.sql` | `config.json` → **datalake** | Schema `lake` + particiones mensuales |

Sin seed data. Tras apply, insertar `cdc_catalog.connections` y `runtime_config` según necesidad.

## CLI (optional)

```bash
docker compose run --rm datasync full-load --tier bronze --conn-id MARIADB_LOCAL
docker compose run --rm datasync daemon --once
docker compose logs -f datasync
```
