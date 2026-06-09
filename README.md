# DataSync

CDC Data Lake pipeline — C++ ingest from MariaDB, MSSQL, and MongoDB into PostgreSQL.

## Install (Docker only)

Requires **Docker** or **Podman** + **Compose v2** on any Linux distro. `./install.sh` prefers **podman** when the `docker` CLI is a podman shim. Rootless podman: `systemctl --user start podman.socket` if the socket is down.

Docker **does not** create PostgreSQL, MariaDB, MSSQL, or MongoDB — only Kafka + the DataSync daemon. Point `config.json` at your existing databases.

```bash
./install.sh
```

On first run, `./install.sh` copies `config.json.example` → `config.json` if missing (edit password, then re-run).

`install.sh` runs the full bootstrap:

1. Builds the **DataSync** image
2. Ensures **`cdc_catalog`** in `config.json` → `datasync.database` and **`lake`** helpers in `datalake.database` (creates DBs if missing; idempotent)
3. Starts **Kafka** (KRaft — no Zookeeper)
4. Starts the **DataSync daemon**
5. **Discover** — skipped in install; run manually after onboarding sources
6. **Discover** — skipped in install; run manually after onboarding sources

No sudo required for a normal install. If systemd units are missing and `sudo` is available, `./install.sh` installs them automatically (`DataSync-kafka` + `DataSync`).

| Service | URL |
|---------|-----|
| Kafka | `localhost:9092` |
| PostgreSQL | external — from `config.json` |

The DataSync container uses **`network_mode: host`** (Linux) so it reaches host PostgreSQL and Kafka (`localhost:9092`) without extra PG configuration.

## systemd

`./install.sh` installs **DataSync-kafka** + **DataSync** systemd units automatically when they are missing (requires `sudo` and system user `datalake`). To skip: `SKIP_SYSTEMD=1 ./install.sh`.

Manual (re-sync units after moving the repo):

```bash
sudo ./deploy/systemd/install-systemd.sh
```

That installs units for **`datalake`** user/group: **DataSync-kafka** + **DataSync** (reconcile runs inside the daemon). Requires system user `datalake` (see `install-systemd.sh` if missing).

Every **`sudo systemctl restart DataSync`** runs **`ExecStartPre`** first → rebuild, then recreates the daemon container.

```bash
sudo systemctl restart DataSync-kafka   # Kafka only
sudo systemctl restart DataSync         # CDC + reconcile (embedded)

systemctl status DataSync-kafka DataSync
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

# End-to-end per connection (discover → full-load → capture → apply):
./scripts/smoke_pipeline.sh
SMOKE_TIER=bronze SMOKE_CONN_ID=MARIADB01 ./scripts/smoke_pipeline.sh
```
