# DataSync

CDC Data Lake — MariaDB, MSSQL, MongoDB → PostgreSQL.

## Install

**Kafka + DataSync** run in **Podman/Docker**. Production uses systemd as user **`datalake`** (rootless podman).

```bash
./install.sh
```

Full prod checklist: **[deploy/PROD.md](deploy/PROD.md)**

### Prod (Oracle Linux / RHEL)

```bash
cd /opt/DataSync
git pull
podman compose stop kafka datasync 2>/dev/null || true   # stop YOUR old containers
./install.sh                                              # uses sudo for systemd
```

### What you need

| Item | Value |
|------|--------|
| OS | Linux + Podman Compose v2 |
| Prod user | `datalake` (systemd runs Podman as this user) |
| Config | `config.json` (PostgreSQL + sources) |
| Kafka | `localhost:9092` (compose container) |
| Port | **9092 free** before install |

Do **not** run `podman compose up` as your personal user on prod after systemd is enabled.

## systemd

Un solo unit **`DataSync.service`**: levanta Kafka + daemon (`podman compose up kafka datasync`).

```bash
sudo systemctl restart DataSync
systemctl status DataSync
```

## SQL

Bootstrap via `./install.sh` — `sql/backup/cdc_catalog_schema_structure.sql`, `datalake_lake_schema.sql`.
