# DataSync — install guide

Operator guide for running DataSync on a host with Docker or Podman.

## Prerequisites

- Linux host (tested with host networking; rootful Docker or rootless Podman)
- Docker + Compose v2 **or** Podman + `podman compose` / `docker-compose`
- PostgreSQL for:
  - **datasync** — control plane DB (`cdc_catalog` created by the binary)
  - **DataLake** — sink database
- Network reachability from the host to source engines (MariaDB / MSSQL / Mongo)

## 1. Clone and configure

```bash
git clone https://github.com/tobbikz/DataSync.git
cd DataSync
cp config.json.example config.json
```

Edit `config.json`:

| Block | Purpose |
|-------|---------|
| `datasync` | Catalog PostgreSQL (`host`, `port`, `database`, `user`, `password`) |
| `datalake` | Lake PostgreSQL |
| `cdc` | Pipeline knobs (idle, slice limits, workers, Kafka options) |

Source connections are **not** only in this file — register them in `cdc_catalog.connections` (UI or SQL) after the catalog schema exists.

## 2. First install

```bash
chmod +x install.sh docker-entrypoint.sh
./install.sh initial
```

This builds images, creates `./kafka-data`, and starts **Kafka + DataSync daemon + UI**.

| Service | Default |
|---------|---------|
| Kafka | `127.0.0.1:9092` (KRaft, data in `./kafka-data`) |
| Daemon | `DataSync daemon` (host network) |
| UI | [http://127.0.0.1:3000](http://127.0.0.1:3000) |

## 3. Day-2 lifecycle

```bash
./install.sh start   # rebuild / recreate stack
./install.sh stop    # stop Kafka + daemon + UI
```

Override Kafka data dir: `DATASYNC_KAFKA_DATA=/path/to/data ./install.sh start`.

## 4. UI credentials

| Variable | Default |
|----------|---------|
| `DATASYNC_UI_USER` | `admin` |
| `DATASYNC_UI_PASSWORD` | `datasync` |
| `DATASYNC_UI_SECRET` | `change-me-in-production` |

Set them in the environment before `./install.sh start`, or in your compose override. Never leave defaults on a shared host.

## 5. Useful CLI (inside the stack)

```bash
docker compose run --rm datasync discover
docker compose run --rm datasync test-connection
docker compose run --rm datasync full-load --conn-id <alias>
docker compose run --rm datasync onboard-pending
docker compose run --rm datasync unquarantine --conn-id <alias> --schema S --table T
```

Binary usage is also printed by `DataSync --help` / unknown command.

## 6. systemd (optional)

[`DataSync.service`](DataSync.service) wraps `install.sh start|stop`. Adjust `WorkingDirectory` / user, then:

```bash
sudo cp DataSync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now DataSync
```

## 7. Lab engines (optional)

```bash
docker compose -f docker-compose.dev-engines.yml up -d
./tools/bootstrap-dev-engines.sh
```

Lab passwords default to `DataSync_Dev1!` — lab only, not for production.

## 8. Hardening checklist

- [ ] Strong PG passwords in `config.json` (file mode `600`)
- [ ] Strong UI password + JWT secret
- [ ] UI / Kafka / PG not published to the public internet without TLS + ACL
- [ ] Least-privilege CDC users on source engines
- [ ] Disk watch on `./kafka-data` (retention is tuned in `docker-compose.yml`)

See also [`SECURITY.md`](SECURITY.md).
