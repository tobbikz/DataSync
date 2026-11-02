# DataSync

**On-prem CDC into your PostgreSQL data lake.**

One C++ daemon, one Kafka bus, one ops UI. Capture from MariaDB, SQL Server, and MongoDB; apply into PostgreSQL with a first-class control plane (`cdc_catalog`) — discover, full-load, DDL sync, quarantine, and lag — without shipping your data to a SaaS.

| | |
|---|---|
| **Sources** | MariaDB / MySQL (binlog) · SQL Server · MongoDB |
| **Sink** | PostgreSQL data lake + PostgreSQL `cdc_catalog` control plane |
| **Deploy** | Docker / Podman Compose · `./install.sh` · systemd |
| **License** | [Apache License 2.0](LICENSE) |
| **Latest** | [`v1.1.1`](https://github.com/tobbikz/DataSync/releases) (see `VERSION` / tags) |

---

## Why DataSync

| Pain | What DataSync does |
|------|--------------------|
| Debezium + Connect + Kafka UI is a lot of moving parts | One daemon + Kafka + Next.js ops console |
| Catalog and lag live in tribal knowledge | `cdc_catalog` tracks connections, positions, quarantine, logs |
| Schema drift breaks apply silently | DDL sync, coercion audit, quarantine with unquarantine CLI |
| Hosted CDC sends rows off-network | Everything stays on your hosts |

**Who it is for:** data platform / DBA teams building an on-prem lake from OLTP.  
**Who it is not:** a managed cloud CDC service, an ELT SaaS, or a general message bus product.

---

## Quick start

### Host stack (operators)

Requires Docker or Podman + Compose v2, and PostgreSQL reachable for **datasync** (catalog) and **DataLake** (sink).

```bash
git clone https://github.com/tobbikz/DataSync.git
cd DataSync
cp config.json.example config.json   # edit hosts / users / passwords
./install.sh initial                 # first time: build + Kafka + daemon + UI
# later: ./install.sh start | stop
```

UI: [http://127.0.0.1:3000](http://127.0.0.1:3000) — default login `admin` / `datasync` (override with `DATASYNC_UI_*`).

Operator detail: [`INSTALL.md`](INSTALL.md).

### Lab engines (developers)

Optional MariaDB / MSSQL / Mongo / Postgres lab:

```bash
docker compose -f docker-compose.dev-engines.yml up -d
./tools/bootstrap-dev-engines.sh
./tools/start-dev-engines.sh
```

### UI only (dev)

```bash
cd ui
cp .env.example .env.local   # optional
npm install
npm run dev                  # http://127.0.0.1:3000
```

Reads `../config.json` (`datasync` block) unless `DATASYNC_PG_*` is set. See [`ui/README.md`](ui/README.md).

### Native C++ build

```bash
# Debian/Ubuntu-style deps — see Dockerfile for the canonical list
cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release
cmake --build cpp/build --target DataSync -j"$(nproc)"
./cpp/build/DataSync --help
```

Unit tests (Docker):

```bash
docker build --target test -t datasync-tests:local .
```

---

## What you get

### Pipeline

- **Capture** — MariaDB binlog, SQL Server CDC/change tracking path, Mongo change streams → Kafka
- **Apply** — Kafka → PostgreSQL lake with dedup, quarantine, optional reselect
- **Full load** — sliced bulk load with checkpoints and onboard-pending
- **DDL sync** — keep lake schemas aligned with sources
- **Daemon** — continuous capture + apply loop (`DataSync daemon`)

### Control plane

- **`cdc_catalog`** — connections, catalog rows, apply positions, logs, health
- **Ops UI** — dashboard, connections, catalog, logs; run discover / onboard / full-load
- **CLI** — `discover`, `full-load`, `kafka-apply`, `unquarantine`, `test-connection`, `coercion-audit`, …

### Ops

- **`install.sh`** — initial / start / stop on Docker or Podman (host network)
- **systemd** — [`DataSync.service`](DataSync.service) template
- **Lab smokes** — `tools/smoke-*.sh` against `docker-compose.dev-engines.yml`

---

## Architecture

```
MariaDB / MSSQL / Mongo ──(CDC)──► DataSync capture ──► Kafka ──► DataSync apply ──► PostgreSQL DataLake
                                         │                                              ▲
                                         └──────── cdc_catalog (PostgreSQL) ────────────┘
                                                           ▲
                                                     Ops UI (Next.js)
```

- Sources are registered in `cdc_catalog.connections` (not only in `config.json`).
- `config.json` holds catalog + lake PostgreSQL endpoints and CDC knobs.
- Compose uses **host networking** so the daemon reaches OLTP and PG on the host.

---

## Repository layout

```
cpp/           C++20 CDC daemon + CLI (CMake)
ui/            Next.js ops console
tools/         Lab engines, bootstrap, smoke scripts
Dockerfile     Multi-stage daemon image (+ test target)
docker-compose.yml              Kafka + daemon + UI
docker-compose.dev-engines.yml  Optional lab databases
install.sh     Host install / start / stop
config.json.example
```

---

## Documentation map

| Doc | Audience |
|-----|----------|
| [`INSTALL.md`](INSTALL.md) | Operators — install, config, systemd |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Contributors — layout, gates |
| [`SECURITY.md`](SECURITY.md) | Vulnerability reporting |
| [`ui/README.md`](ui/README.md) | UI-only development |
| [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) | Community norms |

---

## License & contact

**Apache License 2.0** — see [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

Issues: [github.com/tobbikz/DataSync](https://github.com/tobbikz/DataSync).
