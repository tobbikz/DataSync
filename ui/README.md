# DataSync Control Plane UI

Next.js dashboard for operating the DataSync CDC pipeline — connections, catalog, logs, and health metrics from `cdc_catalog`.

## Quick start (dev)

```bash
cd ui
cp .env.example .env.local   # optional — defaults work for local dev
npm install
npm run dev
```

Open http://localhost:3000 — login with **admin / datasync** (override via env).

The UI reads PostgreSQL credentials from `../config.json` (`datasync` block) unless `DATASYNC_PG_*` env vars are set.

Requires PostgreSQL `datasync` with `cdc_catalog` schema (applied automatically by the DataSync binary on startup from embedded DDL). Without a DB connection, list endpoints return empty results.

## Docker (with stack)

```bash
# From repo root — builds ui + kafka + datasync daemon
docker compose up -d --build ui
```

UI: http://localhost:3000

## Pages

| Route | Data source |
|-------|-------------|
| `/dashboard` | KPIs, events chart, lag by conn |
| `/dashboard/connections` | `cdc_catalog.connections` |
| `/dashboard/catalog` | `cdc_catalog.catalog` |
| `/dashboard/logs` | `cdc_catalog.logs` |

**Actions** (Overview + Connections): `discover`, `onboard-pending --hot-only`, `full-load --conn-id` — spawned via local binary or `docker compose run datasync`.

## Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `DATASYNC_UI_USER` | `admin` | Login username |
| `DATASYNC_UI_PASSWORD` | `datasync` | Login password |
| `DATASYNC_UI_SECRET` | dev secret | JWT signing key |
| `DATASYNC_CONFIG` | `../config.json` | Path to DataSync config |

## Stack choice

Built with **Next.js 16** (App Router) + Tailwind 4 — API routes co-located with the UI, SSR-ready, and easy to dockerize alongside the C++ daemon.
