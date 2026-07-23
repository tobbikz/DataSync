# CDC Project State (actualizado por Orchestrator)

## Objetivo

Pipeline robusto 24/7: `MariaDB/MSSQL/Mongo → C++ capture → Kafka → C++ apply → PostgreSQL DataLake`

**100% C++** — binario `DataSync`; paquete Python `cdc_kafka/` eliminado.

## Arquitectura DB

| DB | Rol |
|----|-----|
| **DataSync** | catalog, runtime_config, logs (cdc_catalog), dedup, apply_position |
| **DataLake** | tablas lake (COPY/INSERT destino) |

Config: **`config.json`** en raíz del repo (PG DataSync + DataLake). Fuentes OLTP en `cdc_catalog.connections`. Tuning en `runtime_config`.

## Completado

- [x] Dual PostgreSQL (datasync + datalake) en C++
- [x] `config.json` — PG DataSync + DataLake; fuentes en `cdc_catalog.connections`
- [x] CDC daemon tiers + slice tuning en `config.json` (`cdc` block); tabla `service_tiers` eliminada (`sql/032`)
- [x] Apply workers por tier desde tabla (override runtime)
- [x] Binario renombrado `DataSync` (CMake + executable)
- [x] systemd `DataSync.service` (ExecStart → DataSync binary + env file)
- [x] Daemon carga tiers desde DB (no hot/gold/silver/bronze hardcoded)
- [x] Native C++ capture + apply (~340k+ events/min)
- [x] MSSQL + MongoDB parity (capture, full-load, catchup)
- [x] Daemon auto full-load aislado (`needs_full_load` → subprocess; capture/apply **deferred** mientras subprocess activo)
- [x] Daemon cycle: full-load en background thread; pre-apply + apply concurrentes (no bloqueo por bulk pending)
- [x] Stream bookmark Kafka: retry watermark (Not leader for partition) + skip sin tumbar tabla
- [x] Mongo onboard fixes: `seed_mongo_cdc_resume` T0, capture idle poll, `apply_position` seed, discover `mongo_id` PK
- [x] Mongo `apply_batch_stats` parity: `capture_lag` desde `cdc_mongo_resume`, `source_schema=cdc_test`
- [x] Mongo discover/prune parity: `fetch_mongo_objects` sets `source_schema=source_database`; SQL `046` for legacy rows
- [x] Mongo pre-apply DDL parity: `sync_mongo_columns_for_tier` (sample → ADD COLUMN + widen); `ddl_sync_sample_size` in `mongo_load`
- [x] Mongo `apply_batch_stats` backfill: SQL `047` (legacy empty `source_schema`, orphaned catalog_id)
- [x] Mongo dev validated (full-load, stats GREEN, manual I/U/D) — **listo para onboard prod**
- [x] **Full-load hardening (migration 050)** — TRUNCATE verified fail-closed, resumable COPY checkpoints (3 engines), row-count verify, daemon/apply isolation
- [x] **Sprint ops (2026-07-04)** — auto-resume checkpoint, catalog pagination 10k+, `onboard-pending` por `catalog.hot`, apply health alerts, apply_outbox lake-first (051), gate estricto checkpoint+subprocess
- [x] **Reconcile lite (052)** — tabla `reconciliation`, CLI `reconcile-lite`, COUNT/MAX pk/MAX ts, `v_reconciliation_latest`, `reconcile_row_delta` en apply stats; snapshot RR por lado + COUNT gated si `kafka_consumer_lag > 0`
- [x] **Full-load verify baseline snapshot (2026-07-23)** — tablas `capture_during_full_load=true` verifican COPY vs `source_rows` del checkpoint truncate (baseline), no vs count live; gap live−lake = backlog CDC esperado (`verify_mode=baseline_snapshot`, `pending_cdc_gap` en logs)

## Daemon 24/7

```bash
cp config.json.example config.json   # edit credentials
./install.sh                         # Kafka + DataSync daemon (Docker)

docker compose run --rm datasync discover
docker compose run --rm datasync daemon --once   # full-load auto si needs_full_load=true
docker compose logs -f datasync
```

El daemon ejecuta **full-load aislado** (subprocess) cuando el catálogo tiene `needs_full_load=true` para ese conn; mientras hay **subprocess o checkpoint COPY activo**, **capture + pre-apply + apply + onboard se difieren** (`full_load_gate` logs).

Onboard manual por tier (`catalog.hot`):

```bash
docker compose run --rm datasync onboard-pending --conn-id MARIADB_LOCAL --hot-only
docker compose run --rm datasync onboard-pending --cold-only
docker compose run --rm datasync reconcile-lite --conn-id MARIADB_LOCAL --hot-only
```

## Onboard (prod)

```bash
# 1. Migraciones + connections
psql ... -d DataLake -f sql/031_datasync_connections.sql

# 2. Discover — marcar tablas active/cdc_enabled; needs_full_load=true en nuevas
./cpp/build/DataSync discover

# 3. Daemon (full-load + CDC automático por tier)
./install.sh
```

Manual full-load (opcional):
```bash
./cpp/build/DataSync full-load --tier bronze --conn-id MARIADB_LOCAL
./cpp/build/DataSync reconcile-lite --hot-only
```

## Sprint reciente — P0+P1 post-auditoría ronda 2 (2025-06-11)

Fixes aplicados (12 items):

- **Kafka:** COMMIT validation app+lake; catalog lake-key always resolve; lag -1 on probe fail
- **CDC:** catchup full-load gate; mongo error rollback; candidates filter; lock-skip no error
- **Load:** mongo mid-load type widen; MSSQL UTC timestamps; MSSQL tables_skipped parity
- **Infra:** dbcmd literal SQL (no dbfcmd % hazard)

Build: `cpp/build-kafka` → `DataSync` OK.

## Re-auditoría ronda 3 — aplicada (2025-06-11)

Fixes (evitando `mariadb_kafka_capture.cpp` — debug agente paralelo):

- **Kafka:** lake COMMIT antes de audit COMMIT; max offset por valor; parse-skip commit offsets
- **CDC:** catchup reset solo `pos.kafka_partition`; fetch_apply_position via catalog source keys
- **MSSQL/Mongo capture:** flush antes de LSN/resume + mark success; rollback on flush fail
- **Infra:** PgTxn destructor no-throw

Build: `cpp/build-kafka` OK.

## Sprint reciente — P0+P1+P2 mega-fix (2025-07-04)

Auditoría completa aplicada vía 4 subagentes + coordinator:

- **P0:** onboard order fix, catalog-before-lake commit, capture→cycle join, stale cdc_in_progress 300s, produce fail-fast, MSSQL LSN gap reboot, parse-skip no commit, no-PK quarantine, MariaDB typed keyset, staging dedup by kafka offset
- **P1:** FK load order MariaDB, FK apply order, lag scan sampling, N+1 cursor batch, dedup chunk 1k, pre-apply incremental, mongo lock, CLI onboard, daemon timeout runtime, index name hash, round-robin capture
- **P2:** mariadb cdc_in_progress, pre-apply errors, log component cdc_kafka_apply, worker spawn prune, rdkafka stubs fail loud, reconcile scripts deleted

Build: Docker `datasync:local` OK. **Migrate + daemon --once** verificados (2025-07-04).

### Fix migración (mismo día)

- Vistas en migraciones 035/037: `reconciliation_rag` (no `apply_health_rag` pre-rename)
- Migración 045: UPDATE antes de CHECK constraint
- `monitoring_views()`: rename idempotente `reconciliation_rag` → `apply_health_rag`

## Próximo sprint

1. Smoke E2E I/U/D automatizado (opcional)
2. QA Mongo/MSSQL full-load resume en dev
3. Migración catálogo prod por tier (operacional)

## Env vars (Docker)

| Variable | Default | Use |
|----------|---------|-----|
| `DATASYNC_CONFIG` | `/app/config.json` | PG credentials |
| `DATASYNC_RUN_MIGRATIONS` | `0` | `1` = baseline + lake DDL on first install only |
| Incremental migrate | daemon start | `run_cdc_daemon` runs `DataSync migrate` before CDC loop |
| `KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka bootstrap (env only; C++ reads via `resolve_kafka_bootstrap`) |
