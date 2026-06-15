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
- [x] Daemon auto full-load aislado (`needs_full_load` → subprocess, sin mezclar capture/apply)
- [x] Daemon cycle: full-load en background thread; pre-apply + apply concurrentes (no bloqueo por bulk pending)
- [x] Stream bookmark Kafka: retry watermark (Not leader for partition) + skip sin tumbar tabla
- [x] Mongo onboard fixes: `seed_mongo_cdc_resume` T0, capture idle poll, `apply_position` seed, discover `mongo_id` PK
- [x] Mongo `apply_batch_stats` parity: `capture_lag` desde `cdc_mongo_resume`, `source_schema=cdc_test`
- [x] Mongo discover/prune parity: `fetch_mongo_objects` sets `source_schema=source_database`; SQL `046` for legacy rows
- [x] Mongo pre-apply DDL parity: `sync_mongo_columns_for_tier` (sample → ADD COLUMN + widen); `ddl_sync_sample_size` in `mongo_load`
- [x] Mongo `apply_batch_stats` backfill: SQL `047` (legacy empty `source_schema`, orphaned catalog_id)
- [x] Mongo dev validated (full-load, stats GREEN, manual I/U/D) — **listo para onboard prod**

## Daemon 24/7

```bash
cp config.json.example config.json   # edit credentials
./install.sh                         # Kafka + DataSync daemon (Docker)

docker compose run --rm datasync discover
docker compose run --rm datasync daemon --once   # full-load auto si needs_full_load=true
docker compose logs -f datasync
```

El daemon ejecuta **full-load aislado** (subprocess) cuando el catálogo tiene `needs_full_load=true` para ese tier+conn; en ese ciclo **no** mezcla capture/apply.

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

## Próximo sprint

1. Coordinar con agente src (apply hang bronze, debug capture instrumentation)
2. Load P1 restantes (FK composite PK, unsigned INT, Mongo JSONB)
3. Infra medium (runtime reload log, dedup orphan keys)

## Env vars (Docker)

| Variable | Default | Use |
|----------|---------|-----|
| `DATASYNC_CONFIG` | `/app/config.json` | PG credentials |
| `DATASYNC_RUN_MIGRATIONS` | `0` | `1` = apply catalog DDL on start |
| `KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka bootstrap (env only; C++ reads via `resolve_kafka_bootstrap`) |
