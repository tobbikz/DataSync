# CDC Project State (actualizado por Orchestrator)

## Objetivo

Pipeline robusto 24/7: `MariaDB/MSSQL/Mongo → C++ capture → Kafka → C++ apply → PostgreSQL DataLake`

**100% C++** — binario `DataSync`; paquete Python `cdc_kafka/` eliminado.

## Arquitectura DB

| DB | Rol |
|----|-----|
| **DataSync** | catalog, logs, connections, apply_position, apply_batch_stats (no `runtime_config`, no `cdc_applied_events`, no `reconciliation`, no `schema_migrations`) |
| **DataLake** | tablas lake (COPY/INSERT destino) |

Config: **`config.json`** = PG credentials + `cdc` slice. Fuentes OLTP en `cdc_catalog.connections`. Tuning en **`cpp/include/pipeline_defaults.hpp`** (rebuild). Schema = app off + `sql/manual/`.

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
- [x] **Full-load verify baseline snapshot (2026-07-23)** — tablas `capture_during_full_load=true` verifican COPY vs `source_rows` del snapshot (truncate checkpoint, fallback copy/ddl porque worker_id=0 se pisa), compara **lake_rows** vs baseline (no `rows_loaded` en resume); gap live−lake = backlog CDC (`baseline_snapshot_resumed` cuando aplica)
- [x] **Capture binlog multi-schema same table (2026-07-23)** — resolver elige schema exacto cuando catálogo tiene `casino.transactions` + `universal_casino.transactions` (fix SchemaMismatch falso positivo)
- [x] **Capture UTF-8 hardening v2 (2026-07-23)** — `json_dump_for_kafka()` con `error_handler_t::replace`; sanitize en `to_kafka_dict` + `row_dict` + `kafka_message_key_for_row`; fila inválida → warning + skip (no abort slice); SQL `mariadb41_partial_full_load_reset.sql`
- [x] **Full-load streaming verify v3 (2026-07-24)** — `capture_during_full_load`: verify pasa si `lake >= baseline` (CDC concurrente); fail solo si lake << baseline o lake >> live (+5%); resume usa `EXISTS` + `last_pk` (no TRUNCATE por checkpoint/session desync); checkpoint `rows_loaded` acumulativo en resume; SQL `plan_b_mariadb41_casino_transactions_clean_reset.sql`
- [x] **Onboard CDC enable after full-load (2026-07-24)** — `enable_cdc_after_full_load*` ya no exige `status=full_load_in_progress` (usa `last_full_load_at` + `NOT cdc_enabled`); `mark_catalog_cdc_success` no pisa status cuando `cdc_enabled=false` (fix race capture_during_full_load)

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

## Sprint — retention prune + apply_position lock (2026-08-06)

- **Prune:** daemon llama `prune_*_batched` con `p_max_batches=1` en loop (commit por batch + 50ms pause); defaults batch_size 1000 (migration 060). Evita statements de 1–2h+ en scrapers.
- **apply_position:** `upsert_apply_position` ya no hace `DELETE WHERE catalog_id=$1` (solo orphans object_uk); reduce blocking storms / 23505 / 40P01 vs apply UPDATE.

## Sprint — #DataSync alert noise + deposit_note partitions (2026-08-06)

- **Alert kafka_backlog:** exclude MARIADB21 Myaffiliates truncate/reload tables; conn/orphan threshold 50k→100k.
- **Alert apply:** exclude short `apply_stale` from apply_position_unhealthy + apply_health_red.
- **Lake:** `ensure_monthly_partitions` skips overlap; `migrate --lake` always refreshes helpers; apply soft-skips overlap. Manual: `sql/fix_deposit_note_partition_overlap.sql`.

## Sprint — apply_position upsert race fix (2026-08-07)

- **Root cause:** apply hot path called `upsert_apply_position` every slice for all tables; dual UK (`catalog_id` + object_uk) raced under concurrent workers → sticky `catalog.failed` (MSSQL_CRM + MariaDB alerts).
- **Fix:** advisory xact lock on object key + retry 23505/40P01; soft-succeed if row exists; apply `ensure_apply_positions` seeds **only missing** rows.
- **Ops:** `sql/recover_apply_position_object_uk_failed.sql` clears failed status without deleting offsets (MariaDB+MSSQL). Deploy binary → run recover → start daemon.

## Sprint — disk pressure: Docker json logs + Kafka retention (2026-08-07)

- Prod host `/` hit 92%: `datasync-kafka-1` `*-json.log` ~40G (no compose log rotation) + `kafka-data` ~38G (`retention.bytes=1GB`/partition).
- **Fix:** `docker-compose.yml` — `logging` json-file `max-size=100m` / `max-file=3` on kafka+datasync; broker defaults `KAFKA_LOG_RETENTION_MS=6h`, `KAFKA_LOG_RETENTION_BYTES=128MB`. `install.sh kafka-retention` defaults aligned.
- **Ops:** after deploy recreate stack; run `./install.sh kafka-retention` so existing topics pick up new retention (broker env alone does not alter created topics).

## Sprint — PG alert root causes: migrate 003, discover prune, catalog deadlock (2026-08-10)

- **Migration 003/004:** versioned with `schema_migrations` (were re-running ALTER/ADD COLUMN every daemon migrate → ACCESS EXCLUSIVE vs apply). 003 no-ops DDL if `reconciliation_run` already dropped (046).
- **Discover prune:** materialize `tmp_catalog_doomed`, early-exit when empty; batch-delete orphan `cdc_applied_events` (5000/batch).
- **Catalog UPDATEs:** `pg_exec_params_retry_deadlock` on `mark_catalog_cdc_*` (same frequency, 40P01 retry + ROLLBACK); single-statement `mark_catalog_cdc_success` (no prior SELECT).
- **Migration 061:** `v_apply_batch_stats_hourly` + `(logged_at, stat_id)` index; prune ORDER BY `logged_at, stat_id`.

## Sprint — retention 3d + drop apply_outbox (2026-08-10)

- **Retention:** `apply_batch_stats` / `cdc_applied_events` / `logs` → **3 days**; batch 500 / max_batches 10000.
- **Removed `apply_outbox`:** unused lake-first audit buffer (~22GB); apply goes lake COMMIT → audit/position directly. Migration **063** DROPs table; C++ drain/insert path deleted.
- **Idle apply_batch_stats rows:** kept.
- **Telemetry:** enough with `logs` + `apply_batch_stats` + `apply_position` — no new tables; fix Superset to use hourly view / stop full-table SUM.

## Sprint — applied_events prune fix migration 064 (2026-08-11)

- **Root cause:** nightly prune marked complete after ≤5M deletes; 912M rows / oldest 2026-06-29; `ORDER BY event_id` (text) not `applied_at`.
- **Migration 064:** prune by `applied_at, event_id`; runtime batch **5000** / max **50000**; FILLFACTOR apply_position=70, catalog=80.
- **Daemon:** do not mark `retention_maintenance_last_run_date` when prune hits cap (`backlog_remaining`).
- **Ops (manual):** `CREATE INDEX CONCURRENTLY cdc_applied_events_applied_at_event_id_idx`; catch-up prune; `REINDEX TABLE CONCURRENTLY apply_position`. SQL: `sql-queries/monitoring/datasync/sql/pg_phase2_applied_events_prune_ops.sql`.

## Sprint — PG apply CPU/RAM (migration 065, 2026-08-14)

- **Workers:** cold `apply_worker_count` **12 → 6** (24 Kafka partitions / 6). Hot stays 3. Example `slice_max_seconds` **60 → 180**.
- **Connections:** long-lived datasync+lake `PgConn` per apply worker (reconnect on failure); `work_mem`/`temp_buffers` 16MB; `log_pg` = datasync.
- **Stats:** `apply_batch_stats` only for tables with events/flush (no idle rows). `ensure_apply_positions` only worker 0.
- **Logs:** dropped per-slice started / tables-selected / per-batch flushing.
- **Lake staging:** reuse TEMP tables (`TRUNCATE`) per session; delete+insert by business PK unchanged (partition key = `_dl_load_timestamp`).
- **Dropped `cdc_applied_events`:** Kafka offset + lake PK idempotency.
- **Prod `config.json`:** set `cdc.slice_max_seconds` to 180 on the host (file not in git).
- **Build:** `./install.sh` / Docker image on the Linux host (`HAVE_RDKAFKA`). This Windows workstation has no Docker/CMake.

## Sprint — hardcoded knobs + no auto-migrate (2026-08-14)

- **Deleted `cdc_catalog.runtime_config`.** All knobs in `pipeline_defaults.hpp`. Manual DROP: `sql/manual/2026-08-14_drop_runtime_config.sql` (app off).
- **Deleted migrate:** no `DataSync migrate`, no `schema_migrate.cpp`, no `prod_ops_embedded.hpp`, no startup migrate, no `DATASYNC_RUN_MIGRATIONS`. Schema = app off + psql.
- **No hot apply path:** one pool of **4** workers; bucketed topics only; `catalog.hot` is onboard filter only.
- **Workers/batches:** apply **8000**; full-load COPY **20000**; Kafka fetch **50MB / 10MB**; producer queue **500k / 1GB**; lake `statement_timeout` **10 min**.
- **Retention “already ran today”:** `cdc_catalog.logs` (`retention_maintenance` / `scheduled batched retention prune completed`). No knobs table.
- **Keep:** long-lived apply PgConn, 16MB work_mem, idle stats skip, TEMP TRUNCATE reuse, lake delete+insert by business PK.

## Sprint — long-lived Kafka consumer + partition lag (2026-08-14)

- **Consumer:** daemon `apply_worker_loop` holds `KafkaApplySession`; `rd_kafka_new` / subscribe / `ensure_topics` only on first slice or topic-set change. Reset on Kafka/fatal only (not per-table apply errors).
- **Lag:** dropped exact table scan (`datasync-table-lag-scan` extra consumer). Slice end uses `high watermark − offset` on the apply consumer, cached per partition. `lag_kind=partition` in `apply_batch_stats.context`.
- **Logs:** removed `kafka poll progress` every 100 events, first-message, and first-table parse. One `kafka-apply completed` per slice remains.
- **Reports:** `PG/MV/patch_datasync_ops_partition_lag.sql` — `sum_kafka_lag` = SUM of MAX(partition lag) per `(conn, topic, partition)`. Same column names (no Superset remint). Apply on **datasync**.
- **Build:** `./install.sh` / Docker on Linux (`HAVE_RDKAFKA`). SQL 1 (`runtime_config` / `cdc_applied_events`) already applied by ops.

## Sprint — idle stats skip + catalog-first Health + pg_cron prune (2026-08-14)

- **C++:** `insert_apply_batch_stats` no-ops when events/parse/drop/dedup are all zero. Dropped `lake batch committed` and `kafka poll idle no messages` logs.
- **Prune:** DataSync daemon **once per CST hour** — idle/zero `apply_batch_stats` then age 3d (shared **1M** row cap) + logs (**1M**). Batches 500. No pg_cron / no Postgres restart. SQL function: `prune_apply_batch_stats_idle_batched`.
- **Reports:** `PG/MV/patch_datasync_ops_health_catalog_first.sql` — Health = catalog ⋈ apply_position ⋈ capture + last non-zero stats. Quiet = `is_inactive` (no fake GREEN heartbeat). Events/Kafka ignore idle/zero slices. Same chart columns (no remint). Unique Health key = `catalog_id`.
- **Ops order:** rebuild binary → retention SQL on datasync → Health/Events MV patch on datasync.

## Sprint — drop reconciliation + schema_migrations (2026-08-14)

- **Removed `reconcile-lite`:** no CLI, no `reconciliation` table, no `v_reconciliation_latest` JOIN in apply. Health = catalog + apply_position + capture. `apply_batch_stats.reconcile_row_delta` column kept (writes 0; no ALTER on the fat table).
- **Dropped `schema_migrations`:** migrate already gone from the binary; ledger is leftover. Manual SQL: `sql/manual/2026-08-14_drop_reconciliation_schema_migrations.sql` (app off).
- **Deploy:** stop DataSync → apply that SQL on **datasync** → rebuild/restart binary.

## Sprint — drop unused cdc_catalog views (2026-08-14)

- Dropped `v_apply_latest`, `v_apply_stale`, `v_cdc_pipeline_summary`, `v_full_load_progress`, `v_kafka_consumer`, `v_apply_batch_stats_hourly`.
- Binary and DataSync Ops MVs do not read them. SQL: `sql/manual/2026-08-14_drop_cdc_catalog_legacy_views.sql` (also included in the reconciliation drop script). App can stay up for views-only.

## Env vars (Docker)

| Variable | Default | Use |
|----------|---------|-----|
| `DATASYNC_CONFIG` | `/app/config.json` | PG credentials |
| `KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka bootstrap (env only; C++ reads via `resolve_kafka_bootstrap`) |
