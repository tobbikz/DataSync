# DataSync — Roadmap de features (backlog)

> Generado: 2026-07-04. Ideas priorizadas para evaluar/implementar después.
> Estado del pipeline: C++ 100%, reconcile pesado eliminado, health vía `apply_batch_stats` RAG.

---

## Ya en el radar (hacer primero)

- [ ] **Smoke E2E I/U/D automatizado** — script que inserte/actualice/borre en MariaDB/MSSQL/Mongo y valide lake + `apply_health_rag=GREEN`
- [ ] **QA resume full-load (Mongo/MSSQL)** — kill mid-COPY → reanudar desde checkpoint (migración 050)
- [ ] **Migración catálogo prod por tier** — discover → marcar `catalog.hot` → `onboard-pending --hot-only` → cold después

---

## P0 — Ops / confiabilidad

- [ ] **CLI `unquarantine` / `rebootstrap`**
  ```bash
  DataSync unquarantine --conn-id X --schema S --table T [--full-reload]
  ```
  Hoy `quarantine_apply_position()` existe; recovery solo vía SQL manual. Enum `rebootstrap_pending` sin uso en C++.

- [x] **Reconcile lite** — `DataSync reconcile-lite` + tabla `cdc_catalog.reconciliation` (052)
  ```bash
  DataSync reconcile-lite [--conn-id ID] [--hot-only|--cold-only] [--sample-pct N]
  ```

- [ ] **Alertas externas** (fan-out desde RED/AMBER; logs siguen siendo source of truth)
  - Webhook Slack/Teams/PagerDuty, o
  - Exporter Prometheus, o
  - Airflow sensor sobre `v_apply_stale`

- [ ] **Full-load selectivo por tabla**
  ```bash
  DataSync full-load --conn-id X --schema S --table T
  ```
  O `needs_full_load=true` en catálogo sin re-truncar toda la conexión

- [ ] **Playbook `gap_detected` / GTID reboot automatizado**
  Detectar → log + alerta → auto `needs_full_load=true` + pause apply

- [ ] **Bulk catalog management**
  - `discover --dry-run` (diff upsert/prune)
  - Bulk enable/disable CDC por patrón schema
  - Reporte post-discover: tablas sin PK

---

## P1 — Escala y calidad de datos

- [ ] **Schema drift más allá de ADD COLUMN**
  - Column rename, type narrowing, NOT NULL sin default, column drop (política)

- [ ] **Capa raw append-only (opt-in)**
  - Tabla `raw.{schema}_{table}_cdc` con cada evento (op, before, after, offset)
  - Mirror = vista actual; raw = auditoría analítica

- [ ] **SCD Type 2 opt-in** (`engine_meta.scd2=true` o tabla `_history`)

- [ ] **Soft deletes** — mapear `deleted_at` → DELETE lake o `_dl_deleted`

- [ ] **Column masking / exclusion** — PII no viaja a Kafka/lake (`extras.masked_columns`)

- [ ] **Catch-up merge post full-load (hot path)**
  ```bash
  DataSync catchup-merge --catalog-id N
  ```
  Explicitar merge de `stream_kafka_offsets` post-TRUNCATE; MSSQL hoy no usa `capture_during_full_load`

- [ ] **Dead-letter queue (DLQ)**
  - Tabla `cdc_catalog.apply_dlq` para `parse_skipped` / `dropped_unrecoverable`

- [ ] **Buffer FK cross-batch** — `deferred_apply_rows` antes de cuarentena

- [ ] **Composite PK: COPY paralelo** — hoy deshabilitado; cuello de botella en tablas grandes

- [ ] **Allowlist/denylist en discover** — filtro por patrón schema o lista en `connections.extras`

- [ ] **Tags en catálogo** — owner, sla_tier, pii, reload_policy en `engine_meta` o columnas

---

## P2 — Plataforma / integración

- [ ] **`DataSync status`** — health CLI (JSON + exit code)
  - RAG por conn (`v_cdc_pipeline_summary`)
  - Cuarentena, full-load activo (`v_full_load_progress`)
  - Capture lag; exit ≠ 0 si RED

- [ ] **`DataSync pause` / `resume`**
  ```bash
  DataSync pause --conn-id X [--schema S --table T]
  DataSync resume ...
  ```

- [ ] **`DataSync test-connection --conn-id X`**
  - GTID, binlog format, MSSQL CDC, Mongo change streams — antes de onboard masivo

- [ ] **Airflow 3.0 operadores** (orquestar, no reemplazar daemon)
  - DiscoverOperator, OnboardOperator, FullLoadOperator, ReconcileLiteOperator, health sensor

- [x] ~~**Consumidor estándar de `apply_outbox`**~~ — **removed** (migration 063); unused, lake→audit directo

- [ ] **Dashboard Grafana empaquetado** + runbooks Obsidian

- [ ] **Alert deduplication** — ventana de silencio / agregación por conn_id

- [ ] **Lineage en lake CDC** — `_dl_cdc_applied_at`, `_dl_kafka_offset` en mirror

---

## Infra y escala

- [ ] **Captura desde réplica MariaDB** — `connections.extras.binlog_host`

- [ ] **Daemon scoped por conn/tier**
  ```bash
  DataSync daemon --conn-id X
  ```
  O systemd separado hot/cold

- [ ] **Kafka lifecycle ops**
  - `DataSync kafka-reset-offsets` (DR)
  - Purge topics huérfanos al podar catálogo
  - Retention hot (24h) vs cold (7d)

- [ ] **Rotación de secretos** — password desde env; reload connections sin restart

- [ ] **Rebootstrap automático ante binlog purged** — flujo atómico: full-load + reset offsets + `apply_position`

---

## Testing y CI

- [ ] **Suite regresión por motor**

  | Escenario | Valida |
  |-----------|--------|
  | Kill mid-COPY + resume | checkpoint 050 |
  | Binlog gap simulado | reboot flow |
  | Tabla sin PK | quarantine/skip |
  | FK parent/child | apply order |
  | Column add mid-CDC | pre-apply DDL |
  | Hot topic lag | per_table path |

- [ ] **Benchmark harness** — events/min reproducible en CI

- [ ] **Contract tests envelope Kafka** — schema versionado op/before/after

---

## Limpieza técnica (deuda)

- [ ] Eliminar o revivir tablas `reconciliation_*` (decisión reconcile lite)
- [ ] Usar o borrar enum `rebootstrap_pending`
- [ ] Documentar eliminación de `apply_catchup_*` del runtime_config
- [ ] Ampliar tests unitarios (hoy solo `kafka_apply_cell_test.cpp`)

---

## No recomendado ahora

| Feature | Motivo |
|---------|--------|
| Reconcile full checksum PK | Costoso a 700M+ filas; ya eliminado |
| Debezium / otro bus | Arquitectura fijada |
| Más keys en `runtime_config` | Regla: 5 keys canónicas |
| Curated dentro de DataSync | Mejor dbt/SQL/Airflow sobre lake |
| API REST del daemon | CLI + systemd + PG catalog suficiente |

---

## Top 10 consolidado (si hay que elegir)

1. Smoke E2E I/U/D automatizado
2. CLI `unquarantine` / `rebootstrap`
3. Reconcile lite (row-count sample)
4. Alertas externas RED/AMBER
5. Full-load selectivo por tabla
6. `DataSync status`
7. Catch-up merge explícito post full-load hot
8. `discover --dry-run`
9. DLQ persistente
10. Daemon scoped por conn/tier

---

## Referencias en repo

- Estado: `.cursor/skills/cdc-orchestrator/PROJECT-STATE.md`
- Vistas monitoring: `prod_ops_embedded.hpp` → `monitoring_views()`
- Checkpoints full-load: migración 050, `full_load_checkpoint.hpp`
- Outbox lake-first: migración 051
- Health alerts: `scan_apply_health_alerts()` en `cdc_daemon.cpp`
