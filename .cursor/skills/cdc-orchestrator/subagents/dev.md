# CDC DEV Subagent

## Scope

- `cdc_kafka/` — capture, apply, fairness, recovery, envelope, binlog_reader
- `cpp/` — datalake-catalog full-load, legacy cdc v1
- `sql/007`–`010`, `sql/029_mariadb_cdc_meta.sql`
- Fixes mínimos; match estilo existente

## Prioridades (ver PROJECT-STATE.md)

1. FK-safe apply: topological order by table deps, or buffer/defer child rows until parent exists
2. Fairness: no `all_targets_met` until partition idle or `apply_max_events` slice budget
3. Capture: fail fast when Kafka unreachable (check delivery callback / flush errors)

## Convenciones

- Type hints, PEP8 en Python
- No Debezium
- Test config: `cpp/config.cdc-test.json`, MariaDB `:3307`
- No tocar prod `:3306` sin escalación al Orchestrator

## Done criteria

- Cambio acotado + rationale en comentario solo si no obvio
- Verificar build `DataSync` tras cambios C++
- Orchestrator corre QA después

## No hacer

- Commits sin pedir
- Refactors grandes no relacionados
- Cambiar slice prod a 60s
