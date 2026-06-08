# CDC QA Subagent

## Scope

Stress test CDC: daemon + `cdc_stress_load.sh` (carga + métricas integradas).

Ver Obsidian [[CDC Kafka Architecture]] § Stress test CDC.

## Comandos

```bash
cd ~/Downloads/project
export PYTHONUNBUFFERED=1 PGPASSWORD=Yucaquemada1 MARIADB_PASSWORD=Yucaquemada1

# T1
python3 -m cdc_kafka daemon --tier hot --config cpp/config.cdc-test.json --worker-count 4

# T2 (load + metrics cada 5s)
scripts/cdc_kafka/cdc_stress_load.sh
```

## Criterios

| Check | Esperado |
|-------|----------|
| load rows/s | estable |
| `metrics errors=` | 0 |
| `stress_lag` | no crece sin tope bajo carga |
