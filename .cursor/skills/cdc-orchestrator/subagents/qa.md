# QA subagent

## Smoke

```bash
./install.sh
```

## Stress (manual)

Use `docker compose run --rm datasync` with load on source DBs; monitor `cdc_catalog.apply_batch_stats` and Kafka UI.
