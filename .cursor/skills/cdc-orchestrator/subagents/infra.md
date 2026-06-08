# Infra subagent

## Stack (single path)

```bash
cp config.json.example config.json   # edit password
./install.sh
```

- Compose: `docker-compose.yml` (Kafka + DataSync daemon)
- DataSync uses **`network_mode: host`** on Linux → `localhost:5432` (PG) + `localhost:9092` (Kafka)
- Kafka UI: http://localhost:8080

## MariaDB GTID (host, not in compose)

Enable on source MariaDB manually when onboarding prod sources.

## Config

- `config.json` at repo root (copy from `config.json.example`)
- PostgreSQL is **external** (host or remote)
