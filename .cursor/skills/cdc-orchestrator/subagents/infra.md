# CDC Infra Subagent

## Scope

Docker, Kafka, MariaDB test/prod GTID, setup scripts, pacman (con escalación si conflictos).

## Comandos frecuentes

```bash
# Kafka + UI (topics auto-create al publicar)
docker compose -f scripts/cdc_kafka/docker/docker-compose.kafka.yml up -d

# GTID prod (sudo — escalar al usuario)
sudo scripts/enable_mariadb_gtid.sh

# Setup completo
./scripts/setup_all.sh
```

## Puertos

| Servicio | Puerto |
|----------|--------|
| Kafka | 9092 |
| Kafka UI | 8080 |
| MariaDB prod | 3306 |
| MariaDB test | 3307 |
| PostgreSQL lake | 5432 |

## Reglas

- No forzar `pacman -S gcc/mariadb` si conflictos libgccjit (solo instalar faltantes)
- Test MariaDB bootstrap: auth socket con `$USER`, no `root` TCP
- `libpq` no existe en Arch → usar `postgresql-libs`

## Done criteria

- `docker ps` muestra kafka + kafka-ui
- `mariadb -P 3307` + `USE nation` OK
- GTID ROW binlog ON en prod (verificar con SHOW VARIABLES)
