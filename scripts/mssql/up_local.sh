#!/usr/bin/env bash
# Start local SQL Server (Docker) and apply CDC dev schema.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE="${ROOT}/scripts/cdc_kafka/docker/docker-compose.mssql.yml"
INIT_SQL="${ROOT}/scripts/cdc_kafka/docker/mssql/init/01-setup.sql"
export MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-Yucaquemada1}"

cd "$ROOT"

echo "=== Starting SQL Server on localhost:1433 (MSSQL_AGENT_ENABLED=true) ==="
docker stop datalake-mssql 2>/dev/null || true
docker rm datalake-mssql 2>/dev/null || true
docker compose -f "$COMPOSE" up -d

echo "=== Waiting for healthcheck ==="
for i in $(seq 1 60); do
  status="$(docker inspect -f '{{.State.Health.Status}}' datalake-mssql 2>/dev/null || echo starting)"
  if [[ "$status" == "healthy" ]]; then
    break
  fi
  sleep 2
done

if [[ "${status:-}" != "healthy" ]]; then
  echo "MSSQL container not healthy yet. Logs:" >&2
  docker logs datalake-mssql --tail 40 >&2 || true
  exit 1
fi

echo "=== Enable SQL Server Agent (requires MSSQL_AGENT_ENABLED=true + container recreate) ==="
docker exec -i datalake-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C \
  -i /dev/stdin < "${ROOT}/scripts/mssql/enable_agent.sql" || true

echo "=== Agent service status ==="
docker exec datalake-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -W -s "|" -Q \
  "SELECT servicename, status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQLServerAgent%'"

echo "=== Applying init SQL (login + datalake_cdc + CDC table) ==="
docker exec -i datalake-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C \
  -i /dev/stdin < "$INIT_SQL"

echo "=== Verify CDC change tables ==="
docker exec datalake-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -d datalake_cdc -W -s "|" -Q \
  "SELECT capture_instance, OBJECT_SCHEMA_NAME(source_object_id), OBJECT_NAME(source_object_id)
   FROM cdc.change_tables"

echo ""
echo "MSSQL ready. If Agent is Stopped, recreate container once:"
echo "  docker compose -f scripts/cdc_kafka/docker/docker-compose.mssql.yml up -d --force-recreate"
echo ""
echo "Discover (CDC tables only):"
echo "  ./cpp/build/DataSync discover"
