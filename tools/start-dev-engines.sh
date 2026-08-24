#!/usr/bin/env bash
# Start MSSQL + MongoDB dev containers and bootstrap schema/CDC/replica set.
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"

MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-DataSync_Dev1!}"
export MSSQL_SA_PASSWORD

log() { printf '[dev-engines] %s\n' "$*"; }

log "starting mssql-test + mongodb-test (host network)"
docker compose -f docker-compose.dev-engines.yml up -d

log "waiting for MSSQL sqlcmd..."
for i in $(seq 1 90); do
  if docker exec -e MSSQL_SA_PASSWORD="$MSSQL_SA_PASSWORD" datasync-mssql-test \
    /opt/mssql-tools18/bin/sqlcmd -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -Q "SELECT 1" -b -o /dev/null 2>/dev/null; then
    break
  fi
  sleep 2
done
docker exec -e MSSQL_SA_PASSWORD="$MSSQL_SA_PASSWORD" datasync-mssql-test \
  /opt/mssql-tools18/bin/sqlcmd -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -Q "SELECT 1" -b -o /dev/null \
  || { log "MSSQL not ready"; docker logs datasync-mssql-test 2>&1 | tail -30; exit 1; }

log "applying MSSQL init.sql (testdb + CDC)"
docker cp "$ROOT/tools/dev-mssql/init.sql" datasync-mssql-test:/tmp/init.sql
docker exec -e MSSQL_SA_PASSWORD="$MSSQL_SA_PASSWORD" datasync-mssql-test \
  /opt/mssql-tools18/bin/sqlcmd -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -i /tmp/init.sql

log "waiting for mongodb health..."
for i in $(seq 1 60); do
  if docker inspect datasync-mongodb-test --format '{{.State.Health.Status}}' 2>/dev/null | grep -qx healthy; then
    break
  fi
  sleep 2
done
docker inspect datasync-mongodb-test --format '{{.State.Health.Status}}' | grep -qx healthy \
  || { log "MongoDB not healthy"; docker logs datasync-mongodb-test 2>&1 | tail -30; exit 1; }

log "init MongoDB replica set + sample docs"
docker cp "$ROOT/tools/dev-mongodb/init-rs.js" datasync-mongodb-test:/tmp/init-rs.js
docker exec datasync-mongodb-test mongosh --quiet /tmp/init-rs.js

log "dev engines ready (127.0.0.1:1433 MSSQL, 127.0.0.1:27017 MongoDB rs0)"
