#!/usr/bin/env bash
# Start Postgres lab + MariaDB/MSSQL/MongoDB and bootstrap schemas.
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"

# Git Bash on Windows rewrites /opt/... into a host path; keep container paths literal.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-DataSync_Dev1!}"
export MSSQL_SA_PASSWORD
MARIADB_ROOT_PASSWORD="${MARIADB_ROOT_PASSWORD:-DataSync_Dev1!}"
SQLCMD=/opt/mssql-tools18/bin/sqlcmd

log() { printf '[dev-engines] %s\n' "$*"; }

log "starting postgres-lab + mariadb-test + mssql-test + mongodb-test (host network)"
docker compose -f docker-compose.dev-engines.yml up -d

log "waiting for Postgres..."
for i in $(seq 1 60); do
  if docker exec datasync-postgres-lab pg_isready -U datasync -h 127.0.0.1 >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker exec datasync-postgres-lab pg_isready -U datasync -h 127.0.0.1 \
  || { log "Postgres not ready"; docker logs datasync-postgres-lab 2>&1 | tail -40; exit 1; }

# Volume may already exist without init script DBs — ensure them.
# (Avoid \gexec via docker exec heredoc — unreliable on Windows/Git Bash.)
ensure_pg_db() {
  local db="$1"
  local exists
  exists="$(docker exec -e PGPASSWORD=DataSync_Dev1! datasync-postgres-lab \
    psql -U datasync -h 127.0.0.1 -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname='${db}'" || true)"
  if [[ "$exists" != "1" ]]; then
    log "creating Postgres database ${db}"
    docker exec -e PGPASSWORD=DataSync_Dev1! datasync-postgres-lab \
      psql -U datasync -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=1 \
      -c "CREATE DATABASE \"${db}\" OWNER datasync"
  fi
}
ensure_pg_db datasync
ensure_pg_db DataLake

log "waiting for MariaDB..."
for i in $(seq 1 60); do
  if docker exec datasync-mariadb-test \
    mariadb -uroot -p"$MARIADB_ROOT_PASSWORD" -e "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker exec datasync-mariadb-test \
  mariadb -uroot -p"$MARIADB_ROOT_PASSWORD" -e "SELECT 1" >/dev/null \
  || { log "MariaDB not ready"; docker logs datasync-mariadb-test 2>&1 | tail -40; exit 1; }

# Re-apply grants/tables (idempotent) in case volume predates init SQL.
docker exec -i datasync-mariadb-test \
  mariadb -uroot -p"$MARIADB_ROOT_PASSWORD" < "$ROOT/tools/dev-mariadb/01-init.sql"

log "waiting for MSSQL sqlcmd..."
for i in $(seq 1 90); do
  if docker exec -e MSSQL_SA_PASSWORD="$MSSQL_SA_PASSWORD" datasync-mssql-test \
    "$SQLCMD" -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -Q "SELECT 1" -b -o /dev/null 2>/dev/null; then
    break
  fi
  sleep 2
done
docker exec -e MSSQL_SA_PASSWORD="$MSSQL_SA_PASSWORD" datasync-mssql-test \
  "$SQLCMD" -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -Q "SELECT 1" -b -o /dev/null \
  || { log "MSSQL not ready"; docker logs datasync-mssql-test 2>&1 | tail -30; exit 1; }

# docker.exe on Windows needs a native host path; Git Bash /c/... becomes C:\c:...
host_path() {
  local p="$1"
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "$p"
  else
    printf '%s' "$p"
  fi
}

log "applying MSSQL init.sql (testdb + CDC)"
docker cp "$(host_path "$ROOT/tools/dev-mssql/init.sql")" datasync-mssql-test:/tmp/init.sql
docker exec -e MSSQL_SA_PASSWORD="$MSSQL_SA_PASSWORD" datasync-mssql-test \
  "$SQLCMD" -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -i /tmp/init.sql

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
docker cp "$(host_path "$ROOT/tools/dev-mongodb/init-rs.js")" datasync-mongodb-test:/tmp/init-rs.js
docker exec datasync-mongodb-test mongosh --quiet /tmp/init-rs.js

log "dev engines ready:"
log "  PG 127.0.0.1:5432 (datasync / DataLake) user=datasync"
log "  MariaDB 127.0.0.1:3306 testdb user=datasync"
log "  MSSQL 127.0.0.1:1433 testdb sa"
log "  MongoDB 127.0.0.1:27017 rs0"
