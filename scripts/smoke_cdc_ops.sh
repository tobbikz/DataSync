#!/usr/bin/env bash
# Full-load (with Kafka onboard) + CDC INSERT/UPDATE/DELETE for MSSQL and MongoDB.
# Usage: SMOKE_TIER=bronze ./scripts/smoke_cdc_ops.sh [config.json]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="${1:-${DATASYNC_CONFIG:-$ROOT/config.json}}"
TIER="${SMOKE_TIER:-bronze}"
BIN="${DATASYNC_BIN:-$ROOT/cpp/build/DataSync}"

# shellcheck source=scripts/container-compose.sh
source "$ROOT/scripts/container-compose.sh"

run_cli() {
  if [[ -x "$BIN" ]]; then
    DATASYNC_CONFIG="$CONFIG" "$BIN" "$@"
  else
    ensure_container_runtime || { echo "✖ need $BIN or podman/docker" >&2; exit 1; }
    docker_compose run --rm \
      -e DATASYNC_CONFIG=/app/config.json \
      -e DATASYNC_HOST_NETWORK=1 \
      -e KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}" \
      -v "$CONFIG:/app/config.json:ro" \
      datasync "$@"
  fi
}

pg_env() {
  python3 - "$CONFIG" "$1" <<'PY'
import json, os, sys
cfg = json.load(open(sys.argv[1]))
ds = cfg[sys.argv[2]]
print(ds["host"], ds["port"], ds["database"], ds["user"], ds["password"])
PY
}

pg_sql() {
  read -r h p d u pw < <(pg_env datasync)
  export PGPASSWORD="$pw"
  psql -h "$h" -p "$p" -U "$u" -d "$d" -At "$@"
}

lake_sql() {
  read -r h p d u pw < <(pg_env datalake)
  export PGPASSWORD="$pw"
  psql -h "$h" -p "$p" -U "$u" -d "$d" -At "$@"
}

reset_mssql_source() {
  docker exec datalake-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Yucaquemada1' -C -d cdc_test -Q "
    DELETE FROM dbo.smoke_cdc;
    SET IDENTITY_INSERT dbo.smoke_cdc ON;
    INSERT INTO dbo.smoke_cdc (id,name) VALUES (1,'alpha'),(2,'beta'),(3,'gamma');
    SET IDENTITY_INSERT dbo.smoke_cdc OFF;
  " >/dev/null
}

reset_mongo_source() {
  docker exec datalake-mongo mongosh --quiet cdc_test --eval "
    db.smoke_cdc.drop();
    db.smoke_cdc.insertMany([{name:'alpha'},{name:'beta'},{name:'gamma'}]);
  " >/dev/null
  pg_sql -c "DELETE FROM cdc_catalog.cdc_mongo_resume WHERE conn_id='MONGO_LOCAL';" >/dev/null
}

mssql_src_count() { docker exec datalake-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Yucaquemada1' -C -d cdc_test -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM dbo.smoke_cdc;" -h -1 | tr -d ' \r\n'; }
mssql_lake_count() { lake_sql -c "SELECT count(*) FROM cdc_test_dbo.smoke_cdc;"; }
mongo_src_count() { docker exec datalake-mongo mongosh --quiet cdc_test --eval "print(db.smoke_cdc.countDocuments())" | tail -1 | tr -d '\r\n'; }
mongo_lake_count() { lake_sql -c "SELECT count(*) FROM cdc_test.smoke_cdc;"; }

prepare_full_load() {
  local conn_id="$1"
  pg_sql -c "UPDATE cdc_catalog.catalog SET active=true, needs_full_load=true, cdc_enabled=false WHERE conn_id='${conn_id}' AND source_table='smoke_cdc';"
  run_cli discover >/dev/null
  run_cli full-load --tier "$TIER" --conn-id "$conn_id"
}

cdc_round() {
  local conn_id="$1"
  local i
  # Two capture/apply passes: MSSQL CDC scan + Kafka apply slice can lag one round.
  for i in 1 2; do
    run_cli capture --tier "$TIER" --conn-id "$conn_id" >/dev/null
    run_cli kafka-apply --tier "$TIER" --conn-id "$conn_id" >/dev/null
  done
}

test_engine() {
  local conn_id="$1"
  local engine="$2"
  echo ""
  echo "━━ CDC ops $conn_id ($engine) tier=$TIER ━━"

  if [[ "$engine" == "mssql" ]]; then
    reset_mssql_source
    prepare_full_load "$conn_id"
    local src lake updated_cnt
    src="$(mssql_src_count)"; lake="$(mssql_lake_count)"
    echo "  full-load: src=$src lake=$lake"
    [[ "$src" == "$lake" ]] || { echo "✖ full-load mismatch"; return 1; }

    docker exec datalake-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Yucaquemada1' -C -d cdc_test -Q "INSERT INTO dbo.smoke_cdc (name) VALUES ('cdc_insert_test');" >/dev/null
    cdc_round "$conn_id"
    src="$(mssql_src_count)"; lake="$(mssql_lake_count)"
    echo "  INSERT: src=$src lake=$lake"; [[ "$src" == "$lake" ]] || return 1

    docker exec datalake-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Yucaquemada1' -C -d cdc_test -Q "UPDATE dbo.smoke_cdc SET name='cdc_updated' WHERE name='cdc_insert_test';" >/dev/null
    cdc_round "$conn_id"
    src="$(mssql_src_count)"; lake="$(mssql_lake_count)"
    updated_cnt="$(lake_sql -c "SELECT count(*) FROM cdc_test_dbo.smoke_cdc WHERE name='cdc_updated';")"
    echo "  UPDATE: src=$src lake=$lake updated_rows=$updated_cnt"
    [[ "$src" == "$lake" && "$updated_cnt" == "1" ]] || return 1

    docker exec datalake-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Yucaquemada1' -C -d cdc_test -Q "DELETE FROM dbo.smoke_cdc WHERE name='cdc_updated';" >/dev/null
    cdc_round "$conn_id"
    src="$(mssql_src_count)"; lake="$(mssql_lake_count)"
    echo "  DELETE: src=$src lake=$lake"; [[ "$src" == "$lake" ]] || return 1
  elif [[ "$engine" == "mongodb" ]]; then
    reset_mongo_source
    prepare_full_load "$conn_id"
    local src lake updated_cnt
    src="$(mongo_src_count)"; lake="$(mongo_lake_count)"
    echo "  full-load: src=$src lake=$lake"
    [[ "$src" == "$lake" ]] || { echo "✖ full-load mismatch"; return 1; }

    docker exec datalake-mongo mongosh --quiet cdc_test --eval "db.smoke_cdc.insertOne({name:'cdc_insert_test'})" >/dev/null
    cdc_round "$conn_id"
    src="$(mongo_src_count)"; lake="$(mongo_lake_count)"
    echo "  INSERT: src=$src lake=$lake"; [[ "$src" == "$lake" ]] || return 1

    docker exec datalake-mongo mongosh --quiet cdc_test --eval "db.smoke_cdc.updateOne({name:'cdc_insert_test'}, {\$set:{name:'cdc_updated'}})" >/dev/null
    cdc_round "$conn_id"
    src="$(mongo_src_count)"; lake="$(mongo_lake_count)"
    updated_cnt="$(lake_sql -c "SELECT count(*) FROM cdc_test.smoke_cdc WHERE name='cdc_updated';")"
    echo "  UPDATE: src=$src lake=$lake updated_rows=$updated_cnt"
    [[ "$src" == "$lake" && "$updated_cnt" == "1" ]] || return 1

    docker exec datalake-mongo mongosh --quiet cdc_test --eval "db.smoke_cdc.deleteOne({name:'cdc_updated'})" >/dev/null
    cdc_round "$conn_id"
    src="$(mongo_src_count)"; lake="$(mongo_lake_count)"
    echo "  DELETE: src=$src lake=$lake"; [[ "$src" == "$lake" ]] || return 1
  else
    echo "✖ unsupported engine: $engine"; return 1
  fi
  echo "✔ $conn_id CDC INSERT/UPDATE/DELETE OK"
}

main() {
  [[ -f "$CONFIG" ]] || { echo "✖ missing config: $CONFIG" >&2; exit 1; }
  failed=0
  test_engine MSSQL_LOCAL mssql || failed=$((failed + 1))
  test_engine MONGO_LOCAL mongodb || failed=$((failed + 1))
  if [[ "$failed" -gt 0 ]]; then
    echo "✖ smoke_cdc_ops failed ($failed engine(s))" >&2
    exit 1
  fi
  echo ""
  echo "✔ smoke_cdc_ops passed (MSSQL + MongoDB)"
}

main "$@"
