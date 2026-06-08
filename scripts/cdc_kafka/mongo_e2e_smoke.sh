#!/usr/bin/env bash
# Mongo CDC E2E smoke (C++ DataSync). Stop DataSync daemon first — libmongoc cannot run twice.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${ROOT}/cpp/build/DataSync"
CONFIG="${ROOT}/config.json"
CONN_ID="${CONN_ID:-MONGO_LOCAL}"
TIER="${TIER:-bronze}"
MONGO_URI="${MONGO_URI:-mongodb://localhost:27017/cdc_test?replicaSet=rs0}"

if [[ ! -x "$BIN" ]]; then
  echo "Build DataSync first: cmake --build ${ROOT}/cpp/build"
  exit 1
fi

if pgrep -x DataSync >/dev/null 2>&1; then
  echo "ERROR: DataSync daemon is running. Stop it first (libmongoc handshake conflict):"
  echo "  sudo systemctl stop DataSync"
  exit 1
fi

echo "== Mongo docker =="
docker compose -f "${ROOT}/scripts/cdc_kafka/docker/docker-compose.mongo.yml" up -d

echo "== Discover + full-load =="
"$BIN" discover --config "$CONFIG"
"$BIN" full-load --tier "$TIER" --conn-id "$CONN_ID" --config "$CONFIG"

echo "== CDC insert =="
mongosh --quiet "$MONGO_URI" --eval '
db.users.insertOne({name: "smoke-" + Date.now(), email: "smoke@test.com"});
print("mongo count", db.users.countDocuments());
'

echo "== Capture + apply =="
"$BIN" capture --conn-id "$CONN_ID" --tier "$TIER" --config "$CONFIG"
"$BIN" kafka-apply --conn-id "$CONN_ID" --tier "$TIER" --config "$CONFIG"

echo "== Reconcile (full) =="
export RECONCILE_MODE=full
"$BIN" reconcile --conn-id "$CONN_ID" --tier "$TIER" --config "$CONFIG"

echo "== Lake row count =="
psql "${PGDATABASE:-DataLake}" -c "SELECT count(*) AS lake_users FROM cdc_test.users;"

echo "OK — restart daemon when done:"
echo "  sudo systemctl start DataSync"
