#!/usr/bin/env bash
# Pre-create bucketed Kafka topics for a conn_id (default MARIADB_LOCAL).
set -euo pipefail

CONN_ID="${1:-MARIADB_LOCAL}"
BUCKETS="${2:-64}"
PARTS="${3:-6}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-infra-kafka-1}"

kafka_cmd() {
  if docker ps --format '{{.Names}}' | grep -qx "$KAFKA_CONTAINER"; then
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 "$@"
  elif command -v kafka-topics.sh >/dev/null 2>&1; then
    kafka-topics.sh --bootstrap-server "$BOOTSTRAP" "$@"
  else
    echo "No kafka-topics (docker container $KAFKA_CONTAINER or local kafka-topics.sh)" >&2
    exit 1
  fi
}

echo "Creating ${BUCKETS} topics for ${CONN_ID} (${PARTS} partitions each)..."
for i in $(seq 0 $((BUCKETS - 1))); do
  topic=$(printf "%s.b%04d" "$CONN_ID" "$i")
  kafka_cmd --create --if-not-exists --topic "$topic" \
    --partitions "$PARTS" --replication-factor 1 >/dev/null
done
echo "Done. Topics: ${CONN_ID}.b0000 .. $(printf '%s.b%04d' "$CONN_ID" $((BUCKETS - 1)))"
