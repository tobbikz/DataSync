#!/usr/bin/env bash
# Apply CDC topic retention to existing Kafka topics (broker defaults do NOT retroactively alter topics).
# Run after docker-compose retention change or on prod once to cap disk growth.
#
# Defaults: 3 days, 1 GiB per partition, 128 MiB segment roll (matches docker-compose.yml + ensure_kafka_topics_exist).
#
# Usage:
#   KAFKA_BOOTSTRAP=127.0.0.1:9092 ./scripts/kafka_set_topic_retention.sh
#   KAFKA_BOOTSTRAP=127.0.0.1:9092 TOPIC_PREFIX=MARIADB01 ./scripts/kafka_set_topic_retention.sh

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
RETENTION_MS="${KAFKA_RETENTION_MS:-259200000}"
RETENTION_BYTES="${KAFKA_RETENTION_BYTES:-1073741824}"
SEGMENT_BYTES="${KAFKA_SEGMENT_BYTES:-134217728}"
TOPIC_PREFIX="${TOPIC_PREFIX:-}"

if command -v docker >/dev/null 2>&1; then
  KAFKA_TOPICS=(docker compose exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP")
  KAFKA_CONFIGS=(docker compose exec -T kafka kafka-configs --bootstrap-server "$BOOTSTRAP")
elif command -v kafka-topics >/dev/null 2>&1; then
  KAFKA_TOPICS=(kafka-topics --bootstrap-server "$BOOTSTRAP")
  KAFKA_CONFIGS=(kafka-configs --bootstrap-server "$BOOTSTRAP")
else
  echo "kafka-topics not found (install Kafka CLI or use docker compose)" >&2
  exit 1
fi

CONFIG="retention.ms=${RETENTION_MS},retention.bytes=${RETENTION_BYTES},segment.bytes=${SEGMENT_BYTES},cleanup.policy=delete"

mapfile -t TOPICS < <("${KAFKA_TOPICS[@]}" --list 2>/dev/null | sort)
if [[ ${#TOPICS[@]} -eq 0 ]]; then
  echo "No topics found at ${BOOTSTRAP}" >&2
  exit 1
fi

count=0
for topic in "${TOPICS[@]}"; do
  [[ -n "$topic" ]] || continue
  if [[ -n "$TOPIC_PREFIX" && "$topic" != "${TOPIC_PREFIX}"* ]]; then
    continue
  fi
  # Skip internal topics unless explicitly needed
  if [[ "$topic" == __* ]]; then
    continue
  fi
  echo "Alter ${topic} -> ${CONFIG}"
  "${KAFKA_CONFIGS[@]}" \
    --entity-type topics \
    --entity-name "$topic" \
    --alter \
    --add-config "$CONFIG"
  count=$((count + 1))
done

echo "Done: ${count} topic(s) updated."
echo "Disk may not shrink until log cleaner runs (retention.check.interval.ms, default 5m)."
echo "Verify: kafka-log-dirs --bootstrap-server ${BOOTSTRAP} --describe | jq '.brokers[].logDirs'"
