#!/usr/bin/env bash
# Pre-start checks for native Kafka (storage, dirs, port).
set -euo pipefail

KAFKA_DATA="${KAFKA_DATA:-/var/lib/kafka/data}"
KAFKA_CONFIG="${KAFKA_CONFIG:-/etc/kafka/server.properties}"
KAFKA_PORT="${KAFKA_PORT:-9092}"

if [[ ! -f "${KAFKA_CONFIG}" ]]; then
  echo "Missing ${KAFKA_CONFIG} — run: sudo deploy/kafka/install-kafka.sh" >&2
  exit 1
fi

if [[ ! -f "${KAFKA_DATA}/meta.properties" ]]; then
  echo "Kafka storage not formatted — run: sudo deploy/kafka/install-kafka.sh" >&2
  exit 1
fi

if (echo >/dev/tcp/127.0.0.1/"${KAFKA_PORT}") 2>/dev/null; then
  echo "Port ${KAFKA_PORT} already in use — stop other Kafka before starting DataSync-kafka" >&2
  exit 1
fi

exit 0
