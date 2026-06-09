#!/usr/bin/env bash
# Idempotent native Kafka install (Apache KRaft, no Docker). Run as root.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASYNC_ROOT="${DATASYNC_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"

KAFKA_VERSION="${KAFKA_VERSION:-3.8.0}"
KAFKA_SCALA="${KAFKA_SCALA:-2.13}"
KAFKA_TGZ="kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
KAFKA_INSTALL_DIR="/opt/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}"
KAFKA_CLUSTER_ID="${KAFKA_CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}"
KAFKA_USER="${KAFKA_USER:-kafka}"
KAFKA_DATA="/var/lib/kafka/data"
KAFKA_LOG="/var/log/kafka"
KAFKA_CONFIG="/etc/kafka/server.properties"

detect_java_home() {
  if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
    printf '%s' "${JAVA_HOME}"
    return 0
  fi
  local j
  for j in /usr/lib/jvm/java-17-openjdk /usr/lib/jvm/java-21-openjdk /usr/lib/jvm/java-17; do
    if [[ -x "${j}/bin/java" ]]; then
      printf '%s' "${j}"
      return 0
    fi
  done
  if command -v java >/dev/null 2>&1; then
    dirname "$(dirname "$(readlink -f "$(command -v java)")")"
    return 0
  fi
  return 1
}

ensure_java() {
  if detect_java_home >/dev/null; then
    return 0
  fi
  echo "Installing Java (OpenJDK 17)..." >&2
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y java-17-openjdk-headless
  elif command -v yum >/dev/null 2>&1; then
    yum install -y java-17-openjdk-headless
  elif command -v apt-get >/dev/null 2>&1; then
    apt-get update && apt-get install -y openjdk-17-jre-headless
  elif command -v pacman >/dev/null 2>&1; then
    pacman -Sy --noconfirm jre-openjdk
  else
    echo "Install Java 17+ manually, then re-run" >&2
    exit 1
  fi
  detect_java_home >/dev/null
}

ensure_kafka_user() {
  if id "${KAFKA_USER}" &>/dev/null; then
    return 0
  fi
  useradd -r -m -d /var/lib/kafka -s /sbin/nologin "${KAFKA_USER}"
}

purge_docker_kafka() {
  local root="${DATASYNC_ROOT}" clean="${root}/deploy/systemd/kafka-force-clean.sh"
  [[ -x "$clean" ]] && /bin/bash "$clean" || true
}

install_kafka_tgz() {
  if [[ -L "${KAFKA_HOME}" || -d "${KAFKA_HOME}/bin" ]]; then
    return 0
  fi

  local url="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"
  local tmp
  tmp="$(mktemp -d)"
  echo "Downloading ${url}..." >&2
  curl -fsSL "${url}" -o "${tmp}/${KAFKA_TGZ}"
  rm -rf "${KAFKA_INSTALL_DIR}"
  tar -xzf "${tmp}/${KAFKA_TGZ}" -C /opt
  ln -sfn "${KAFKA_INSTALL_DIR}" "${KAFKA_HOME}"
  rm -rf "${tmp}"
}

install_config() {
  mkdir -p /etc/kafka
  install -m 0644 "${SCRIPT_DIR}/server.properties" "${KAFKA_CONFIG}"
  mkdir -p "${KAFKA_DATA}" "${KAFKA_LOG}"
  chown -R "${KAFKA_USER}:${KAFKA_USER}" /var/lib/kafka /var/log/kafka /etc/kafka
}

format_storage() {
  local java_home meta
  java_home="$(detect_java_home)"
  if [[ -f "${KAFKA_DATA}/meta.properties" ]]; then
    echo "Kafka storage already formatted at ${KAFKA_DATA}" >&2
    return 0
  fi
  echo "Formatting KRaft storage (cluster.id=${KAFKA_CLUSTER_ID})..." >&2
  runuser -u "${KAFKA_USER}" -- \
    env JAVA_HOME="${java_home}" PATH="${java_home}/bin:${PATH}" \
    "${KAFKA_HOME}/bin/kafka-storage.sh" format \
      -t "${KAFKA_CLUSTER_ID}" -c "${KAFKA_CONFIG}" --ignore-formatted
}

install_systemd_unit() {
  local java_home unit_dst="/etc/systemd/system/DataSync-kafka.service"
  java_home="$(detect_java_home)"
  sed -e "s|@DATASYNC_ROOT@|${DATASYNC_ROOT}|g" \
      -e "s|@KAFKA_HOME@|${KAFKA_HOME}|g" \
      -e "s|@JAVA_HOME@|${java_home}|g" \
      "${SCRIPT_DIR}/DataSync-kafka.service" > "${unit_dst}"
  chmod 0644 "${unit_dst}"
  systemctl daemon-reload
}

main() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Run as root: sudo $0" >&2
    exit 1
  fi

  purge_docker_kafka
  ensure_java
  ensure_kafka_user
  install_kafka_tgz
  install_config
  format_storage
  install_systemd_unit

  echo "Native Kafka installed: ${KAFKA_HOME} → localhost:9092 (KRaft)" >&2
  echo "Enable: systemctl enable --now DataSync-kafka.service" >&2
}

main "$@"
