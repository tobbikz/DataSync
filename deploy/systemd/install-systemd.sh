#!/usr/bin/env bash
# Install DataSync systemd units + native Kafka (no Docker for broker).
# Usage: sudo deploy/systemd/install-systemd.sh [--no-enable]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SYSTEMD_DIR="/etc/systemd/system"
ENV_DIR="/etc/datasync"
ENV_FILE="${ENV_DIR}/datasync.env"
DO_ENABLE=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-enable) DO_ENABLE=0; shift ;;
    -h|--help)
      cat <<EOF
Usage: sudo $0 [--no-enable]

Installs native Kafka (DataSync-kafka.service) + DataSync.service (Podman daemon).
Reconcile runs inside the daemon.

  --no-enable  install units only (no systemctl enable --now)
EOF
      exit 0
      ;;
    --native)
      echo "Removed: --native mode." >&2
      exit 2
      ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo $0" >&2
  exit 1
fi

DATASYNC_USER="datalake"
DATASYNC_GROUP="datalake"
if ! id "${DATASYNC_USER}" &>/dev/null; then
  echo "System user ${DATASYNC_USER} not found. Create it first, e.g.:" >&2
  echo "  sudo useradd -r -m -d /opt/datalake -s /bin/bash ${DATASYNC_USER}" >&2
  exit 1
fi
DATASYNC_UID="$(id -u "${DATASYNC_USER}")"
RUNTIME_DIR="/run/user/${DATASYNC_UID}"

disable_legacy_units() {
  local unit removed=0
  for unit in datalake-cdc.service DataSync-reconcile.service DataSync-reconcile.timer datasync.service; do
    if [[ -f "${SYSTEMD_DIR}/${unit}" ]]; then
      systemctl disable --now "${unit}" 2>/dev/null || true
      rm -f "${SYSTEMD_DIR}/${unit}"
      echo "Removed legacy unit: ${unit}"
      removed=1
    fi
  done
  while IFS= read -r unit; do
    [[ -n "${unit}" ]] || continue
    systemctl disable --now "${unit}" 2>/dev/null || true
    echo "Disabled legacy instance: ${unit}"
    removed=1
  done < <(systemctl list-units --all --no-legend 'cdc-kafka@*' 2>/dev/null | awk '{print $1}')
  while IFS= read -r unit; do
    [[ -n "${unit}" ]] || continue
    systemctl disable --now "${unit}" 2>/dev/null || true
    echo "Disabled legacy instance: ${unit}"
    removed=1
  done < <(systemctl list-units --all --no-legend 'cdc-kafka-reconcile@*' 2>/dev/null | awk '{print $1}')
  for unit in cdc-kafka@.service cdc-kafka-reconcile@.service; do
    if [[ -f "${SYSTEMD_DIR}/${unit}" ]]; then
      rm -f "${SYSTEMD_DIR}/${unit}"
      echo "Removed legacy unit: ${unit}"
      removed=1
    fi
  done
  [[ "${removed}" -eq 1 ]] && systemctl daemon-reload
}

render_unit() {
  local src="$1" dst="$2"
  sed -e "s|@DATASYNC_ROOT@|${ROOT}|g" \
      -e "s|@DATASYNC_UID@|${DATASYNC_UID}|g" \
      "${src}" > "${dst}"
}

ensure_podman_user_socket() {
  command -v podman >/dev/null 2>&1 || return 0
  [[ -S "${RUNTIME_DIR}/podman/podman.sock" ]] && return 0

  loginctl enable-linger "${DATASYNC_USER}" 2>/dev/null || true
  if command -v runuser >/dev/null 2>&1; then
    runuser -u "${DATASYNC_USER}" -- \
      env XDG_RUNTIME_DIR="${RUNTIME_DIR}" \
      systemctl --user start podman.socket 2>/dev/null || true
  fi
}

sync_env_file() {
  render_unit "${SCRIPT_DIR}/DataSync.env.example" "${ENV_FILE}"
  if command -v podman >/dev/null 2>&1; then
    local podman_sock="unix://${RUNTIME_DIR}/podman/podman.sock"
    if grep -q '^DOCKER_HOST=' "${ENV_FILE}"; then
      sed -i "s|^DOCKER_HOST=.*|DOCKER_HOST=${podman_sock}|" "${ENV_FILE}"
    else
      echo "DOCKER_HOST=${podman_sock}" >> "${ENV_FILE}"
    fi
  elif grep -q '^DOCKER_HOST=' "${ENV_FILE}"; then
    sed -i '/^DOCKER_HOST=/d' "${ENV_FILE}"
  fi
  chmod 0644 "${ENV_FILE}"
}

chmod +x "${SCRIPT_DIR}"/datasync-*.sh
chmod +x "${ROOT}/deploy/kafka/"*.sh

mkdir -p "${ENV_DIR}"
sync_env_file
echo "Synced ${ENV_FILE} (DATASYNC_ROOT=${ROOT}, user=${DATASYNC_USER})"

ensure_podman_user_socket

if [[ ! -r "${ROOT}/config.json" ]]; then
  echo "WARN: ${DATASYNC_USER} cannot read ${ROOT}/config.json — chown/chmod for systemd" >&2
fi

echo "Installing native Apache Kafka (KRaft)..." >&2
DATASYNC_ROOT="${ROOT}" /bin/bash "${ROOT}/deploy/kafka/install-kafka.sh"

render_unit "${SCRIPT_DIR}/DataSync.service" "${SYSTEMD_DIR}/DataSync.service"

disable_legacy_units

systemctl daemon-reload
systemctl reset-failed DataSync-kafka.service DataSync.service 2>/dev/null || true

# shellcheck source=deploy/systemd/datasync-lib.sh
source "${SCRIPT_DIR}/datasync-lib.sh"
stop_compose_datasync

if [[ "${DO_ENABLE}" -eq 1 ]]; then
  systemctl enable DataSync-kafka.service DataSync.service
  systemctl restart DataSync-kafka.service
  sleep 5
  if ! systemctl is-active --quiet DataSync-kafka.service; then
    echo "ERROR: DataSync-kafka.service failed — journalctl -u DataSync-kafka -n 40 --no-pager" >&2
    exit 1
  fi
  if ! kafka_tcp_ok; then
    echo "ERROR: Kafka not listening on localhost:9092" >&2
    exit 1
  fi
  systemctl restart DataSync.service
  echo ""
  echo "Enabled: DataSync-kafka (native) + DataSync.service (user=${DATASYNC_USER})"
  if ! systemctl is-active --quiet DataSync.service; then
    echo "ERROR: DataSync.service failed — journalctl -u DataSync -n 40 --no-pager" >&2
    exit 1
  fi
else
  echo ""
  echo "Units installed (not enabled — omit --no-enable to start)."
fi

cat <<EOF

Installed:
  ${SYSTEMD_DIR}/DataSync-kafka.service  (native Apache Kafka @ localhost:9092)
  ${SYSTEMD_DIR}/DataSync.service        (Podman datasync daemon)
  ${ENV_FILE}
  /etc/kafka/server.properties

Restart:
  sudo systemctl restart DataSync-kafka
  sudo systemctl restart DataSync

Status:
  systemctl status DataSync-kafka DataSync
  ss -tlnp | grep 9092

Logs:
  journalctl -u DataSync-kafka -f
  cd ${ROOT} && podman compose logs -f datasync

EOF
