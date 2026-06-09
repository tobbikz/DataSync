#!/usr/bin/env bash
# Install DataSync systemd: Podman Kafka + CDC daemon (single unit, User=datalake).
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

Podman Kafka + CDC daemon as one stack (DataSync.service), user datalake.
Run once on prod, then: git pull && ./install.sh

  --no-enable  install units only
EOF
      exit 0
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
  echo "Creating system user ${DATASYNC_USER}..." >&2
  useradd -r -m -d /opt/datalake -s /bin/bash "${DATASYNC_USER}"
fi
DATASYNC_UID="$(id -u "${DATASYNC_USER}")"
RUNTIME_DIR="/run/user/${DATASYNC_UID}"

disable_legacy_units() {
  local unit removed=0
  for unit in datalake-cdc.service DataSync-reconcile.service DataSync-reconcile.timer \
              datasync.service DataSync-kafka.service; do
    if [[ -f "${SYSTEMD_DIR}/${unit}" ]]; then
      systemctl disable --now "${unit}" 2>/dev/null || true
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

prepare_datalake_host() {
  echo "Preparing host for User=${DATASYNC_USER}..." >&2
  loginctl enable-linger "${DATASYNC_USER}" 2>/dev/null || true
  chown -R "${DATASYNC_USER}:${DATASYNC_GROUP}" "${ROOT}"
  chmod +x "${ROOT}/deploy/container-compose.sh" "${ROOT}/deploy/validate-stack.sh" "${ROOT}/deploy/systemd/"*.sh "${ROOT}/deploy/systemd/kafka-force-clean.sh" 2>/dev/null || true

  systemctl stop DataSync.service DataSync-kafka.service 2>/dev/null || true

  if command -v runuser >/dev/null 2>&1; then
    runuser -u "${DATASYNC_USER}" -- \
      env XDG_RUNTIME_DIR="${RUNTIME_DIR}" \
          DBUS_SESSION_BUS_ADDRESS="unix:path=${RUNTIME_DIR}/bus" \
      systemctl --user enable --now podman.socket 2>/dev/null || true
  fi

  DATASYNC_ROOT="${ROOT}" /bin/bash "${SCRIPT_DIR}/kafka-force-clean.sh"
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

chmod +x "${SCRIPT_DIR}"/datasync-*.sh "${SCRIPT_DIR}"/kafka-force-clean.sh "${ROOT}/deploy/container-compose.sh" 2>/dev/null || true

mkdir -p "${ENV_DIR}"
sync_env_file
prepare_datalake_host

render_unit "${SCRIPT_DIR}/DataSync.service" "${SYSTEMD_DIR}/DataSync.service"

disable_legacy_units
systemctl daemon-reload
systemctl reset-failed DataSync.service 2>/dev/null || true

if [[ ! -r "${ROOT}/config.json" ]]; then
  echo "WARN: ${DATASYNC_USER} cannot read ${ROOT}/config.json" >&2
fi

if [[ "${DO_ENABLE}" -eq 1 ]]; then
  systemctl enable DataSync.service
  systemctl restart DataSync.service
  sleep 3
  if ! systemctl is-active --quiet DataSync.service; then
    echo "ERROR: DataSync failed — journalctl -u DataSync -n 40 --no-pager" >&2
    exit 1
  fi
  # shellcheck source=deploy/systemd/datasync-lib.sh
  source "${SCRIPT_DIR}/datasync-lib.sh"
  if ! kafka_tcp_ok; then
    echo "ERROR: Kafka not on localhost:9092" >&2
    exit 1
  fi
  echo ""
  echo "OK: DataSync stack (Kafka + daemon, user=${DATASYNC_USER}, uid=${DATASYNC_UID})"
fi

cat <<EOF

Installed (Podman, user=datalake):
  ${SYSTEMD_DIR}/DataSync.service
  ${ENV_FILE}

Prod updates:  cd ${ROOT} && git pull && ./install.sh

Restart:
  sudo systemctl restart DataSync

Status:
  systemctl status DataSync
  sudo -u datalake env XDG_RUNTIME_DIR=${RUNTIME_DIR} DOCKER_HOST=unix://${RUNTIME_DIR}/podman/podman.sock podman compose -f ${ROOT}/docker-compose.yml ps

Logs:
  journalctl -u DataSync -n 50 --no-pager
  sudo -u datalake ... podman compose logs -f kafka
  sudo -u datalake ... podman compose logs -f datasync

EOF
