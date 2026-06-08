#!/usr/bin/env bash
# Install DataSync systemd units and optional env file.
# Usage: sudo deploy/systemd/install-systemd.sh [--native]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SYSTEMD_DIR="/etc/systemd/system"
ENV_DIR="/etc/datasync"
ENV_FILE="${ENV_DIR}/datasync.env"
CLI_DEPLOY_MODE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --native) CLI_DEPLOY_MODE="native"; shift ;;
    -h|--help)
      sed -n '2,4p' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo $0" >&2
  exit 1
fi

disable_legacy_units() {
  local unit removed=0
  for unit in datalake-cdc.service; do
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
  sed "s|@DATASYNC_ROOT@|${ROOT}|g" "${src}" > "${dst}"
}

chmod +x "${SCRIPT_DIR}"/datasync-*.sh

mkdir -p "${ENV_DIR}"
if [[ ! -f "${ENV_FILE}" ]]; then
  DEPLOY_MODE="${CLI_DEPLOY_MODE:-docker}"
  sed "s|@DATASYNC_ROOT@|${ROOT}|g" "${SCRIPT_DIR}/DataSync.env.example" > "${ENV_FILE}"
  if [[ "${DEPLOY_MODE}" == "native" ]]; then
    sed -i 's/^DATASYNC_DEPLOY_MODE=.*/DATASYNC_DEPLOY_MODE=native/' "${ENV_FILE}"
  fi
  chmod 0640 "${ENV_FILE}"
  echo "Created ${ENV_FILE}"
else
  echo "Keeping existing ${ENV_FILE}"
  if [[ -n "${CLI_DEPLOY_MODE}" ]]; then
    DEPLOY_MODE="${CLI_DEPLOY_MODE}"
    if grep -q '^DATASYNC_DEPLOY_MODE=' "${ENV_FILE}"; then
      sed -i "s/^DATASYNC_DEPLOY_MODE=.*/DATASYNC_DEPLOY_MODE=${DEPLOY_MODE}/" "${ENV_FILE}"
    else
      echo "DATASYNC_DEPLOY_MODE=${DEPLOY_MODE}" >> "${ENV_FILE}"
    fi
  elif grep -q '^DATASYNC_DEPLOY_MODE=' "${ENV_FILE}"; then
    DEPLOY_MODE="$(grep '^DATASYNC_DEPLOY_MODE=' "${ENV_FILE}" | cut -d= -f2- | tr -d '"')"
  else
    DEPLOY_MODE="docker"
  fi
fi

if [[ "${DEPLOY_MODE}" == "native" ]]; then
  SERVICE_SRC="${SCRIPT_DIR}/DataSync-native.service"
else
  SERVICE_SRC="${SCRIPT_DIR}/DataSync-docker.service"
fi

render_unit "${SERVICE_SRC}" "${SYSTEMD_DIR}/DataSync.service"
render_unit "${SCRIPT_DIR}/DataSync-reconcile.service" "${SYSTEMD_DIR}/DataSync-reconcile.service"
install -m 0644 "${SCRIPT_DIR}/DataSync-reconcile.timer" "${SYSTEMD_DIR}/"

disable_legacy_units

systemctl daemon-reload
systemctl reset-failed DataSync.service 2>/dev/null || true

cat <<EOF

Installed (mode=${DEPLOY_MODE}):
  ${SYSTEMD_DIR}/DataSync.service
  ${SYSTEMD_DIR}/DataSync-reconcile.service
  ${SYSTEMD_DIR}/DataSync-reconcile.timer
  ${ENV_FILE}

Every start/restart rebuilds first (ExecStartPre → datasync-build.sh).

Enable CDC daemon:
  sudo systemctl enable --now DataSync

Restart with rebuild:
  sudo systemctl restart DataSync

Enable reconcile every 4h:
  sudo systemctl enable --now DataSync-reconcile.timer

CLI smoke:
  ${ROOT}/deploy/systemd/datasync-cli.sh daemon --once

Logs (docker):
  docker compose -f ${ROOT}/docker-compose.yml logs -f datasync

Logs (native):
  journalctl -u DataSync -f

EOF
