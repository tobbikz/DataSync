#!/usr/bin/env bash
# Install DataSync systemd units and enable CDC + reconcile timer.
# Usage: sudo deploy/systemd/install-systemd.sh [--native] [--no-enable]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SYSTEMD_DIR="/etc/systemd/system"
ENV_DIR="/etc/datasync"
ENV_FILE="${ENV_DIR}/datasync.env"
CLI_DEPLOY_MODE=""
DO_ENABLE=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --native) CLI_DEPLOY_MODE="native"; shift ;;
    --no-enable) DO_ENABLE=0; shift ;;
    -h|--help)
      cat <<EOF
Usage: sudo $0 [--native] [--no-enable]

Installs DataSync.service + DataSync-reconcile.timer for user datalake
(rootless podman/docker). By default enables and starts both.

  --native     native binary instead of Docker/Podman compose
  --no-enable  install units only (no systemctl enable --now)
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
  echo "System user ${DATASYNC_USER} not found. Create it first, e.g.:" >&2
  echo "  sudo useradd -r -m -d /opt/datalake -s /bin/bash ${DATASYNC_USER}" >&2
  echo "  sudo usermod -aG docker ${DATASYNC_USER}   # if using docker group" >&2
  exit 1
fi
DATASYNC_UID="$(id -u "${DATASYNC_USER}")"
RUNTIME_DIR="/run/user/${DATASYNC_UID}"

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
  local deploy_mode="$1"
  render_unit "${SCRIPT_DIR}/DataSync.env.example" "${ENV_FILE}"
  sed -i "s/^DATASYNC_DEPLOY_MODE=.*/DATASYNC_DEPLOY_MODE=${deploy_mode}/" "${ENV_FILE}"
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

mkdir -p "${ENV_DIR}"
if [[ -n "${CLI_DEPLOY_MODE}" ]]; then
  DEPLOY_MODE="${CLI_DEPLOY_MODE}"
elif [[ -f "${ENV_FILE}" ]] && grep -q '^DATASYNC_DEPLOY_MODE=' "${ENV_FILE}"; then
  DEPLOY_MODE="$(grep '^DATASYNC_DEPLOY_MODE=' "${ENV_FILE}" | cut -d= -f2- | tr -d '"')"
else
  DEPLOY_MODE="docker"
fi
sync_env_file "${DEPLOY_MODE}"
echo "Synced ${ENV_FILE} (DATASYNC_ROOT=${ROOT}, user=${DATASYNC_USER})"

if [[ "${DEPLOY_MODE}" == "native" ]]; then
  SERVICE_SRC="${SCRIPT_DIR}/DataSync-native.service"
else
  SERVICE_SRC="${SCRIPT_DIR}/DataSync-docker.service"
  ensure_podman_user_socket
fi

render_unit "${SERVICE_SRC}" "${SYSTEMD_DIR}/DataSync.service"
render_unit "${SCRIPT_DIR}/DataSync-reconcile.service" "${SYSTEMD_DIR}/DataSync-reconcile.service"
install -m 0644 "${SCRIPT_DIR}/DataSync-reconcile.timer" "${SYSTEMD_DIR}/"

disable_legacy_units

systemctl daemon-reload
systemctl reset-failed DataSync.service 2>/dev/null || true

# shellcheck source=deploy/systemd/datasync-lib.sh
source "${SCRIPT_DIR}/datasync-lib.sh"
ensure_single_daemon

if [[ "${DO_ENABLE}" -eq 1 ]]; then
  systemctl enable --now DataSync.service
  systemctl enable --now DataSync-reconcile.timer
  echo ""
  echo "Enabled: DataSync.service + DataSync-reconcile.timer (user=${DATASYNC_USER}, uid=${DATASYNC_UID})"
else
  echo ""
  echo "Units installed (not enabled — pass default install or omit --no-enable)."
fi

cat <<EOF

Installed (mode=${DEPLOY_MODE}, user=datalake):
  ${SYSTEMD_DIR}/DataSync.service
  ${SYSTEMD_DIR}/DataSync-reconcile.service
  ${SYSTEMD_DIR}/DataSync-reconcile.timer
  ${ENV_FILE}

Every start/restart rebuilds first (ExecStartPre → datasync-build.sh).

Restart with rebuild:
  sudo systemctl restart DataSync

Status:
  systemctl status DataSync
  systemctl status DataSync-reconcile.timer

CLI smoke:
  ${ROOT}/deploy/systemd/datasync-cli.sh daemon --once

Logs (docker/podman):
  cd ${ROOT} && podman compose logs -f datasync

Logs (native):
  journalctl -u DataSync -f

EOF
