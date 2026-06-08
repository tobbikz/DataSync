#!/usr/bin/env bash
# Install DataSync systemd unit and optional env file.
# Usage: sudo deploy/systemd/install-systemd.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SYSTEMD_DIR="/etc/systemd/system"
ENV_DIR="/etc/datasync"
ENV_FILE="${ENV_DIR}/datasync.env"

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
  if [[ -f "${SYSTEMD_DIR}/cdc-kafka@.service" ]]; then
    rm -f "${SYSTEMD_DIR}/cdc-kafka@.service"
    echo "Removed legacy unit: cdc-kafka@.service"
    removed=1
  fi
  if [[ -f "${SYSTEMD_DIR}/cdc-kafka-reconcile@.service" ]]; then
    rm -f "${SYSTEMD_DIR}/cdc-kafka-reconcile@.service"
    echo "Removed legacy unit: cdc-kafka-reconcile@.service"
    removed=1
  fi
  [[ "${removed}" -eq 1 ]] && systemctl daemon-reload
}

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo $0" >&2
  exit 1
fi

install -m 0644 "${SCRIPT_DIR}/DataSync.service" "${SYSTEMD_DIR}/"
install -m 0644 "${SCRIPT_DIR}/DataSync-reconcile.service" "${SYSTEMD_DIR}/"
install -m 0644 "${SCRIPT_DIR}/DataSync-reconcile.timer" "${SYSTEMD_DIR}/"

disable_legacy_units

mkdir -p "${ENV_DIR}"
if [[ ! -f "${ENV_FILE}" ]]; then
  install -m 0640 "${SCRIPT_DIR}/DataSync.env.example" "${ENV_FILE}"
  echo "Created ${ENV_FILE} from example — edit credentials before enable."
else
  echo "Keeping existing ${ENV_FILE}"
fi

echo "Reloading systemd..."
systemctl daemon-reload
systemctl reset-failed DataSync.service 2>/dev/null || true

cat <<EOF

Installed:
  ${SYSTEMD_DIR}/DataSync.service
  ${ENV_FILE} (if newly created)

Enable CDC daemon:
  systemctl enable --now DataSync

Enable reconcile every 4h (recommended):
  systemctl enable --now DataSync-reconcile.timer

Or long-running loop (same interval, no timer):
  systemctl enable --now DataSync-reconcile.service
  # edit service: remove --once from ExecStart for infinite loop

Smoke test:
  deploy/systemd/datasync-cli.sh daemon --once

Credentials: edit ${ROOT}/config.json (copy from config.json.example)

Status:
  systemctl status DataSync
  journalctl -u DataSync -f

Legacy Python units (cdc-kafka@*, cdc-kafka-reconcile@*, datalake-cdc) are disabled and removed on install.

EOF
