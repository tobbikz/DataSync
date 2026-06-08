#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DROP_IN="/etc/my.cnf.d/datalake-gtid.cnf"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run with sudo: sudo $0"
  exit 1
fi

install -m 0644 "${ROOT}/deploy/mariadb/mariadb-gtid.cnf" "${DROP_IN}"
# Ensure base binlog drop-in exists
if [[ ! -f /etc/my.cnf.d/datalake-binlog.cnf ]]; then
  install -m 0644 "${ROOT}/deploy/mariadb/mariadb-binlog.cnf" /etc/my.cnf.d/datalake-binlog.cnf
fi
systemctl restart mariadb
sleep 2
mariadb -e "SHOW VARIABLES WHERE Variable_name IN ('gtid_strict_mode','log_bin','binlog_format'); SELECT @@gtid_current_pos;"
