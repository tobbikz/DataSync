#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DROP_IN="/etc/my.cnf.d/datalake-binlog.cnf"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run with sudo: sudo $0"
  exit 1
fi

install -m 0644 "${ROOT}/deploy/mariadb/mariadb-binlog.cnf" "${DROP_IN}"
systemctl restart mariadb

sleep 2
mariadb -e "SHOW VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','server_id');"

echo
echo "Binlog enabled. Capture T0 after full load:"
echo "  FLUSH BINARY LOGS;"
echo "  ./build/DataSync full-load"
