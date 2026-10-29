#!/bin/bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
SELECT 'CREATE DATABASE datasync'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'datasync')\gexec
SELECT 'CREATE DATABASE "DataLake"'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'DataLake')\gexec
EOSQL
