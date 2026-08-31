#pragma once

#include "config.hpp"
#include "mariadb_conn.hpp"

#include <libpq-fe.h>
#include <mysql/mysql.h>

#include <optional>
#include <string>
#include <vector>

struct MariaDbColumn;

struct DdlSyncResult {
    int columns_added{0};
    int columns_widened{0};
    int columns_dropped{0};
    int indexes_created{0};
    int foreign_keys_created{0};
    /** Drift the lake cannot resolve in place; the table was flagged for a full-load reboot. */
    bool full_load_requested{false};
    std::string drift_reason;
};

// After TRUNCATE, before COPY: sync missing columns, secondary indexes, FKs (parents must exist in lake).
// pk_cols guards the apply index: a PK column is never dropped here, only reloaded.
DdlSyncResult sync_mariadb_ddl_after_truncate(
    PGconn* pg,
    MYSQL* mysql,
    const std::string& schema,
    const std::string& table,
    const std::vector<MariaDbColumn>& cols,
    const std::vector<std::string>& pk_cols = {});

// CDC DDL replay: sync columns only (create lake table if missing). No indexes/FKs.
// catalog_pg/catalog_id let ambiguous drift (possible rename) flag the table for a reload.
DdlSyncResult sync_mariadb_columns_to_lake(
    PGconn* pg,
    MYSQL* mysql,
    const std::string& schema,
    const std::string& table,
    PGconn* catalog_pg = nullptr,
    long long catalog_id = 0,
    const std::vector<std::string>& pk_cols = {});

struct DdlSyncRunStats {
    int tables_processed{0};
    int tables_success{0};
    int tables_failed{0};
    int columns_added{0};
    int columns_dropped{0};
    /** Tables whose drift could not be resolved in place and now await a full-load reboot. */
    int tables_flagged_for_reload{0};
};

DdlSyncRunStats run_mariadb_ddl_sync(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::optional<std::string>& source_schema,
    const std::optional<std::string>& source_table);

int run_mariadb_ddl_sync_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& source_schema = std::nullopt,
    const std::optional<std::string>& source_table = std::nullopt);
