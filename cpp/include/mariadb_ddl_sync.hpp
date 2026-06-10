#pragma once

#include "config.hpp"
#include "mariadb_conn.hpp"
#include "runtime_config.hpp"

#include <libpq-fe.h>
#include <mysql/mysql.h>

#include <optional>
#include <string>
#include <vector>

struct MariaDbColumn;

struct DdlSyncResult {
    int columns_added{0};
    int columns_widened{0};
    int indexes_created{0};
    int foreign_keys_created{0};
};

// After TRUNCATE, before COPY: sync missing columns, secondary indexes, FKs (parents must exist in lake).
DdlSyncResult sync_mariadb_ddl_after_truncate(
    PGconn* pg,
    MYSQL* mysql,
    const std::string& schema,
    const std::string& table,
    const std::vector<MariaDbColumn>& cols,
    const RuntimeConfig& cfg,
    const std::string& conn_id);

// CDC DDL replay: sync columns only (create lake table if missing). No indexes/FKs.
DdlSyncResult sync_mariadb_columns_to_lake(
    PGconn* pg,
    MYSQL* mysql,
    const std::string& schema,
    const std::string& table,
    const RuntimeConfig& cfg,
    const std::string& conn_id);

struct DdlSyncRunStats {
    int tables_processed{0};
    int tables_success{0};
    int tables_failed{0};
    int columns_added{0};
};

DdlSyncRunStats run_mariadb_ddl_sync(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::optional<std::string>& source_schema,
    const std::optional<std::string>& source_table);

int run_mariadb_ddl_sync_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::optional<std::string>& source_schema = std::nullopt,
    const std::optional<std::string>& source_table = std::nullopt);

// Topological order: parents before children (FK-safe full load).
std::vector<std::string> sort_tables_by_fk_order(
    MariaDbConn& conn,
    const std::string& schema,
    const std::vector<std::string>& tables,
    const MariaDbRetryOptions& retry = {});

// FK dependency levels: level 0 = no in-batch parents; same level may load in parallel.
std::vector<std::vector<std::string>> group_tables_by_fk_level(
    MariaDbConn& conn,
    const std::string& schema,
    const std::vector<std::string>& tables,
    const MariaDbRetryOptions& retry = {});
