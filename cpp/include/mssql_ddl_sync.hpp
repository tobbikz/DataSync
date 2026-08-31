#pragma once

#include "config.hpp"
#include "mariadb_ddl_sync.hpp"
#include "mssql_schema.hpp"

#include <libpq-fe.h>

#ifdef HAVE_FREETDS
#include <sybdb.h>
#endif

#include <optional>
#include <string>
#include <vector>

#ifdef HAVE_FREETDS

// pk_cols guards the apply index: a PK column is never dropped here, only reloaded.
DdlSyncResult sync_mssql_ddl_after_truncate(
    PGconn* pg,
    MssqlConn& mssql,
    const std::string& source_database,
    const std::string& source_schema,
    const std::string& source_table,
    const std::vector<MssqlColumn>& cols,
    const std::vector<std::string>& pk_cols = {});

// catalog_pg/catalog_id let ambiguous drift (possible rename) flag the table for a reload.
DdlSyncResult sync_mssql_columns_to_lake(
    PGconn* pg,
    MssqlConn& mssql,
    const std::string& source_database,
    const std::string& source_schema,
    const std::string& source_table,
    PGconn* catalog_pg = nullptr,
    long long catalog_id = 0,
    const std::vector<std::string>& pk_cols = {});

DdlSyncRunStats run_mssql_ddl_sync(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::optional<std::string>& source_schema,
    const std::optional<std::string>& source_table);

int run_mssql_ddl_sync_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& source_schema = std::nullopt,
    const std::optional<std::string>& source_table = std::nullopt);

#endif
