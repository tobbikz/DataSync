#pragma once

#include "mssql_conn.hpp"

#include "mariadb_schema.hpp"

#include <string>
#include <vector>

struct MssqlColumn {
    std::string name;
    std::string mssql_type;
    bool is_pk{false};
    std::string pg_type;
};

std::string mssql_to_pg_type(const std::string& data_type, int max_length, int precision, int scale);

#ifdef HAVE_FREETDS
std::vector<MssqlColumn> fetch_mssql_columns(
    DBPROCESS* db,
    const std::string& schema,
    const std::string& table);

void ensure_mssql_lake_table_base(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::vector<MssqlColumn>& cols,
    const std::vector<std::string>& source_pk_cols,
    int partition_months_ahead = 3);

std::vector<std::string> fetch_lake_primary_key_columns(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table);
#endif
