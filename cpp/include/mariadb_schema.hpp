#pragma once

#include <libpq-fe.h>
#include <mysql/mysql.h>

#include <string>
#include <vector>

struct MariaDbColumn {
    std::string name;
    std::string mysql_type;
    bool is_pk{false};
    std::string pg_type;
};

std::string pg_ident(const std::string& name);
std::string mariadb_to_pg_type(const std::string& mysql_type_raw);

void pg_exec(PGconn* pg, const std::string& sql);
void pg_exec_params_simple(PGconn* pg, const char* sql, int n, const char* const* vals);

std::vector<MariaDbColumn> fetch_mariadb_columns(MYSQL* mysql, const std::string& schema, const std::string& table);

void ensure_lake_table_base(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::vector<MariaDbColumn>& cols,
    int partition_months_ahead = 3);

void truncate_lake_table(PGconn* pg, const std::string& schema, const std::string& table);

bool pg_lake_table_exists(PGconn* pg, const std::string& schema, const std::string& table);
