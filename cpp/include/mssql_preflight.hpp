#pragma once

#include <libpq-fe.h>

#include <string>
#include <vector>

#ifdef HAVE_FREETDS
#include "mssql_conn.hpp"
#endif

struct MssqlPreflightResult {
    bool ok{true};
    std::vector<std::string> errors;
    std::vector<std::string> warnings;
};

#ifdef HAVE_FREETDS
MssqlPreflightResult check_mssql_cdc_ready(MssqlConn& mssql, const std::string& database);
MssqlPreflightResult check_mssql_load_ready(MssqlConn& mssql, const std::string& database);
MssqlPreflightResult check_mssql_catalog_capture_instances(PGconn* pg, const std::string& conn_id);
#endif

void merge_mssql_preflight(MssqlPreflightResult& into, const MssqlPreflightResult& part);
