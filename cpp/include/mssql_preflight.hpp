#pragma once

#include <libpq-fe.h>

#include <map>
#include <string>
#include <vector>

#ifdef HAVE_FREETDS
#include "mssql_conn.hpp"
#endif

struct MssqlPreflightResult {
    bool ok{true};
    std::vector<std::string> errors;
    std::vector<std::string> warnings;
    /** Observed server state (CDC flag, agent status, cleanup retention, ...) for the report. */
    std::map<std::string, std::string> facts;
};

#ifdef HAVE_FREETDS
MssqlPreflightResult check_mssql_cdc_ready(MssqlConn& mssql, const std::string& database);
MssqlPreflightResult check_mssql_load_ready(MssqlConn& mssql, const std::string& database);
MssqlPreflightResult check_mssql_catalog_capture_instances(PGconn* pg, const std::string& conn_id);
/** SQL Agent + CDC job state. DataSync runs sp_cdc_scan itself, so a stopped agent is not fatal;
 *  a running capture job is what competes with it. */
MssqlPreflightResult check_mssql_agent_ready(MssqlConn& mssql, const std::string& database);
#endif

void merge_mssql_preflight(MssqlPreflightResult& into, const MssqlPreflightResult& part);
