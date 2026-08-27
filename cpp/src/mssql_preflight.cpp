#include "mssql_preflight.hpp"

#include <libpq-fe.h>

#include <regex>
#include <stdexcept>

void merge_mssql_preflight(MssqlPreflightResult& into, const MssqlPreflightResult& part) {
    if (!part.ok) {
        into.ok = false;
    }
    into.errors.insert(into.errors.end(), part.errors.begin(), part.errors.end());
    into.warnings.insert(into.warnings.end(), part.warnings.begin(), part.warnings.end());
    for (const auto& [key, value] : part.facts) {
        into.facts[key] = value;
    }
}

namespace {

bool cdc_flag_is_enabled(const std::string& text) {
    return text == "1" || text == "true" || text == "True" || text == "TRUE";
}

bool validate_mssql_capture_instance(const std::string& name) {
    static const std::regex re(R"(^[A-Za-z0-9_]+$)");
    return !name.empty() && std::regex_match(name, re);
}

std::string quote_mssql_literal(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 2);
    for (const char c : value) {
        if (c == '\'') {
            out.push_back('\'');
        }
        out.push_back(c);
    }
    return out;
}

std::string first_cell(const MssqlQueryResult& result) {
    if (result.rows.empty() || result.rows[0].empty()) {
        return {};
    }
    return trim_mssql_text(result.rows[0][0].text);
}

}  // namespace

MssqlPreflightResult check_mssql_load_ready(MssqlConn& mssql, const std::string& database) {
    MssqlPreflightResult result;
    if (!mssql.handle) {
        result.ok = false;
        result.errors.push_back("mssql handle is null");
        return result;
    }

    try {
        mssql.use_database(database);
        const auto ping = mssql.query("SELECT 1 AS ok");
        if (ping.rows.empty()) {
            result.ok = false;
            result.errors.push_back("connection check returned no rows for database " + database);
            return result;
        }
        result.facts["server_version"] =
            first_cell(mssql.query("SELECT CONVERT(VARCHAR(64), SERVERPROPERTY('ProductVersion'))"));
    } catch (const std::exception& ex) {
        result.ok = false;
        result.errors.push_back(std::string("connection check failed for database ") + database + ": " + ex.what());
    }
    return result;
}

MssqlPreflightResult check_mssql_cdc_ready(MssqlConn& mssql, const std::string& database) {
    MssqlPreflightResult result = check_mssql_load_ready(mssql, database);
    if (!result.ok) {
        return result;
    }

    try {
        mssql.use_database(database);
        const auto cdc_flag = mssql.query("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()");
        if (cdc_flag.rows.empty() || cdc_flag.rows[0].empty()) {
            result.ok = false;
            result.errors.push_back("is_cdc_enabled not found for database " + database);
            return result;
        }
        const bool cdc_on = cdc_flag_is_enabled(cdc_flag.rows[0][0].text);
        result.facts["is_cdc_enabled"] = cdc_on ? "1" : "0";
        if (!cdc_on) {
            result.warnings.push_back("database " + database + " has CDC disabled (is_cdc_enabled=0)");
            return result;
        }

        const auto capture_rows = mssql.query(
            "SELECT COUNT(*) FROM cdc.change_tables ct "
            "JOIN sys.tables t ON ct.source_object_id = t.object_id");
        if (!capture_rows.rows.empty() && !capture_rows.rows[0].empty()) {
            const std::string count = trim_mssql_text(capture_rows.rows[0][0].text);
            result.facts["capture_instances"] = count;
            if (count == "0") {
                result.warnings.push_back("database " + database + " has CDC enabled but no capture instances yet");
            }
        }
    } catch (const std::exception& ex) {
        result.ok = false;
        result.errors.push_back(std::string("CDC preflight failed for database ") + database + ": " + ex.what());
    }
    return result;
}

MssqlPreflightResult check_mssql_agent_ready(MssqlConn& mssql, const std::string& database) {
    MssqlPreflightResult result;

    std::string agent_status;
    try {
        agent_status = first_cell(mssql.query(
            "SELECT TOP 1 status_desc FROM sys.dm_server_services "
            "WHERE servicename LIKE 'SQL Server Agent%'"));
    } catch (const std::exception& ex) {
        result.warnings.push_back(
            std::string("SQL Server Agent status unavailable (needs VIEW SERVER STATE): ") + ex.what());
    }

    bool capture_job = false;
    bool cleanup_job = false;
    try {
        mssql.use_database(database);
        const auto jobs = mssql.query(
            "SELECT job_type, retention FROM msdb.dbo.cdc_jobs "
            "WHERE database_id = DB_ID('" + quote_mssql_literal(database) + "')");
        for (const auto& row : jobs.rows) {
            if (row.empty()) {
                continue;
            }
            const std::string job_type = trim_mssql_text(row[0].text);
            if (job_type == "capture") {
                capture_job = true;
            } else if (job_type == "cleanup") {
                cleanup_job = true;
                if (row.size() > 1) {
                    result.facts["cdc_cleanup_retention_minutes"] = trim_mssql_text(row[1].text);
                }
            }
        }
    } catch (const std::exception& ex) {
        result.warnings.push_back(std::string("cdc job inspection failed: ") + ex.what());
    }

    if (agent_status.empty()) {
        return result;
    }
    result.facts["sql_server_agent"] = agent_status;
    result.facts["cdc_capture_job"] = capture_job ? "present" : "absent";
    result.facts["cdc_cleanup_job"] = cleanup_job ? "present" : "absent";

    // DataSync calls sys.sp_cdc_scan on every capture slice, so it never waits on the agent. The
    // agent only matters for the two side effects DataSync does not drive itself.
    if (agent_status == "Running") {
        if (capture_job) {
            result.warnings.push_back(
                "SQL Server Agent is running with the CDC capture job present: it competes with the "
                "sp_cdc_scan DataSync issues per slice and one of the two will fail with 'another "
                "instance is already running' (harmless, but noisy). Consider disabling the job");
        }
    } else {
        result.warnings.push_back(
            "SQL Server Agent is " + agent_status +
            ": capture is unaffected (DataSync runs sp_cdc_scan itself) but the CDC cleanup job never "
            "runs, so cdc.* change tables grow without bound");
    }

    return result;
}

MssqlPreflightResult check_mssql_catalog_capture_instances(PGconn* pg, const std::string& conn_id) {
    MssqlPreflightResult result;
    if (!pg) {
        result.ok = false;
        result.errors.push_back("postgres handle is null");
        return result;
    }

    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT source_database, source_schema, source_table, engine_meta->>'capture_instance' AS capture_instance
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = 'mssql'
          AND active = true
          AND cdc_enabled = true
          AND needs_full_load = true
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        result.warnings.push_back("capture_instance catalog check skipped: query failed");
        return result;
    }

    const int rows = PQntuples(res);
    for (int i = 0; i < rows; ++i) {
        const char* db = PQgetvalue(res, i, 0);
        const char* schema = PQgetvalue(res, i, 1);
        const char* table = PQgetvalue(res, i, 2);
        const char* cap = PQgetvalue(res, i, 3);
        const std::string capture_instance = cap ? cap : "";
        const std::string fq =
            std::string(db ? db : "") + "." + std::string(schema ? schema : "") + "." +
            std::string(table ? table : "");
        if (!validate_mssql_capture_instance(capture_instance)) {
            result.warnings.push_back(
                fq + " has missing or invalid capture_instance (value=" + capture_instance + ")");
        }
    }
    PQclear(res);
    return result;
}
