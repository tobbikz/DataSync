#include "schema_migrate.hpp"

#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "schema_embedded.hpp"

#include <cstdlib>
#include <stdexcept>
#include <string>

namespace {

void exec_schema_sql(PGconn* pg, const std::string_view& sql, const std::string_view& label) {
    if (sql.empty()) {
        return;
    }
    PGresult* res = PQexec(pg, std::string(sql).c_str());
    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
        const std::string err = pg ? PQerrorMessage(pg) : "null connection";
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error(
            "schema migration failed in " + std::string(label) + ": " + err);
    }
    PQclear(res);
}

bool schema_reset_enabled() {
    if (const char* env = std::getenv("DATASYNC_SCHEMA_RESET")) {
        return env[0] == '1' && env[1] == '\0';
    }
    return false;
}

}  // namespace

void ensure_cdc_catalog_schema(PGconn* pg) {
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        throw std::runtime_error("ensure_cdc_catalog_schema: PostgreSQL connection not ready");
    }

    const bool reset = schema_reset_enabled();
    const auto scripts = schema_embedded::cdc_catalog_scripts();
    std::size_t applied = 0;

    for (const auto& script : scripts) {
        if (script.destructive && !reset) {
            continue;
        }
        exec_schema_sql(pg, script.sql, script.filename);
        ++applied;
    }

    log_write(pg, {
        .level = LogLevel::Info,
        .component = "schema_migrate",
        .message = reset ? "cdc_catalog schema reset and migrated" : "cdc_catalog schema ensured",
        .context = {{"source", "embedded"}, {"files", applied}, {"reset", reset}},
    });
}

void ensure_lake_schema(PGconn* pg) {
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        throw std::runtime_error("ensure_lake_schema: PostgreSQL connection not ready");
    }

    const auto sql = schema_embedded::lake_helpers_sql();
    exec_schema_sql(pg, sql, "lake/010_helpers.sql");

    log_write(pg, {
        .level = LogLevel::Info,
        .component = "schema_migrate",
        .message = "lake schema ensured",
        .context = {{"source", "embedded"}},
    });
}
