#include "schema_migrate.hpp"

#include "mariadb_schema.hpp"
#include "obs_log.hpp"
#include "prod_ops_embedded.hpp"

#include <nlohmann/json.hpp>

#include <sstream>
#include <stdexcept>
#include <string_view>

namespace {

void ensure_schema_migrations_table(PGconn* pg) {
    pg_exec(
        pg,
        R"(
        CREATE TABLE IF NOT EXISTS cdc_catalog.schema_migrations (
            version integer NOT NULL PRIMARY KEY,
            description text NOT NULL,
            applied_at timestamp with time zone DEFAULT now() NOT NULL
        )
        )");
}

std::string strip_psql_meta_commands(std::string_view sql) {
    std::ostringstream out;
    std::size_t pos = 0;
    while (pos < sql.size()) {
        const std::size_t end = sql.find('\n', pos);
        const std::string_view line =
            end == std::string_view::npos ? sql.substr(pos) : sql.substr(pos, end - pos);
        pos = end == std::string_view::npos ? sql.size() : end + 1;
        if (line.empty()) {
            continue;
        }
        if (line.front() == '\\') {
            continue;
        }
        out << line << '\n';
    }
    return out.str();
}

void exec_sql_section(PGconn* pg, std::string_view sql, const std::string& section) {
    const std::string cleaned = strip_psql_meta_commands(sql);
    if (cleaned.empty()) {
        return;
    }
    PGresult* res = PQexec(pg, cleaned.c_str());
    if (!res) {
        throw std::runtime_error(section + ": PQexec returned null");
    }
    const ExecStatusType st = PQresultStatus(res);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(pg);
        const char* msg = PQresultErrorMessage(res);
        PQclear(res);
        throw std::runtime_error(section + " failed: " + std::string(msg ? msg : err));
    }
    PQclear(res);
}

void log_query_rows(PGconn* pg, const std::string& label, const std::string& sql) {
    PGresult* res = PQexec(pg, sql.c_str());
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        log_write(pg, {
            .level = LogLevel::Warning,
            .component = "catalog",
            .message = "diagnostics query failed",
            .context = {{"label", label}},
        });
        return;
    }
    const int rows = PQntuples(res);
    const int cols = PQnfields(res);
    nlohmann::json rows_json = nlohmann::json::array();
    for (int r = 0; r < rows && r < 50; ++r) {
        nlohmann::json row = nlohmann::json::object();
        for (int c = 0; c < cols; ++c) {
            const char* val = PQgetvalue(res, r, c);
            row[PQfname(res, c)] = val ? val : nullptr;
        }
        rows_json.push_back(std::move(row));
    }
    PQclear(res);
    log_write(pg, {
        .level = LogLevel::Info,
        .component = "catalog",
        .message = "diagnostics " + label,
        .context = {{"rows", rows}, {"sample", rows_json}},
    });
}

void run_diagnostics(PGconn* pg) {
    log_write(pg, {
        .level = LogLevel::Info,
        .component = "catalog",
        .message = "diagnostics started",
    });

    log_query_rows(
        pg,
        "connections",
        R"(SELECT alias AS conn_id, db_engine, host, port, active
           FROM cdc_catalog.connections ORDER BY alias)");

    log_query_rows(
        pg,
        "capture_eligibility",
        R"(SELECT conn_id, source_schema, source_table, active, cdc_enabled,
                  needs_full_load, capture_during_full_load, has_pk, status,
                  (active AND cdc_enabled
                   AND (NOT needs_full_load OR capture_during_full_load)
                   AND has_pk
                   AND status NOT IN ('skipped', 'disabled')) AS capture_ready
           FROM cdc_catalog.catalog
           ORDER BY conn_id, capture_ready DESC, source_schema, source_table
           LIMIT 100)");

    log_query_rows(
        pg,
        "capture_position",
        R"(SELECT conn_id, binlog_file, binlog_position, status, last_error, updated_at
           FROM cdc_catalog.capture_position ORDER BY conn_id)");

    log_query_rows(
        pg,
        "recent_capture_logs",
        R"(SELECT created_at, level, component, message, conn_id
           FROM cdc_catalog.logs
           WHERE component IN ('cdc_kafka_capture', 'cdc_daemon', 'cdc_kafka_apply_cpp')
             AND created_at > now() - interval '6 hours'
           ORDER BY created_at DESC LIMIT 20)");

    log_write(pg, {
        .level = LogLevel::Info,
        .component = "catalog",
        .message = "diagnostics completed",
    });
}

}  // namespace

bool catalog_schema_exists(PGconn* pg) {
    PGresult* res = PQexec(
        pg,
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'cdc_catalog'");
    const bool ok = res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0;
    if (res) {
        PQclear(res);
    }
    return ok;
}

bool lake_schema_exists(PGconn* pg) {
    PGresult* res = PQexec(
        pg,
        R"(SELECT 1 FROM pg_proc p
           JOIN pg_namespace n ON n.oid = p.pronamespace
           WHERE n.nspname = 'lake' AND p.proname = 'ensure_monthly_partitions')");
    const bool ok = res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0;
    if (res) {
        PQclear(res);
    }
    return ok;
}

int run_schema_migrate(PGconn* log_pg, PGconn* lake_pg, const SchemaMigrateOptions& options) {
    int steps = 0;

    if (options.baseline) {
        if (catalog_schema_exists(log_pg)) {
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "catalog",
                .message = "migrate baseline skipped: cdc_catalog exists",
            });
        } else {
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "catalog",
                .message = "migrate applying baseline",
            });
            exec_sql_section(log_pg, prod_ops_embedded::datasync_baseline(), "datasync_baseline");
            ensure_schema_migrations_table(log_pg);
            steps += 1;
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "catalog",
                .message = "migrate baseline applied",
            });
        }
    }

    if (options.lake) {
        if (!lake_pg) {
            throw std::runtime_error("migrate --lake requires datalake connection");
        }
        if (lake_schema_exists(lake_pg)) {
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "catalog",
                .message = "migrate lake skipped: lake schema exists",
            });
        } else {
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "catalog",
                .message = "migrate applying lake schema",
            });
            exec_sql_section(lake_pg, prod_ops_embedded::datalake_lake(), "datalake_lake");
            steps += 1;
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "catalog",
                .message = "migrate lake applied",
            });
        }
    }

    if (options.incremental) {
        ensure_schema_migrations_table(log_pg);
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "catalog",
            .message = "migrate applying incremental",
        });
        exec_sql_section(log_pg, prod_ops_embedded::datasync_incremental(), "datasync_incremental");
        try {
            exec_sql_section(log_pg, prod_ops_embedded::monitoring_views(), "monitoring_views");
        } catch (const std::exception& ex) {
            log_write(log_pg, {
                .level = LogLevel::Warning,
                .component = "catalog",
                .message = "monitoring_views failed (non-fatal)",
                .context = {{"error", ex.what()}},
            });
        }
        steps += 1;
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "catalog",
            .message = "migrate incremental applied",
        });
    }

    if (options.diagnostics) {
        run_diagnostics(log_pg);
        steps += 1;
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "catalog",
        .message = "migrate completed",
        .context = {{"steps", steps}},
    });
    return 0;
}
