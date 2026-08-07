#include "schema_migrate.hpp"

#include "mariadb_schema.hpp"
#include "obs_log.hpp"
#include "pipeline_defaults.hpp"
#include "prod_ops_embedded.hpp"

#include <nlohmann/json.hpp>

#include <sstream>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace {

constexpr const char* kRequiredCatalogTables[] = {
    "apply_batch_stats",
    "apply_outbox",
    "apply_position",
    "capture_position",
    "catalog",
    "cdc_applied_events",
    "cdc_mongo_resume",
    "cdc_mssql_lsn",
    "connections",
    "full_load_checkpoint",
    "logs",
    "reconciliation",
    "runtime_config",
    "schema_migrations",
};

constexpr const char* kRequiredCatalogViews[] = {
    "v_apply_latest",
    "v_reconciliation_latest",
};

bool regclass_exists(PGconn* pg, const std::string& qualified_name) {
    const char* vals[] = {qualified_name.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT to_regclass($1) IS NOT NULL",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    bool ok = false;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        const char* val = PQgetvalue(res, 0, 0);
        ok = val && val[0] == 't';
    }
    if (res) {
        PQclear(res);
    }
    return ok;
}

int catalog_table_count(PGconn* pg) {
    PGresult* res = PQexec(
        pg,
        "SELECT count(*)::integer FROM pg_tables WHERE schemaname = 'cdc_catalog'");
    int count = -1;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        count = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return count;
}

int latest_schema_migration_version(PGconn* pg) {
    PGresult* res = PQexec(pg, "SELECT COALESCE(max(version), 0)::integer FROM cdc_catalog.schema_migrations");
    int version = -1;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        version = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return version;
}

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

bool try_advisory_lock(PGconn* pg, long long lock_key) {
    const std::string key = std::to_string(lock_key);
    const char* vals[] = {key.c_str()};
    PGresult* res = PQexecParams(
        pg, "SELECT pg_try_advisory_lock($1::bigint)", 1, nullptr, vals, nullptr, nullptr, 0);
    bool locked = false;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        locked = PQgetvalue(res, 0, 0)[0] == 't';
    }
    if (res) {
        PQclear(res);
    }
    return locked;
}

bool advisory_lock_blocking(PGconn* pg, long long lock_key) {
    const std::string key = std::to_string(lock_key);
    const char* vals[] = {key.c_str()};
    PGresult* res = PQexecParams(
        pg, "SELECT pg_advisory_lock($1::bigint)", 1, nullptr, vals, nullptr, nullptr, 0);
    bool locked = false;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        locked = PQgetvalue(res, 0, 0)[0] == 't';
    }
    if (res) {
        PQclear(res);
    }
    return locked;
}

void advisory_unlock(PGconn* pg, long long lock_key) {
    const std::string key = std::to_string(lock_key);
    const char* vals[] = {key.c_str()};
    PGresult* res = PQexecParams(
        pg, "SELECT pg_advisory_unlock($1::bigint)", 1, nullptr, vals, nullptr, nullptr, 0);
    if (res) {
        PQclear(res);
    }
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
                   AND (
                     (db_engine <> 'mssql' AND (NOT needs_full_load OR capture_during_full_load))
                     OR (db_engine = 'mssql' AND NOT needs_full_load)
                   )
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
        R"(SELECT logged_at, level, component, message, conn_id
           FROM cdc_catalog.logs
           WHERE component IN ('cdc_kafka_capture', 'cdc_daemon', 'cdc_kafka_apply_cpp')
             AND logged_at > now() - interval '6 hours'
           ORDER BY logged_at DESC LIMIT 20)");

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
        // Always re-apply: datalake_lake() is CREATE OR REPLACE (partition helpers evolve).
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "catalog",
            .message = lake_schema_exists(lake_pg)
                ? "migrate refreshing lake helpers"
                : "migrate applying lake schema",
        });
        exec_sql_section(lake_pg, prod_ops_embedded::datalake_lake(), "datalake_lake");
        steps += 1;
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "catalog",
            .message = "migrate lake applied",
        });
    }

    if (options.incremental) {
        ensure_schema_migrations_table(log_pg);
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "catalog",
            .message = "migrate applying incremental",
        });
        exec_sql_section(log_pg, prod_ops_embedded::datasync_incremental(), "datasync_incremental");
        exec_sql_section(log_pg, prod_ops_embedded::schema_patches(), "schema_patches");
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
        repair_catalog_schema(log_pg);
        validate_catalog_schema(log_pg);
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

std::vector<std::string> missing_catalog_schema_objects(PGconn* pg) {
    std::vector<std::string> missing;
    if (!pg) {
        missing.emplace_back("cdc_catalog (no connection)");
        return missing;
    }
    for (const char* table : kRequiredCatalogTables) {
        const std::string name = std::string("cdc_catalog.") + table;
        if (!regclass_exists(pg, name)) {
            missing.push_back("table:" + std::string(table));
        }
    }
    for (const char* view : kRequiredCatalogViews) {
        const std::string name = std::string("cdc_catalog.") + view;
        if (!regclass_exists(pg, name)) {
            missing.push_back("view:" + std::string(view));
        }
    }
    return missing;
}

void validate_catalog_schema(PGconn* pg) {
    const auto missing = missing_catalog_schema_objects(pg);
    if (missing.empty()) {
        log_write(pg, {
            .level = LogLevel::Info,
            .component = "catalog",
            .message = "catalog schema validated",
            .context = {
                {"table_count", catalog_table_count(pg)},
                {"expected_tables", kExpectedCatalogTableCount},
                {"schema_version", latest_schema_migration_version(pg)},
            },
        });
        return;
    }
    std::ostringstream detail;
    for (std::size_t i = 0; i < missing.size(); ++i) {
        if (i > 0) {
            detail << ", ";
        }
        detail << missing[i];
    }
    throw std::runtime_error(
        "cdc_catalog schema incomplete after repair (missing " + std::to_string(missing.size()) +
        " object(s): " + detail.str() + "; tables=" + std::to_string(catalog_table_count(pg)) +
        "/" + std::to_string(kExpectedCatalogTableCount) + ", schema_version=" +
        std::to_string(latest_schema_migration_version(pg)) + ")");
}

void repair_catalog_schema(PGconn* pg) {
    if (!pg || !catalog_schema_exists(pg)) {
        return;
    }
    const auto missing_before = missing_catalog_schema_objects(pg);
    if (missing_before.empty()) {
        return;
    }
    nlohmann::json missing_json = nlohmann::json::array();
    for (const auto& item : missing_before) {
        missing_json.push_back(item);
    }
    log_write(pg, {
        .level = LogLevel::Warning,
        .component = "catalog",
        .message = "catalog schema repair started",
        .context = {
            {"missing", missing_json},
            {"table_count", catalog_table_count(pg)},
            {"schema_version", latest_schema_migration_version(pg)},
        },
    });
    exec_sql_section(pg, prod_ops_embedded::catalog_schema_repair(), "catalog_schema_repair");
    const auto missing_after = missing_catalog_schema_objects(pg);
    nlohmann::json still_missing = nlohmann::json::array();
    for (const auto& item : missing_after) {
        still_missing.push_back(item);
    }
    log_write(pg, {
        .level = missing_after.empty() ? LogLevel::Info : LogLevel::Warning,
        .component = "catalog",
        .message = missing_after.empty() ? "catalog schema repair completed"
                                         : "catalog schema repair partial",
        .context = {
            {"repaired_count", static_cast<int>(missing_before.size() - missing_after.size())},
            {"still_missing", still_missing},
            {"table_count", catalog_table_count(pg)},
        },
    });
}

void run_startup_schema_migrate(PGconn* log_pg) {
    if (!log_pg || !catalog_schema_exists(log_pg)) {
        return;
    }

    const long long lock_key = pipeline_defaults::kSchemaMigrateAdvisoryLockKey;
    const bool owns_lock = try_advisory_lock(log_pg, lock_key);
    if (!owns_lock) {
        // Another process is migrating — wait, then skip (schema already applied).
        if (advisory_lock_blocking(log_pg, lock_key)) {
            advisory_unlock(log_pg, lock_key);
        }
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "catalog",
            .message = "startup schema migrate skipped: peer completed",
        });
        return;
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "catalog",
        .message = "startup schema migrate started",
    });
    SchemaMigrateOptions opts;
    opts.incremental = true;
    try {
        (void)run_schema_migrate(log_pg, nullptr, opts);
    } catch (const std::exception& ex) {
        advisory_unlock(log_pg, lock_key);
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "catalog",
            .message = "startup schema migrate failed",
            .context = {{"error", ex.what()}},
        });
        throw;
    }
    advisory_unlock(log_pg, lock_key);
}
