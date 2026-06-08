#include "mariadb_ddl_sync.hpp"

#include "config.hpp"
#include "mariadb_conn.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"

#include <iostream>
#include <optional>
#include <sstream>
#include <vector>

namespace {

struct TableTarget {
    std::string schema;
    std::string table;
};

std::vector<TableTarget> fetch_ddl_targets(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::optional<std::string>& source_schema,
    const std::optional<std::string>& source_table) {
    std::string sql = R"(
        SELECT source_schema, source_table
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = 'mariadb'
          AND active = true
          AND cdc_enabled = true
    )";
    std::vector<std::string> vals = {conn_id};
    if (service_tier && !service_tier->empty()) {
        sql += " AND service_tier::text = lower($" + std::to_string(vals.size() + 1) + ")";
        vals.push_back(*service_tier);
    }
    if (source_schema && !source_schema->empty()) {
        sql += " AND source_schema = $" + std::to_string(vals.size() + 1);
        vals.push_back(*source_schema);
    }
    if (source_table && !source_table->empty()) {
        sql += " AND source_table = $" + std::to_string(vals.size() + 1);
        vals.push_back(*source_table);
    }
    sql += " ORDER BY source_schema, source_table";

    std::vector<const char*> ptrs;
    ptrs.reserve(vals.size());
    for (const auto& v : vals) {
        ptrs.push_back(v.c_str());
    }

    PGresult* res = PQexecParams(
        pg,
        sql.c_str(),
        static_cast<int>(ptrs.size()),
        nullptr,
        ptrs.data(),
        nullptr,
        nullptr,
        0);

    std::vector<TableTarget> out;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    for (int i = 0; i < PQntuples(res); ++i) {
        out.push_back({PQgetvalue(res, i, 0), PQgetvalue(res, i, 1)});
    }
    PQclear(res);
    return out;
}

}  // namespace

DdlSyncRunStats run_mariadb_ddl_sync(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::optional<std::string>& source_schema,
    const std::optional<std::string>& source_table) {
    DdlSyncRunStats stats;
    RuntimeConfig runtime;
    runtime.reload(log_pg);

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "mariadb_ddl_sync",
        .message = "ddl sync started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"tier", service_tier.value_or("all")}},
    });

    const MariaDbSource* source = find_mariadb_source(cfg, conn_id);
    if (!source) {
        throw std::runtime_error("unknown conn_id: " + conn_id);
    }

    PgConn app_pg(cfg.datasync.conn_string());
    PgConn lake_pg(cfg.datalake.conn_string());
    const auto targets = fetch_ddl_targets(app_pg.raw, conn_id, service_tier, source_schema, source_table);
    if (targets.empty()) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "mariadb_ddl_sync",
            .message = "ddl sync skipped: no tables",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
        });
        return stats;
    }

    MariaDbConn mysql(*source);
    runtime.reload(app_pg.raw);

    for (const auto& target : targets) {
        stats.tables_processed += 1;
        try {
            runtime.reload(app_pg.raw);
            const auto result = sync_mariadb_columns_to_lake(
                lake_pg.raw, mysql.handle, target.schema, target.table, runtime, conn_id);
            stats.tables_success += 1;
            stats.columns_added += result.columns_added;
            if (result.columns_added > 0) {
                log_write(log_pg, {
                    .level = LogLevel::Info,
                    .component = "mariadb_ddl_sync",
                    .message = "ddl sync table columns added",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = target.schema,
                    .source_table = target.table,
                    .context = {{"columns_added", result.columns_added}},
                });
            }
        } catch (const std::exception& ex) {
            stats.tables_failed += 1;
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "mariadb_ddl_sync",
                .message = "ddl sync table failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = target.schema,
                .source_table = target.table,
                .context = {{"error", ex.what()}},
            });
        }
    }

    const auto level = stats.tables_failed == 0 ? LogLevel::Info : LogLevel::Warning;
    log_write(log_pg, {
        .level = level,
        .component = "mariadb_ddl_sync",
        .message = stats.tables_failed == 0 ? "ddl sync completed" : "ddl sync completed with errors",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"tables_processed", stats.tables_processed},
            {"tables_success", stats.tables_success},
            {"tables_failed", stats.tables_failed},
            {"columns_added", stats.columns_added},
        },
    });

    return stats;
}

int run_mariadb_ddl_sync_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::optional<std::string>& source_schema,
    const std::optional<std::string>& source_table) {
    const std::string batch_id = make_batch_id();
    const auto stats = run_mariadb_ddl_sync(cfg, log_pg, batch_id, conn_id, service_tier, source_schema, source_table);
    std::cout << "{\"batch_id\":\"" << batch_id << "\",\"tables_processed\":" << stats.tables_processed
              << ",\"tables_success\":" << stats.tables_success << ",\"tables_failed\":" << stats.tables_failed
              << ",\"columns_added\":" << stats.columns_added << "}" << std::endl;
    return stats.tables_failed == 0 ? 0 : 1;
}
