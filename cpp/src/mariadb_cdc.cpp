#include "mariadb_cdc.hpp"

#include "capture_common.hpp"
#include "mariadb_binlog_cli.hpp"
#include "mariadb_binlog.hpp"
#include "mariadb_conn.hpp"
#include "mariadb_datetime.hpp"
#include "mariadb_schema.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace {

struct CdcTableMeta {
    long long catalog_id{0};
    std::string source_schema;
    std::string source_table;
    std::vector<std::string> pk_cols;
    std::vector<MariaDbColumn> cols;
};

struct TableKey {
    std::string schema;
    std::string table;
    bool operator<(const TableKey& o) const {
        if (schema != o.schema) {
            return schema < o.schema;
        }
        return table < o.table;
    }
};

struct RowValues {
    std::vector<std::string> values;
    bool pk_only{false};
};

long long elapsed_ms(const std::chrono::steady_clock::time_point& start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start)
        .count();
}

void log_cdc(
    PGconn* log_pg,
    LogLevel level,
    const std::string& batch_id,
    const std::string& message,
    const nlohmann::json& context,
    const std::string& conn_id = {},
    const std::string& schema = {},
    const std::string& table = {}) {
    if (!log_pg) {
        return;
    }
    LogEvent ev;
    ev.level = level;
    ev.component = "mariadb_cdc";
    ev.message = message;
    ev.batch_id = batch_id;
    if (!conn_id.empty()) {
        ev.conn_id = conn_id;
    }
    if (!schema.empty()) {
        ev.source_schema = schema;
    }
    if (!table.empty()) {
        ev.source_table = table;
    }
    ev.context = context;
    log_write(log_pg, ev);
}

std::string utc_now_ts() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S+00");
    return oss.str();
}

std::string utc_now_date() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d");
    return oss.str();
}

void apply_upsert_batch(
    PGconn* pg,
    const CdcTableMeta& meta,
    const std::vector<RowValues>& rows,
    const std::string& snapshot_id) {
    if (rows.empty()) {
        return;
    }

    const std::string load_ts = utc_now_ts();
    const std::string load_date = utc_now_date();
    const std::string fq = pg_ident(meta.source_schema) + "." + pg_ident(meta.source_table);

    std::ostringstream col_list;
    for (const auto& col : meta.cols) {
        col_list << pg_ident(col.name) << ", ";
    }
    col_list << pg_ident("_dl_load_timestamp") << ", " << pg_ident("_dl_load_date") << ", "
             << pg_ident("_dl_source_system") << ", " << pg_ident("_dl_snapshot_id");

    std::ostringstream pk_list;
    for (std::size_t i = 0; i < meta.pk_cols.size(); ++i) {
        if (i) {
            pk_list << ", ";
        }
        pk_list << pg_ident(meta.pk_cols[i]);
    }

    std::ostringstream set_clause;
    for (const auto& col : meta.cols) {
        set_clause << pg_ident(col.name) << " = EXCLUDED." << pg_ident(col.name) << ", ";
    }
    set_clause << pg_ident("_dl_load_timestamp") << " = EXCLUDED." << pg_ident("_dl_load_timestamp") << ", "
               << pg_ident("_dl_load_date") << " = EXCLUDED." << pg_ident("_dl_load_date") << ", "
               << pg_ident("_dl_snapshot_id") << " = EXCLUDED." << pg_ident("_dl_snapshot_id");

    std::ostringstream values_sql;
    for (const auto& row : rows) {
        if (!values_sql.str().empty()) {
            values_sql << ", ";
        }
        values_sql << "(";
        for (std::size_t i = 0; i < row.values.size(); ++i) {
            if (i) {
                values_sql << ", ";
            }
            values_sql << row.values[i];
        }
        values_sql << ", " << pg_escape_literal(load_ts) << ", " << pg_escape_literal(load_date)
                   << ", 'MariaDB', " << pg_escape_literal(snapshot_id) << ")";
    }

    const std::string sql = "INSERT INTO " + fq + " (" + col_list.str() + ") VALUES " + values_sql.str() +
                            " ON CONFLICT (" + pk_list.str() + ") DO UPDATE SET " + set_clause.str();
    pg_exec(pg, sql);
}

void apply_delete_batch(PGconn* pg, const CdcTableMeta& meta, const std::vector<RowValues>& rows) {
    if (rows.empty()) {
        return;
    }

    const std::string fq = pg_ident(meta.source_schema) + "." + pg_ident(meta.source_table);
    std::unordered_map<std::string, std::size_t> pk_index;
    for (std::size_t i = 0; i < meta.cols.size(); ++i) {
        pk_index[meta.cols[i].name] = i;
    }

    for (const auto& row : rows) {
        if (row.pk_only || row.values.size() == meta.pk_cols.size()) {
            std::ostringstream where;
            for (std::size_t i = 0; i < meta.pk_cols.size(); ++i) {
                if (i) {
                    where << " AND ";
                }
                where << pg_ident(meta.pk_cols[i]) << " IS NOT DISTINCT FROM " << row.values[i];
            }
            pg_exec(pg, "DELETE FROM " + fq + " WHERE " + where.str());
            continue;
        }

        std::ostringstream where;
        for (std::size_t i = 0; i < meta.pk_cols.size(); ++i) {
            if (i) {
                where << " AND ";
            }
            const auto it = pk_index.find(meta.pk_cols[i]);
            if (it == pk_index.end()) {
                continue;
            }
            where << pg_ident(meta.pk_cols[i]) << " IS NOT DISTINCT FROM " << row.values[it->second];
        }
        pg_exec(pg, "DELETE FROM " + fq + " WHERE " + where.str());
    }
}

struct ConnCdcState {
    std::map<TableKey, CdcTableMeta> meta_by_key;
    std::set<long long> cdc_active_catalog_ids;
    long long events{0};
    long long upserts{0};
    long long deletes{0};
};

CdcRunStats run_cdc_for_conn(
    const AppConfig& cfg,
    PGconn* log_pg,
    PGconn* pg,
    const MariaDbSource& source,
    const std::vector<CdcTableMeta>& tables,
    RuntimeConfig& runtime,
    const std::string& batch_id,
    const std::optional<std::string>& service_tier) {
    (void)cfg;
    (void)service_tier;
    CdcRunStats stats;
    stats.conns_processed = 1;

    runtime.reload(pg);
    const int max_seconds = runtime.get_int("cdc_max_seconds", 300, "mariadb_cdc", source.conn_id);
    const int max_events = runtime.get_int("cdc_max_events", 50000, "mariadb_cdc", source.conn_id);

    const auto pos = fetch_binlog_position(pg, source.conn_id);
    if (!pos) {
        log_cdc(
            log_pg,
            LogLevel::Warning,
            batch_id,
            "cdc skipped: no binlog position (run full load first)",
            {},
            source.conn_id);
        return stats;
    }

    ConnCdcState state;
    std::set<TableKey> wanted;
    MariaDbConn mariadb(source);
    std::vector<CdcTableMeta> table_metas = tables;
    for (auto& meta : table_metas) {
        meta.cols = fetch_mariadb_columns(mariadb.handle, meta.source_schema, meta.source_table);
        const TableKey key{meta.source_schema, meta.source_table};
        wanted.insert(key);
        state.meta_by_key[key] = meta;
    }

    if (wanted.empty()) {
        return stats;
    }

    clear_stale_cdc_in_progress(pg, source.conn_id, service_tier, "mariadb");

    log_cdc(
        log_pg,
        LogLevel::Info,
        batch_id,
        "cdc conn started",
        {{"binlog_file", pos->file},
         {"binlog_position", pos->position},
         {"tables", wanted.size()},
         {"max_seconds", max_seconds},
         {"max_events", max_events},
         {"reader", "mariadb-binlog"}},
        source.conn_id);

    const auto run_start = std::chrono::steady_clock::now();
    const std::string snapshot_id = batch_id;

    const BinlogCliStats cli_stats = read_remote_binlog_cli(
        source,
        *pos,
        max_seconds,
        max_events,
        [&](const std::string& schema,
            const std::string& table,
            const std::string& op,
            const std::vector<std::string>& col_values,
            const std::vector<std::string>* /*before_col_values*/) {
            const TableKey key{schema, table};
            if (!wanted.count(key)) {
                return;
            }
            const auto& meta = state.meta_by_key[key];
            if (state.cdc_active_catalog_ids.insert(meta.catalog_id).second) {
                mark_catalog_cdc_in_progress(pg, meta.catalog_id);
            }
            RowValues row;
            row.values.resize(meta.cols.size(), "NULL");
            for (std::size_t i = 0; i < col_values.size() && i < row.values.size(); ++i) {
                if (col_values[i].empty()) {
                    row.values[i] = "NULL";
                } else {
                    row.values[i] = normalize_pg_sql_literal(col_values[i], meta.cols[i].pg_type);
                }
            }

            if (op == "DELETE") {
                row.pk_only = true;
                row.values.clear();
                for (const auto& pk : meta.pk_cols) {
                    for (std::size_t i = 0; i < meta.cols.size(); ++i) {
                        if (meta.cols[i].name == pk) {
                            row.values.push_back(
                                i < col_values.size() && !col_values[i].empty() ? col_values[i] : "NULL");
                            break;
                        }
                    }
                }
                apply_delete_batch(pg, meta, {row});
                state.deletes += 1;
            } else {
                apply_upsert_batch(pg, meta, {row}, snapshot_id);
                state.upserts += 1;
            }
            state.events += 1;
        },
        [&]() {
            return elapsed_ms(run_start) >= max_seconds * 1000LL;
        });

    upsert_binlog_position(pg, source.conn_id, cli_stats.last_file, cli_stats.last_position);

    for (long long catalog_id : state.cdc_active_catalog_ids) {
        mark_catalog_cdc_success(pg, catalog_id);
    }

    stats.events_applied = state.events;
    stats.upserts = state.upserts;
    stats.deletes = state.deletes;

    log_cdc(
        log_pg,
        LogLevel::Info,
        batch_id,
        "cdc conn completed",
        {{"events", state.events},
         {"upserts", state.upserts},
         {"deletes", state.deletes},
         {"binlog_file", cli_stats.last_file},
         {"binlog_position", cli_stats.last_position},
         {"duration_ms", elapsed_ms(run_start)}},
        source.conn_id);

    return stats;
}

}  // namespace

CdcRunStats run_mariadb_cdc(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& service_tier) {
    const auto run_start = std::chrono::steady_clock::now();
    CdcRunStats stats;

    PgConn pg(cfg.datasync.conn_string());
    RuntimeConfig runtime;
    runtime.reload(pg.raw);

    log_cdc(
        log_pg,
        LogLevel::Info,
        batch_id,
        "cdc run started",
        {{"service_tier", service_tier.value_or("all")},
         {"max_seconds", runtime.get_int("cdc_max_seconds", 300, "mariadb_cdc")},
         {"max_events", runtime.get_int("cdc_max_events", 50000, "mariadb_cdc")}});

    const char* sql =
        service_tier && !service_tier->empty()
            ? R"(
            SELECT catalog_id, conn_id, source_schema, source_table, COALESCE(pk_columns, '')
            FROM cdc_catalog.catalog
            WHERE db_engine = 'mariadb'
              AND active = true
              AND cdc_enabled = true
              AND needs_full_load = false
              AND has_pk = true
              AND status NOT IN ('skipped', 'disabled')
              AND service_tier::text = lower($1)
            ORDER BY conn_id, source_schema, source_table
            )"
            : R"(
            SELECT catalog_id, conn_id, source_schema, source_table, COALESCE(pk_columns, '')
            FROM cdc_catalog.catalog
            WHERE db_engine = 'mariadb'
              AND active = true
              AND cdc_enabled = true
              AND needs_full_load = false
              AND has_pk = true
              AND status NOT IN ('skipped', 'disabled')
            ORDER BY conn_id, source_schema, source_table
            )";

    PGresult* res = nullptr;
    if (service_tier && !service_tier->empty()) {
        const char* vals[] = {service_tier->c_str()};
        res = PQexecParams(pg.raw, sql, 1, nullptr, vals, nullptr, nullptr, 0);
    } else {
        res = PQexec(pg.raw, sql);
    }
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("failed to fetch cdc catalog rows");
    }

    std::map<std::string, std::vector<CdcTableMeta>> by_conn;
    for (int i = 0; i < PQntuples(res); ++i) {
        CdcTableMeta row;
        row.catalog_id = std::atoll(PQgetvalue(res, i, 0));
        const std::string conn_id = PQgetvalue(res, i, 1);
        row.source_schema = PQgetvalue(res, i, 2);
        row.source_table = PQgetvalue(res, i, 3);
        row.pk_cols = split_pk_columns(PQgetvalue(res, i, 4));
        by_conn[conn_id].push_back(std::move(row));
    }
    PQclear(res);

    if (by_conn.empty()) {
        log_cdc(log_pg, LogLevel::Info, batch_id, "cdc run completed", {{"tables", 0}});
        return stats;
    }

    for (const auto& [conn_id, tables] : by_conn) {
        const MariaDbSource* src = find_mariadb_source(cfg, conn_id);
        if (!src) {
            stats.conns_failed += 1;
            log_cdc(
                log_pg,
                LogLevel::Error,
                batch_id,
                "cdc skipped: unknown conn_id",
                {{"conn_id", conn_id}},
                conn_id);
            continue;
        }
        try {
            const auto conn_stats =
                run_cdc_for_conn(cfg, log_pg, pg.raw, *src, tables, runtime, batch_id, service_tier);
            stats.conns_processed += conn_stats.conns_processed;
            stats.events_applied += conn_stats.events_applied;
            stats.upserts += conn_stats.upserts;
            stats.deletes += conn_stats.deletes;
        } catch (const std::exception& ex) {
            stats.conns_failed += 1;
            log_cdc(
                log_pg,
                LogLevel::Error,
                batch_id,
                "cdc conn failed",
                {{"error", ex.what()}},
                conn_id);
        }
    }

    log_cdc(
        log_pg,
        LogLevel::Info,
        batch_id,
        stats.conns_failed == 0 ? "cdc run completed" : "cdc run completed with errors",
        {{"events_applied", stats.events_applied},
         {"upserts", stats.upserts},
         {"deletes", stats.deletes},
         {"conns_failed", stats.conns_failed},
         {"duration_ms", elapsed_ms(run_start)}});

    return stats;
}
