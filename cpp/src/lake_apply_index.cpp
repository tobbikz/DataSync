#include "lake_apply_index.hpp"

#include "capture_common.hpp"
#include "mariadb_schema.hpp"
#include "mongo_lake.hpp"
#include "mssql_lake.hpp"
#include "pg_conn.hpp"

#include <sstream>
#include <mutex>
#include <unordered_set>

namespace {

std::mutex g_mirror_index_ensured_mu;
std::unordered_set<std::string> g_mirror_index_ensured;

bool lake_index_exists(PGconn* pg, const std::string& schema, const std::string& index_name) {
    const char* vals[] = {schema.c_str(), index_name.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'i'
        LIMIT 1
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    const bool exists = res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0;
    if (res) {
        PQclear(res);
    }
    return exists;
}

void resolve_lake_table_names(
    const std::string& db_engine,
    const std::string& source_database,
    const std::string& source_schema,
    const std::string& source_table,
    std::string& lake_schema,
    std::string& lake_table) {
    if (db_engine == "mssql") {
        lake_schema = mssql_pg_schema_name(source_database, source_schema);
        lake_table = mssql_pg_table_name(source_table);
    } else if (db_engine == "mongodb") {
        lake_schema = mongo_pg_schema_name(source_database);
        lake_table = mongo_pg_table_name(source_table);
    } else {
        lake_schema = source_schema;
        lake_table = source_table;
    }
}

}  // namespace

std::string mirror_apply_pk_index_name(const std::string& schema, const std::string& table) {
    std::string idx_name = "dl_mir_" + schema + "_" + table + "_pk";
    if (idx_name.size() > 63) {
        const auto hash = std::hash<std::string>{}(idx_name);
        std::ostringstream suffix;
        suffix << std::hex << (hash & 0xFFFFFFFF);
        const std::string suffix_str = suffix.str();
        idx_name.resize(63 - suffix_str.size() - 1);
        idx_name += "_" + suffix_str;
    }
    return idx_name;
}

void ensure_mirror_apply_pk_index(
    PGconn* pg,
    const std::string& lake_schema,
    const std::string& lake_table,
    const std::vector<std::string>& pk_cols) {
    if (!pg || pk_cols.empty()) {
        return;
    }
    const std::string cache_key = lake_schema + "\0" + lake_table;
    {
        std::lock_guard<std::mutex> lock(g_mirror_index_ensured_mu);
        if (g_mirror_index_ensured.count(cache_key) > 0) {
            return;
        }
    }
    if (!pg_lake_table_exists(pg, lake_schema, lake_table)) {
        return;
    }
    const std::string idx_name = mirror_apply_pk_index_name(lake_schema, lake_table);
    std::ostringstream cols;
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        if (i) {
            cols << ", ";
        }
        cols << pg_ident(pk_cols[i]);
    }
    const std::string fq = pg_ident(lake_schema) + "." + pg_ident(lake_table);
    pg_exec(
        pg,
        "CREATE INDEX IF NOT EXISTS " + pg_ident(idx_name) + " ON " + fq + " (" + cols.str() + ")");
    std::lock_guard<std::mutex> lock(g_mirror_index_ensured_mu);
    g_mirror_index_ensured.insert(cache_key);
}

MirrorApplyIndexBackfillStats backfill_mirror_apply_pk_indexes(
    PGconn* app_pg,
    PGconn* lake_pg,
    const std::string& conn_id) {
    MirrorApplyIndexBackfillStats stats;
    if (!app_pg || !lake_pg) {
        return stats;
    }

    static std::mutex backfill_mu;
    static std::unordered_set<std::string> backfilled_keys;
    const std::string run_key = conn_id.empty() ? "__all__" : conn_id;
    {
        std::lock_guard<std::mutex> lock(backfill_mu);
        if (!backfilled_keys.insert(run_key).second) {
            return stats;
        }
    }

    std::string sql = R"(
        SELECT conn_id, db_engine::text, source_database, source_schema, source_table, pk_columns
        FROM cdc_catalog.catalog
        WHERE active = true
          AND has_pk = true
          AND trim(COALESCE(pk_columns, '')) <> ''
          AND status NOT IN ('skipped', 'disabled')
    )";
    std::vector<const char*> vals;
    if (!conn_id.empty()) {
        sql += " AND conn_id = $1";
        vals.push_back(conn_id.c_str());
    }
    sql += " ORDER BY conn_id, source_schema, source_table";

    PGresult* res = PQexecParams(
        app_pg,
        sql.c_str(),
        static_cast<int>(vals.size()),
        nullptr,
        vals.empty() ? nullptr : vals.data(),
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        stats.errors += 1;
        return stats;
    }

    static thread_local std::unordered_set<std::string> backfill_ensured;
    for (int i = 0; i < PQntuples(res); ++i) {
        stats.tables_seen += 1;
        const std::string db_engine = PQgetvalue(res, i, 1) ? PQgetvalue(res, i, 1) : "mariadb";
        const std::string source_database = PQgetvalue(res, i, 2) ? PQgetvalue(res, i, 2) : "";
        const std::string source_schema = PQgetvalue(res, i, 3) ? PQgetvalue(res, i, 3) : "";
        const std::string source_table = PQgetvalue(res, i, 4) ? PQgetvalue(res, i, 4) : "";
        const std::string pk_columns = PQgetvalue(res, i, 5) ? PQgetvalue(res, i, 5) : "";
        const auto pk_cols = split_pk_columns(pk_columns);
        if (pk_cols.empty()) {
            stats.tables_skipped += 1;
            continue;
        }

        std::string lake_schema;
        std::string lake_table;
        resolve_lake_table_names(db_engine, source_database, source_schema, source_table, lake_schema, lake_table);
        const std::string cache_key = lake_schema + "\0" + lake_table;
        if (backfill_ensured.count(cache_key) > 0) {
            stats.tables_skipped += 1;
            continue;
        }
        if (!pg_lake_table_exists(lake_pg, lake_schema, lake_table)) {
            stats.tables_skipped += 1;
            continue;
        }

        try {
            const std::string idx_name = mirror_apply_pk_index_name(lake_schema, lake_table);
            const bool had_index = lake_index_exists(lake_pg, lake_schema, idx_name);
            ensure_mirror_apply_pk_index(lake_pg, lake_schema, lake_table, pk_cols);
            backfill_ensured.insert(cache_key);
            if (!had_index) {
                stats.indexes_created += 1;
            }
        } catch (...) {
            stats.errors += 1;
        }
    }
    PQclear(res);

    if (stats.errors > 0) {
        std::lock_guard<std::mutex> lock(backfill_mu);
        backfilled_keys.erase(run_key);
    }
    return stats;
}
