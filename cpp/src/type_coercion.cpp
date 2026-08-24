#include "type_coercion.hpp"

#include "capture_common.hpp"
#include "mariadb_conn.hpp"
#include "mariadb_schema.hpp"
#include "mongo_lake.hpp"
#include "mssql_lake.hpp"
#include "obs_log.hpp"

#ifdef HAVE_FREETDS
#include "mssql_conn.hpp"
#include "mssql_schema.hpp"
#endif

#ifdef HAVE_MONGOC
#include "mongo_conn.hpp"
#endif

#include <nlohmann/json.hpp>

#include <iostream>
#include <stdexcept>

namespace {

using json = nlohmann::json;

bool pg_types_compatible(const std::string& expected_pg, const std::string& actual_pg_raw) {
    const std::string actual = coercion_to_lower(actual_pg_raw);
    const std::string expected = coercion_to_lower(expected_pg);
    if (actual.empty()) {
        return false;
    }
    if (expected == actual) {
        return true;
    }
    if (expected == "boolean" && (actual == "bool")) {
        return true;
    }
    if (expected == "timestamptz" && (actual == "timestamp with time zone")) {
        return true;
    }
    if (expected == "bytea" && actual == "bytea") {
        return true;
    }
    if (expected == "boolean" && (actual == "text" || actual == "character varying")) {
        return false;
    }
    return expected == actual;
}

void flag_catalog_coercion_reload(
    PGconn* pg,
    long long catalog_id,
    const std::string& db_engine,
    int target_version) {
    const std::string id = std::to_string(catalog_id);
    const std::string version = std::to_string(target_version);
    const char* vals[] = {id.c_str(), db_engine.c_str(), version.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET needs_full_load = true,
            capture_during_full_load = true,
            cdc_enabled = true,
            status = 'pending'::cdc_catalog.replication_status,
            last_error = 'type coercion reload queued (' || $2::text || ' v' || $3::text || ')',
            updated_at = now()
        WHERE catalog_id = $1::bigint
          AND active = true
          AND has_pk = true
        )",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

void stamp_coercion_version(PGconn* pg, long long catalog_id, const char* meta_key, int version) {
    if (!pg || catalog_id <= 0 || !meta_key || !*meta_key) {
        return;
    }
    const std::string id = std::to_string(catalog_id);
    const std::string version_txt = std::to_string(version);
    const char* vals[] = {id.c_str(), meta_key, version_txt.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET engine_meta = jsonb_set(
                COALESCE(engine_meta, '{}'::jsonb),
                ARRAY[$2::text],
                to_jsonb($3::int),
                true),
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

CoercionColumnFinding make_finding(
    const std::string& column,
    const std::string& source_type,
    const std::string& expected_pg,
    const std::string& lake_type,
    bool version_stale,
    bool type_mismatch) {
    CoercionColumnFinding finding;
    finding.column = column;
    finding.source_type = source_type;
    finding.expected_pg = expected_pg;
    finding.actual_pg = lake_type.empty() ? "(missing)" : lake_type;
    if (version_stale && type_mismatch) {
        finding.reason = "version_stale+lake_type_mismatch";
    } else if (version_stale) {
        finding.reason = "version_stale";
    } else {
        finding.reason = "lake_type_mismatch";
    }
    return finding;
}

PGresult* query_engine_catalog(
    PGconn* catalog_pg,
    const char* db_engine,
    const std::optional<std::string>& conn_id_filter) {
    std::string sql =
        R"(
        SELECT catalog_id, conn_id, source_database, source_schema, source_table, engine_meta::text
        FROM cdc_catalog.catalog
        WHERE active = true
          AND has_pk = true
          AND db_engine = )";
    sql += db_engine;
    sql += R"(
        )";
    std::vector<const char*> vals;
    if (conn_id_filter.has_value()) {
        sql += " AND conn_id = $1";
        vals.push_back(conn_id_filter->c_str());
    }
    sql += " ORDER BY conn_id, source_schema, source_table";
    return conn_id_filter.has_value()
        ? PQexecParams(catalog_pg, sql.c_str(), 1, nullptr, vals.data(), nullptr, nullptr, 0)
        : PQexec(catalog_pg, sql.c_str());
}

json engine_report_json(
    const std::string& engine,
    int coercion_version,
    const CoercionAuditResult& result) {
    json report;
    report["engine"] = engine;
    report["coercion_version"] = coercion_version;
    report["scanned"] = static_cast<int>(result.scanned.size());
    report["stale"] = static_cast<int>(result.stale.size());
    report["flagged"] = result.flagged;

    json stale = json::array();
    for (const auto& table : result.stale) {
        json cols = json::array();
        for (const auto& col : table.findings) {
            cols.push_back({
                {"column", col.column},
                {"source_type", col.source_type},
                {"expected_pg", col.expected_pg},
                {"actual_pg", col.actual_pg},
                {"reason", col.reason},
            });
        }
        stale.push_back({
            {"catalog_id", table.catalog_id},
            {"conn_id", table.conn_id},
            {"schema", table.source_schema},
            {"table", table.source_table},
            {"stored_version", table.stored_version},
            {"columns", cols},
        });
    }
    report["tables"] = stale;
    return report;
}

}  // namespace

void stamp_mariadb_coercion_version(PGconn* pg, long long catalog_id) {
    stamp_coercion_version(pg, catalog_id, kCoercionMariaDbMetaKey, COERCION_MARIADB_VERSION);
}

void stamp_mssql_coercion_version(PGconn* pg, long long catalog_id) {
    stamp_coercion_version(pg, catalog_id, kCoercionMssqlMetaKey, COERCION_MSSQL_VERSION);
}

void stamp_mongodb_coercion_version(PGconn* pg, long long catalog_id) {
    stamp_coercion_version(pg, catalog_id, kCoercionMongodbMetaKey, COERCION_MONGODB_VERSION);
}

void stamp_coercion_version_for_engine(PGconn* pg, long long catalog_id, const std::string& db_engine) {
    if (db_engine == "mariadb") {
        stamp_mariadb_coercion_version(pg, catalog_id);
    } else if (db_engine == "mssql") {
        stamp_mssql_coercion_version(pg, catalog_id);
    } else if (db_engine == "mongodb") {
        stamp_mongodb_coercion_version(pg, catalog_id);
    }
}

CoercionAuditResult audit_mariadb_type_coercion(
    const AppConfig& cfg,
    PGconn* catalog_pg,
    PGconn* lake_pg,
    const std::optional<std::string>& conn_id_filter,
    bool apply_flags) {
    CoercionAuditResult out;
    if (!catalog_pg || PQstatus(catalog_pg) != CONNECTION_OK) {
        throw std::runtime_error("catalog pg connection invalid");
    }
    if (!lake_pg || PQstatus(lake_pg) != CONNECTION_OK) {
        throw std::runtime_error("lake pg connection invalid");
    }

    PGresult* res = query_engine_catalog(catalog_pg, "'mariadb'::cdc_catalog.db_engine", conn_id_filter);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(catalog_pg);
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("coercion audit mariadb catalog query failed: " + err);
    }

    const int n = PQntuples(res);
    for (int i = 0; i < n; ++i) {
        const long long catalog_id = std::atoll(PQgetvalue(res, i, 0));
        const std::string conn_id = PQgetvalue(res, i, 1);
        const std::string schema = PQgetvalue(res, i, 3);
        const std::string table = PQgetvalue(res, i, 4);
        const std::string meta_txt = PQgetisnull(res, i, 5) ? "" : PQgetvalue(res, i, 5);
        const int stored_version = mariadb_coercion_version_from_meta(meta_txt);

        CoercionTableAudit row;
        row.catalog_id = catalog_id;
        row.conn_id = conn_id;
        row.source_schema = schema;
        row.source_table = table;
        row.stored_version = stored_version;
        out.scanned.push_back(row);

        const MariaDbSource* source = find_mariadb_source(cfg, conn_id);
        if (!source) {
            continue;
        }

        MariaDbConn mariadb(*source);
        std::vector<MariaDbColumn> cols;
        try {
            cols = fetch_mariadb_columns(mariadb.handle, schema, table);
        } catch (const std::exception& ex) {
            log_write(catalog_pg, {
                .level = LogLevel::Warning,
                .component = "type_coercion_audit",
                .message = "skipped table: could not read MariaDB columns",
                .conn_id = conn_id,
                .source_schema = schema,
                .source_table = table,
                .context = {{"error", ex.what()}, {"catalog_id", catalog_id}, {"db_engine", "mariadb"}},
            });
            continue;
        }

        CoercionTableAudit stale_row = row;
        for (const auto& col : cols) {
            if (!mariadb_column_coercion_sensitive(col.mysql_type)) {
                continue;
            }
            const bool version_stale = stored_version < COERCION_MARIADB_VERSION;
            const std::string lake_type =
                pg_lake_table_exists(lake_pg, schema, table)
                    ? lake_column_data_type(lake_pg, schema, table, col.name)
                    : std::string{};
            const bool type_mismatch = !lake_type.empty() && !pg_types_compatible(col.pg_type, lake_type);
            if (!version_stale && !type_mismatch) {
                continue;
            }
            stale_row.findings.push_back(
                make_finding(col.name, col.mysql_type, col.pg_type, lake_type, version_stale, type_mismatch));
        }

        if (!stale_row.needs_reload()) {
            continue;
        }

        out.stale.push_back(stale_row);
        if (apply_flags) {
            flag_catalog_coercion_reload(catalog_pg, catalog_id, "mariadb", COERCION_MARIADB_VERSION);
            out.flagged += 1;
            log_write(catalog_pg, {
                .level = LogLevel::Info,
                .component = "type_coercion_audit",
                .message = "catalog flagged for type coercion full-load",
                .conn_id = conn_id,
                .source_schema = schema,
                .source_table = table,
                .context = {
                    {"db_engine", "mariadb"},
                    {"catalog_id", catalog_id},
                    {"stored_version", stored_version},
                    {"target_version", COERCION_MARIADB_VERSION},
                    {"columns", stale_row.findings.size()},
                },
            });
        }
    }

    PQclear(res);
    return out;
}

#ifdef HAVE_FREETDS
CoercionAuditResult audit_mssql_type_coercion(
    const AppConfig& cfg,
    PGconn* catalog_pg,
    PGconn* lake_pg,
    const std::optional<std::string>& conn_id_filter,
    bool apply_flags) {
    CoercionAuditResult out;
    if (!catalog_pg || PQstatus(catalog_pg) != CONNECTION_OK) {
        throw std::runtime_error("catalog pg connection invalid");
    }
    if (!lake_pg || PQstatus(lake_pg) != CONNECTION_OK) {
        throw std::runtime_error("lake pg connection invalid");
    }

    PGresult* res = query_engine_catalog(catalog_pg, "'mssql'::cdc_catalog.db_engine", conn_id_filter);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(catalog_pg);
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("coercion audit mssql catalog query failed: " + err);
    }

    const int n = PQntuples(res);
    for (int i = 0; i < n; ++i) {
        const long long catalog_id = std::atoll(PQgetvalue(res, i, 0));
        const std::string conn_id = PQgetvalue(res, i, 1);
        const std::string source_database = PQgetvalue(res, i, 2);
        const std::string schema = PQgetvalue(res, i, 3);
        const std::string table = PQgetvalue(res, i, 4);
        const std::string meta_txt = PQgetisnull(res, i, 5) ? "" : PQgetvalue(res, i, 5);
        const int stored_version = mssql_coercion_version_from_meta(meta_txt);

        CoercionTableAudit row;
        row.catalog_id = catalog_id;
        row.conn_id = conn_id;
        row.source_schema = schema;
        row.source_table = table;
        row.stored_version = stored_version;
        out.scanned.push_back(row);

        const MssqlSource* source = find_mssql_source(cfg, conn_id);
        if (!source) {
            continue;
        }

        MssqlConn mssql(*source);
        mssql.use_database(source_database);
        std::vector<MssqlColumn> cols;
        try {
            cols = fetch_mssql_columns(mssql.handle, schema, table);
        } catch (const std::exception& ex) {
            log_write(catalog_pg, {
                .level = LogLevel::Warning,
                .component = "type_coercion_audit",
                .message = "skipped table: could not read MSSQL columns",
                .conn_id = conn_id,
                .source_schema = schema,
                .source_table = table,
                .context = {{"error", ex.what()}, {"catalog_id", catalog_id}, {"db_engine", "mssql"}},
            });
            continue;
        }

        const std::string lake_schema = mssql_pg_schema_name(source_database, schema);
        const std::string lake_table = mssql_pg_table_name(table);
        CoercionTableAudit stale_row = row;
        for (const auto& col : cols) {
            if (!mssql_column_coercion_sensitive(col.mssql_type)) {
                continue;
            }
            const bool version_stale = stored_version < COERCION_MSSQL_VERSION;
            const std::string lake_type =
                pg_lake_table_exists(lake_pg, lake_schema, lake_table)
                    ? lake_column_data_type(lake_pg, lake_schema, lake_table, col.name)
                    : std::string{};
            const bool type_mismatch = !lake_type.empty() && !pg_types_compatible(col.pg_type, lake_type);
            if (!version_stale && !type_mismatch) {
                continue;
            }
            stale_row.findings.push_back(
                make_finding(col.name, col.mssql_type, col.pg_type, lake_type, version_stale, type_mismatch));
        }

        if (!stale_row.needs_reload()) {
            continue;
        }

        out.stale.push_back(stale_row);
        if (apply_flags) {
            flag_catalog_coercion_reload(catalog_pg, catalog_id, "mssql", COERCION_MSSQL_VERSION);
            out.flagged += 1;
            log_write(catalog_pg, {
                .level = LogLevel::Info,
                .component = "type_coercion_audit",
                .message = "catalog flagged for type coercion full-load",
                .conn_id = conn_id,
                .source_schema = schema,
                .source_table = table,
                .context = {
                    {"db_engine", "mssql"},
                    {"catalog_id", catalog_id},
                    {"stored_version", stored_version},
                    {"target_version", COERCION_MSSQL_VERSION},
                    {"columns", stale_row.findings.size()},
                },
            });
        }
    }

    PQclear(res);
    return out;
}
#else
CoercionAuditResult audit_mssql_type_coercion(
    const AppConfig&,
    PGconn*,
    PGconn*,
    const std::optional<std::string>&,
    bool) {
    return {};
}
#endif

#ifdef HAVE_MONGOC
CoercionAuditResult audit_mongodb_type_coercion(
    const AppConfig& cfg,
    PGconn* catalog_pg,
    PGconn* lake_pg,
    const std::optional<std::string>& conn_id_filter,
    bool apply_flags) {
    CoercionAuditResult out;
    if (!catalog_pg || PQstatus(catalog_pg) != CONNECTION_OK) {
        throw std::runtime_error("catalog pg connection invalid");
    }
    if (!lake_pg || PQstatus(lake_pg) != CONNECTION_OK) {
        throw std::runtime_error("lake pg connection invalid");
    }

    PGresult* res = query_engine_catalog(catalog_pg, "'mongodb'::cdc_catalog.db_engine", conn_id_filter);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(catalog_pg);
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("coercion audit mongodb catalog query failed: " + err);
    }

    const int n = PQntuples(res);
    for (int i = 0; i < n; ++i) {
        const long long catalog_id = std::atoll(PQgetvalue(res, i, 0));
        const std::string conn_id = PQgetvalue(res, i, 1);
        const std::string source_database = PQgetvalue(res, i, 2);
        const std::string schema = PQgetvalue(res, i, 3);
        const std::string table = PQgetvalue(res, i, 4);
        const std::string meta_txt = PQgetisnull(res, i, 5) ? "" : PQgetvalue(res, i, 5);
        const int stored_version = mongodb_coercion_version_from_meta(meta_txt);

        CoercionTableAudit row;
        row.catalog_id = catalog_id;
        row.conn_id = conn_id;
        row.source_schema = schema;
        row.source_table = table;
        row.stored_version = stored_version;
        out.scanned.push_back(row);

        const MongoSource* source = find_mongo_source(cfg, conn_id);
        if (!source) {
            continue;
        }

        MongoConn mongo(*source);
        mongoc_collection_t* coll = mongo.collection(source_database, table);
        if (!coll) {
            log_write(catalog_pg, {
                .level = LogLevel::Warning,
                .component = "type_coercion_audit",
                .message = "skipped collection: could not open Mongo collection",
                .conn_id = conn_id,
                .source_schema = schema,
                .source_table = table,
                .context = {{"catalog_id", catalog_id}, {"db_engine", "mongodb"}},
            });
            continue;
        }

        std::map<std::string, std::string> inferred_cols;
        try {
            const auto sample = sample_flattened_mongo_docs(coll, 1000);
            inferred_cols = infer_schema_from_flat_rows(sample, 1000);
        } catch (const std::exception& ex) {
            log_write(catalog_pg, {
                .level = LogLevel::Warning,
                .component = "type_coercion_audit",
                .message = "skipped collection: could not sample Mongo documents",
                .conn_id = conn_id,
                .source_schema = schema,
                .source_table = table,
                .context = {{"error", ex.what()}, {"catalog_id", catalog_id}, {"db_engine", "mongodb"}},
            });
            continue;
        }

        const std::string lake_schema = mongo_pg_schema_name(source_database);
        const std::string lake_table = mongo_pg_table_name(table);
        CoercionTableAudit stale_row = row;
        for (const auto& [col_name, pg_type] : inferred_cols) {
            if (!mongo_inferred_type_coercion_sensitive(pg_type)) {
                continue;
            }
            const bool version_stale = stored_version < COERCION_MONGODB_VERSION;
            const std::string lake_type =
                pg_lake_table_exists(lake_pg, lake_schema, lake_table)
                    ? lake_column_data_type(lake_pg, lake_schema, lake_table, col_name)
                    : std::string{};
            const bool type_mismatch = !lake_type.empty() && !pg_types_compatible(pg_type, lake_type);
            if (!version_stale && !type_mismatch) {
                continue;
            }
            stale_row.findings.push_back(
                make_finding(col_name, "inferred:" + pg_type, pg_type, lake_type, version_stale, type_mismatch));
        }

        if (!stale_row.needs_reload()) {
            continue;
        }

        out.stale.push_back(stale_row);
        if (apply_flags) {
            flag_catalog_coercion_reload(catalog_pg, catalog_id, "mongodb", COERCION_MONGODB_VERSION);
            out.flagged += 1;
            log_write(catalog_pg, {
                .level = LogLevel::Info,
                .component = "type_coercion_audit",
                .message = "catalog flagged for type coercion full-load",
                .conn_id = conn_id,
                .source_schema = schema,
                .source_table = table,
                .context = {
                    {"db_engine", "mongodb"},
                    {"catalog_id", catalog_id},
                    {"stored_version", stored_version},
                    {"target_version", COERCION_MONGODB_VERSION},
                    {"columns", stale_row.findings.size()},
                },
            });
        }
    }

    PQclear(res);
    return out;
}
#else
CoercionAuditResult audit_mongodb_type_coercion(
    const AppConfig&,
    PGconn*,
    PGconn*,
    const std::optional<std::string>&,
    bool) {
    return {};
}
#endif

int run_coercion_audit_cli(
    const AppConfig& cfg,
    PGconn* catalog_pg,
    PGconn* lake_pg,
    const std::optional<std::string>& conn_id_filter,
    bool apply_flags) {
    const auto mariadb = audit_mariadb_type_coercion(cfg, catalog_pg, lake_pg, conn_id_filter, apply_flags);
    const auto mssql = audit_mssql_type_coercion(cfg, catalog_pg, lake_pg, conn_id_filter, apply_flags);
    const auto mongodb = audit_mongodb_type_coercion(cfg, catalog_pg, lake_pg, conn_id_filter, apply_flags);

    json report;
    report["apply"] = apply_flags;
    report["engines"] = json::array({"mariadb", "mssql", "mongodb"});
    report["mariadb"] = engine_report_json("mariadb", COERCION_MARIADB_VERSION, mariadb);
    report["mssql"] = engine_report_json("mssql", COERCION_MSSQL_VERSION, mssql);
    report["mongodb"] = engine_report_json("mongodb", COERCION_MONGODB_VERSION, mongodb);
    report["scanned"] = static_cast<int>(mariadb.scanned.size() + mssql.scanned.size() + mongodb.scanned.size());
    report["stale"] =
        static_cast<int>(mariadb.stale.size() + mssql.stale.size() + mongodb.stale.size());
    report["flagged"] = mariadb.flagged + mssql.flagged + mongodb.flagged;
    std::cout << report.dump(2) << '\n';
    return 0;
}
