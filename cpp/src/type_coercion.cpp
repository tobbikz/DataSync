#include "type_coercion.hpp"

#include "capture_common.hpp"
#include "mariadb_conn.hpp"
#include "mariadb_schema.hpp"
#include "obs_log.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <iostream>
#include <sstream>
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
    // Legacy bad loads: bool column landed as text.
    if (expected == "boolean" && (actual == "text" || actual == "character varying")) {
        return false;
    }
    return expected == actual;
}

void flag_catalog_coercion_reload(PGconn* pg, long long catalog_id, int target_version) {
    const std::string id = std::to_string(catalog_id);
    const std::string version = std::to_string(target_version);
    const char* vals[] = {id.c_str(), version.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET needs_full_load = true,
            capture_during_full_load = true,
            cdc_enabled = true,
            status = 'pending'::cdc_catalog.replication_status,
            last_error = 'type coercion reload queued (mariadb v' || $2::text || ')',
            updated_at = now()
        WHERE catalog_id = $1::bigint
          AND active = true
          AND has_pk = true
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

}  // namespace

void stamp_mariadb_coercion_version(PGconn* pg, long long catalog_id) {
    if (!pg || catalog_id <= 0) {
        return;
    }
    const std::string id = std::to_string(catalog_id);
    const std::string version = std::to_string(COERCION_MARIADB_VERSION);
    const char* vals[] = {id.c_str(), version.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET engine_meta = jsonb_set(
                COALESCE(engine_meta, '{}'::jsonb),
                '{coercion_mariadb_version}',
                to_jsonb($2::int),
                true),
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
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

    std::string sql =
        R"(
        SELECT catalog_id, conn_id, source_schema, source_table, engine_meta::text
        FROM cdc_catalog.catalog
        WHERE active = true
          AND has_pk = true
          AND db_engine = 'mariadb'::cdc_catalog.db_engine
        )";
    std::vector<const char*> vals;
    if (conn_id_filter.has_value()) {
        sql += " AND conn_id = $1";
        vals.push_back(conn_id_filter->c_str());
    }
    sql += " ORDER BY conn_id, source_schema, source_table";

    PGresult* res = conn_id_filter.has_value()
        ? PQexecParams(catalog_pg, sql.c_str(), 1, nullptr, vals.data(), nullptr, nullptr, 0)
        : PQexec(catalog_pg, sql.c_str());
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(catalog_pg);
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("coercion audit catalog query failed: " + err);
    }

    const int n = PQntuples(res);
    for (int i = 0; i < n; ++i) {
        const long long catalog_id = std::atoll(PQgetvalue(res, i, 0));
        const std::string conn_id = PQgetvalue(res, i, 1);
        const std::string schema = PQgetvalue(res, i, 2);
        const std::string table = PQgetvalue(res, i, 3);
        const std::string meta_txt = PQgetisnull(res, i, 4) ? "" : PQgetvalue(res, i, 4);
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
                .context = {{"error", ex.what()}, {"catalog_id", catalog_id}},
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

            CoercionColumnFinding finding;
            finding.column = col.name;
            finding.mysql_type = col.mysql_type;
            finding.expected_pg = col.pg_type;
            finding.actual_pg = lake_type.empty() ? "(missing)" : lake_type;
            if (version_stale && type_mismatch) {
                finding.reason = "version_stale+lake_type_mismatch";
            } else if (version_stale) {
                finding.reason = "version_stale";
            } else {
                finding.reason = "lake_type_mismatch";
            }
            stale_row.findings.push_back(std::move(finding));
        }

        if (!stale_row.needs_reload()) {
            continue;
        }

        out.stale.push_back(stale_row);
        if (apply_flags) {
            flag_catalog_coercion_reload(catalog_pg, catalog_id, COERCION_MARIADB_VERSION);
            out.flagged += 1;
            log_write(catalog_pg, {
                .level = LogLevel::Info,
                .component = "type_coercion_audit",
                .message = "catalog flagged for type coercion full-load",
                .conn_id = conn_id,
                .source_schema = schema,
                .source_table = table,
                .context = {
                    {"catalog_id", catalog_id},
                    {"stored_version", stored_version},
                    {"target_version", COERCION_MARIADB_VERSION},
                    {"columns", stale_row.findings.size()},
                },
            });
        }
    }

    if (res) {
        PQclear(res);
    }
    return out;
}

int run_coercion_audit_cli(
    const AppConfig& cfg,
    PGconn* catalog_pg,
    PGconn* lake_pg,
    const std::optional<std::string>& conn_id_filter,
    bool apply_flags) {
    const auto result = audit_mariadb_type_coercion(cfg, catalog_pg, lake_pg, conn_id_filter, apply_flags);

    json report;
    report["engine"] = "mariadb";
    report["coercion_version"] = COERCION_MARIADB_VERSION;
    report["apply"] = apply_flags;
    report["scanned"] = static_cast<int>(result.scanned.size());
    report["stale"] = static_cast<int>(result.stale.size());
    report["flagged"] = result.flagged;

    json stale = json::array();
    for (const auto& table : result.stale) {
        json cols = json::array();
        for (const auto& col : table.findings) {
            cols.push_back({
                {"column", col.column},
                {"mysql_type", col.mysql_type},
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
    std::cout << report.dump(2) << '\n';
    return 0;
}
