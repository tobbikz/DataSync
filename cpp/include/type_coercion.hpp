#pragma once

#include "config.hpp"

#include <libpq-fe.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <optional>
#include <string>
#include <vector>

/** Bump when COPY/envelope coercion rules change for that engine. */
inline constexpr int COERCION_MARIADB_VERSION = 1;
inline constexpr int COERCION_MSSQL_VERSION = 1;
inline constexpr int COERCION_MONGODB_VERSION = 1;

inline constexpr const char* kCoercionMariaDbMetaKey = "coercion_mariadb_version";
inline constexpr const char* kCoercionMssqlMetaKey = "coercion_mssql_version";
inline constexpr const char* kCoercionMongodbMetaKey = "coercion_mongodb_version";

inline std::string coercion_to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

inline int coercion_version_from_meta(const std::string& engine_meta_text, const char* meta_key) {
    if (engine_meta_text.empty() || !meta_key || !*meta_key) {
        return -1;
    }
    try {
        const auto meta = nlohmann::json::parse(engine_meta_text);
        if (meta.contains(meta_key) && meta[meta_key].is_number_integer()) {
            return meta[meta_key].get<int>();
        }
    } catch (...) {
    }
    return -1;
}

inline bool mariadb_column_coercion_sensitive(const std::string& mysql_column_type) {
    const std::string t = coercion_to_lower(mysql_column_type);
    if (t.rfind("bit", 0) == 0) {
        return true;
    }
    if (t.rfind("tinyint(1)", 0) == 0) {
        return true;
    }
    if (t.find("datetime") != std::string::npos || t.rfind("timestamp", 0) == 0) {
        return true;
    }
    return false;
}

inline bool mssql_column_coercion_sensitive(const std::string& mssql_column_type) {
    const std::string t = coercion_to_lower(mssql_column_type);
    if (t == "bit") {
        return true;
    }
    if (t == "datetime" || t == "datetime2" || t == "smalldatetime") {
        return true;
    }
    return false;
}

/** Inferred PG type from Mongo sample (BOOLEAN / future TIMESTAMPTZ). */
inline bool mongo_inferred_type_coercion_sensitive(const std::string& pg_type) {
    const std::string t = coercion_to_lower(pg_type);
    return t == "boolean" || t == "timestamptz";
}

inline int mariadb_coercion_version_from_meta(const std::string& engine_meta_text) {
    return coercion_version_from_meta(engine_meta_text, kCoercionMariaDbMetaKey);
}

inline int mssql_coercion_version_from_meta(const std::string& engine_meta_text) {
    return coercion_version_from_meta(engine_meta_text, kCoercionMssqlMetaKey);
}

inline int mongodb_coercion_version_from_meta(const std::string& engine_meta_text) {
    return coercion_version_from_meta(engine_meta_text, kCoercionMongodbMetaKey);
}

struct CoercionColumnFinding {
    std::string column;
    std::string source_type;
    std::string expected_pg;
    std::string actual_pg;
    std::string reason;
};

struct CoercionTableAudit {
    long long catalog_id{0};
    std::string conn_id;
    std::string source_schema;
    std::string source_table;
    int stored_version{-1};
    std::vector<CoercionColumnFinding> findings;

    bool needs_reload() const { return !findings.empty(); }
};

struct CoercionAuditResult {
    std::vector<CoercionTableAudit> scanned;
    std::vector<CoercionTableAudit> stale;
    int flagged{0};
};

/** Dry-run unless apply_flags=true (sets needs_full_load on stale tables). */
CoercionAuditResult audit_mariadb_type_coercion(
    const AppConfig& cfg,
    PGconn* catalog_pg,
    PGconn* lake_pg,
    const std::optional<std::string>& conn_id_filter,
    bool apply_flags);

CoercionAuditResult audit_mssql_type_coercion(
    const AppConfig& cfg,
    PGconn* catalog_pg,
    PGconn* lake_pg,
    const std::optional<std::string>& conn_id_filter,
    bool apply_flags);

CoercionAuditResult audit_mongodb_type_coercion(
    const AppConfig& cfg,
    PGconn* catalog_pg,
    PGconn* lake_pg,
    const std::optional<std::string>& conn_id_filter,
    bool apply_flags);

void stamp_mariadb_coercion_version(PGconn* pg, long long catalog_id);
void stamp_mssql_coercion_version(PGconn* pg, long long catalog_id);
void stamp_mongodb_coercion_version(PGconn* pg, long long catalog_id);

void stamp_coercion_version_for_engine(PGconn* pg, long long catalog_id, const std::string& db_engine);

int run_coercion_audit_cli(
    const AppConfig& cfg,
    PGconn* catalog_pg,
    PGconn* lake_pg,
    const std::optional<std::string>& conn_id_filter,
    bool apply_flags);
