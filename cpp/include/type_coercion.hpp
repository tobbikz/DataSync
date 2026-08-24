#pragma once

#include "config.hpp"

#include <libpq-fe.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <optional>
#include <string>
#include <vector>

/** Bump when MariaDB COPY/envelope coercion rules change (BIT→BOOLEAN, etc.). */
inline constexpr int COERCION_MARIADB_VERSION = 1;

inline std::string coercion_to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
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

inline int mariadb_coercion_version_from_meta(const std::string& engine_meta_text) {
    if (engine_meta_text.empty()) {
        return -1;
    }
    try {
        const auto meta = nlohmann::json::parse(engine_meta_text);
        if (meta.contains("coercion_mariadb_version") && meta["coercion_mariadb_version"].is_number_integer()) {
            return meta["coercion_mariadb_version"].get<int>();
        }
    } catch (...) {
    }
    return -1;
}

struct CoercionColumnFinding {
    std::string column;
    std::string mysql_type;
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

/** Dry-run unless apply_flags=true (sets needs_full_load on stale tables). MariaDB only. */
CoercionAuditResult audit_mariadb_type_coercion(
    const AppConfig& cfg,
    PGconn* catalog_pg,
    PGconn* lake_pg,
    const std::optional<std::string>& conn_id_filter,
    bool apply_flags);

void stamp_mariadb_coercion_version(PGconn* pg, long long catalog_id);

int run_coercion_audit_cli(
    const AppConfig& cfg,
    PGconn* catalog_pg,
    PGconn* lake_pg,
    const std::optional<std::string>& conn_id_filter,
    bool apply_flags);
