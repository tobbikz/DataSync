#include "config.hpp"

#include <nlohmann/json.hpp>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <algorithm>

namespace {

std::uint16_t json_port(const nlohmann::json& j, const char* key, std::uint16_t fallback) {
    if (!j.contains(key)) {
        return fallback;
    }
    if (j[key].is_number_unsigned() || j[key].is_number_integer()) {
        const int p = j[key].get<int>();
        if (p > 0 && p <= 65535) {
            return static_cast<std::uint16_t>(p);
        }
    }
    return fallback;
}

PgConfig parse_pg(const nlohmann::json& j, const PgConfig& defaults = {}) {
    PgConfig pg;
    pg.host = j.value("host", defaults.host.empty() ? "localhost" : defaults.host);
    pg.port = json_port(j, "port", defaults.port ? defaults.port : 5432);
    pg.database = j.value("database", defaults.database.empty() ? "DataLake" : defaults.database);
    pg.user = j.value("user", defaults.user);
    pg.password = j.value("password", defaults.password);
    pg.sslmode = j.value("sslmode", defaults.sslmode);
    if (pg.user.empty()) {
        throw std::runtime_error("config.json: postgres user is required");
    }
    if (pg.password.empty()) {
        throw std::runtime_error("config.json: postgres password is required");
    }
    return pg;
}

std::filesystem::path executable_dir(const char* argv0) {
    if (!argv0 || !*argv0) {
        return std::filesystem::current_path();
    }
    std::error_code ec;
    const auto p = std::filesystem::absolute(argv0, ec);
    if (ec) {
        return std::filesystem::current_path();
    }
    return p.parent_path();
}

std::filesystem::path find_default_config_path(const char* argv0) {
    if (const char* env = std::getenv("DATASYNC_CONFIG")) {
        return env;
    }
    if (const char* root = std::getenv("DATASYNC_ROOT")) {
        const auto p = std::filesystem::path(root) / "config.json";
        if (std::filesystem::exists(p)) {
            return p;
        }
    }

    const std::filesystem::path cwd_cfg = std::filesystem::current_path() / "config.json";
    if (std::filesystem::exists(cwd_cfg)) {
        return cwd_cfg;
    }

    // cpp/build/DataSync -> project root/config.json
    const auto bin_dir = executable_dir(argv0);
    const auto project_cfg = bin_dir.parent_path().parent_path() / "config.json";
    if (std::filesystem::exists(project_cfg)) {
        return project_cfg;
    }

    throw std::runtime_error(
        "config.json not found — copy config.json.example to project root or set DATASYNC_CONFIG");
}

}  // namespace

CdcConfig default_cdc_config() {
    CdcConfig cdc;
    cdc.round_idle_seconds = 5;
    cdc.slice_max_seconds = 15;
    cdc.slice_max_events = 10'000'000;
    return cdc;
}

namespace {

void parse_cdc_config(const nlohmann::json& root, CdcConfig& cdc) {
    cdc = default_cdc_config();
    if (!root.contains("cdc") || !root["cdc"].is_object()) {
        return;
    }
    const auto& j = root["cdc"];
    if (j.contains("round_idle_seconds")) {
        cdc.round_idle_seconds = j["round_idle_seconds"].get<int>();
    }
    if (j.contains("slice_max_seconds")) {
        cdc.slice_max_seconds = j["slice_max_seconds"].get<int>();
    }
    if (j.contains("slice_max_events")) {
        cdc.slice_max_events = j["slice_max_events"].get<int>();
    }
}

std::string pg_quote_conn_value(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('\'');
    for (const char c : value) {
        if (c == '\'') {
            out += "''";
        } else if (c == '\\') {
            out += "\\\\";
        } else {
            out.push_back(c);
        }
    }
    out.push_back('\'');
    return out;
}

}  // namespace

std::string PgConfig::conn_string() const {
    std::string s = "host=" + host +
                    " port=" + std::to_string(port) +
                    " dbname=" + database +
                    " user=" + pg_quote_conn_value(user) +
                    " password=" + pg_quote_conn_value(password);
    if (!sslmode.empty()) {
        s += " sslmode=" + sslmode;
    }
    return s;
}

AppConfig load_config(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("cannot open config: " + path);
    }

    nlohmann::json root;
    in >> root;

    if (!root.contains("datasync") || !root["datasync"].is_object()) {
        throw std::runtime_error("config.json: missing object \"datasync\"");
    }

    AppConfig cfg;
    cfg.datasync = parse_pg(root["datasync"]);

    if (root.contains("datalake") && root["datalake"].is_object()) {
        cfg.datalake = parse_pg(root["datalake"], cfg.datasync);
    } else {
        cfg.datalake = cfg.datasync;
        if (root.contains("datalake") && root["datalake"].is_string()) {
            cfg.datalake.database = root["datalake"].get<std::string>();
        }
    }

    parse_cdc_config(root, cfg.cdc);

    if (const char* env = std::getenv("DATASYNC_PG_PASSWORD")) {
        if (env[0] != '\0') {
            cfg.datasync.password = env;
        }
    }
    if (const char* env = std::getenv("DATALAKE_PG_PASSWORD")) {
        if (env[0] != '\0') {
            cfg.datalake.password = env;
        }
    }
    if (const char* env = std::getenv("DATASYNC_PG_SSLMODE")) {
        cfg.datasync.sslmode = env;
    }
    if (const char* env = std::getenv("DATALAKE_PG_SSLMODE")) {
        cfg.datalake.sslmode = env;
    }

    return cfg;
}

AppConfig load_config_from_env() {
    return load_config(find_default_config_path(nullptr).string());
}

AppConfig load_config_auto(const char* argv0) {
    return load_config(find_default_config_path(argv0).string());
}

std::string conn_engine(const AppConfig& cfg, const std::string& conn_id) {
    for (const auto& m : cfg.mariadb_sources) {
        if (m.conn_id == conn_id) {
            return "mariadb";
        }
    }
    for (const auto& m : cfg.mssql_sources) {
        if (m.conn_id == conn_id) {
            return "mssql";
        }
    }
    for (const auto& m : cfg.mongo_sources) {
        if (m.conn_id == conn_id) {
            return "mongodb";
        }
    }
    throw std::runtime_error(
        "conn_id not found in cdc_catalog.connections (reload after bootstrap): " + conn_id);
}
