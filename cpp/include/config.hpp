#pragma once

#include <cstdint>
#include <string>
#include <vector>

struct PgConfig {
    std::string host{"localhost"};
    std::uint16_t port{5432};
    std::string database{"DataLake"};
    std::string user;
    std::string password;
    std::string sslmode;
    std::string conn_string() const;
};

struct MariaDbSource {
    std::string conn_id;
    std::string host{"localhost"};
    std::uint16_t port{3306};
    std::string db_name;
    std::string user;
    std::string password;
};

struct MssqlSource {
    std::string conn_id;
    std::string host{"localhost"};
    std::uint16_t port{1433};
    std::string db_name;
    std::string user;
    std::string password;
};

struct MongoSource {
    std::string conn_id;
    std::string host{"localhost"};
    std::uint16_t port{27017};
    std::string db_name;
    std::string user;
    std::string password;
    std::string replica_set;
    bool replica_set_in_extras{false};
};

/** CDC daemon tuning — config.json only (not runtime_config). */
struct CdcConfig {
    int round_idle_seconds{5};
    int slice_max_seconds{60};
    int slice_max_events{10'000'000};
};

struct AppConfig {
    /** Control plane: catalog, runtime_config, logs, dedup, apply_position. */
    PgConfig datasync;
    /** Lake targets: full-load COPY and CDC apply INSERT/COPY. */
    PgConfig datalake;
    std::vector<MariaDbSource> mariadb_sources;
    std::vector<MssqlSource> mssql_sources;
    std::vector<MongoSource> mongo_sources;
    CdcConfig cdc;
};

CdcConfig default_cdc_config();

/** Load DataSync + DataLake PG from config.json (project root). Sources from cdc_catalog.connections. */
AppConfig load_config(const std::string& path);
AppConfig load_config_auto(const char* argv0);
/** @deprecated use load_config_auto */
AppConfig load_config_from_env();

/** Returns "mariadb", "mssql", or "mongodb" for a conn_id in config. */
std::string conn_engine(const AppConfig& cfg, const std::string& conn_id);
