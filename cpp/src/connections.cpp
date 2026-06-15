#include "connections.hpp"
#include "obs_log.hpp"

#include <nlohmann/json.hpp>

#include <cstdlib>

namespace {

std::mutex& connections_mutex_instance() {
    static std::mutex mutex;
    return mutex;
}

std::uint16_t parse_port(const char* text, std::uint16_t fallback) {
    if (!text || !*text) {
        return fallback;
    }
    const int p = std::atoi(text);
    if (p <= 0 || p > 65535) {
        return fallback;
    }
    return static_cast<std::uint16_t>(p);
}

std::string extras_string(PGresult* res, int row) {
    const char* v = PQgetvalue(res, row, 7);
    return v ? v : "{}";
}

std::string normalize_tcp_host(std::string host) {
    if (host == "localhost") {
        return "127.0.0.1";
    }
    return host.empty() ? "127.0.0.1" : host;
}

// Force TCP for remote tooling; keep "localhost" when password empty (unix socket / peer auth).
std::string normalize_mariadb_host(std::string host, bool force_tcp) {
    if (!force_tcp) {
        return host.empty() ? "localhost" : host;
    }
    if (host == "localhost") {
        return "127.0.0.1";
    }
    return host;
}

MariaDbSource row_to_mariadb(PGresult* res, int row) {
    MariaDbSource src;
    const char* conn_id_val = PQgetvalue(res, row, 0);
    src.conn_id = conn_id_val ? conn_id_val : "";
    const std::string raw_host = PQgetvalue(res, row, 2) ? PQgetvalue(res, row, 2) : "localhost";
    src.password = PQgetvalue(res, row, 6) ? PQgetvalue(res, row, 6) : "";
    src.host = normalize_mariadb_host(raw_host, !src.password.empty());
    src.port = parse_port(PQgetvalue(res, row, 3), 3306);
    src.db_name = PQgetvalue(res, row, 4) ? PQgetvalue(res, row, 4) : "";
    src.user = PQgetvalue(res, row, 5) ? PQgetvalue(res, row, 5) : "";
    return src;
}

MssqlSource row_to_mssql(PGresult* res, int row) {
    MssqlSource src;
    const char* conn_id_val = PQgetvalue(res, row, 0);
    src.conn_id = conn_id_val ? conn_id_val : "";
    src.host = normalize_tcp_host(PQgetvalue(res, row, 2) ? PQgetvalue(res, row, 2) : "localhost");
    src.port = parse_port(PQgetvalue(res, row, 3), 1433);
    src.db_name = PQgetvalue(res, row, 4) ? PQgetvalue(res, row, 4) : "";
    src.user = PQgetvalue(res, row, 5) ? PQgetvalue(res, row, 5) : "";
    src.password = PQgetvalue(res, row, 6) ? PQgetvalue(res, row, 6) : "";
    return src;
}

MongoSource row_to_mongo(PGresult* res, int row) {
    MongoSource src;
    const char* conn_id_val = PQgetvalue(res, row, 0);
    src.conn_id = conn_id_val ? conn_id_val : "";
    src.host = normalize_tcp_host(PQgetvalue(res, row, 2) ? PQgetvalue(res, row, 2) : "localhost");
    src.port = parse_port(PQgetvalue(res, row, 3), 27017);
    src.db_name = PQgetvalue(res, row, 4) ? PQgetvalue(res, row, 4) : "";
    src.user = PQgetvalue(res, row, 5) ? PQgetvalue(res, row, 5) : "";
    src.password = PQgetvalue(res, row, 6) ? PQgetvalue(res, row, 6) : "";
    const auto extras = nlohmann::json::parse(extras_string(res, row), nullptr, false);
    if (extras.is_object()) {
        if (const auto it = extras.find("replica_set"); it != extras.end() && it->is_string()) {
            src.replica_set = it->get<std::string>();
            src.replica_set_in_extras = true;
        }
    }
    return src;
}

}  // namespace

std::mutex& app_config_mutex() {
    return connections_mutex_instance();
}

int reload_connections_nolock(PGconn* pg, AppConfig& cfg) {
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        return -1;
    }

    static const char* kSql = R"(
        SELECT alias, db_engine::text, host, port::text, db_name, username, password, extras::text
        FROM cdc_catalog.connections
        WHERE active = true
        ORDER BY alias
    )";

    PGresult* res = PQexec(pg, kSql);
    if (!res) {
        log_write(pg, {
            .level = LogLevel::Warning,
            .component = "catalog",
            .message = "reload_connections query failed",
            .context = {{"error", "PQexec returned null"}},
        });
        return -1;
    }
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        const char* err = PQresultErrorMessage(res);
        log_write(pg, {
            .level = LogLevel::Warning,
            .component = "catalog",
            .message = "reload_connections query failed",
            .context = {{"sqlstate", PQresultErrorField(res, PG_DIAG_SQLSTATE) ? PQresultErrorField(res, PG_DIAG_SQLSTATE) : ""},
                        {"error", err ? err : ""}},
        });
        PQclear(res);
        return -1;
    }

    const int rows = PQntuples(res);
    if (rows == 0) {
        PQclear(res);
        cfg.mariadb_sources.clear();
        cfg.mssql_sources.clear();
        cfg.mongo_sources.clear();
        return 0;
    }

    std::vector<MariaDbSource> mariadb;
    std::vector<MssqlSource> mssql;
    std::vector<MongoSource> mongo;
    mariadb.reserve(static_cast<std::size_t>(rows));
    mssql.reserve(static_cast<std::size_t>(rows));
    mongo.reserve(static_cast<std::size_t>(rows));

    for (int i = 0; i < rows; ++i) {
        const char* engine = PQgetvalue(res, i, 1);
        if (!engine) {
            continue;
        }
        const std::string eng = engine;
        if (eng == "mariadb") {
            mariadb.push_back(row_to_mariadb(res, i));
        } else if (eng == "mssql") {
            mssql.push_back(row_to_mssql(res, i));
        } else if (eng == "mongodb") {
            mongo.push_back(row_to_mongo(res, i));
        }
    }
    PQclear(res);

    cfg.mariadb_sources = std::move(mariadb);
    cfg.mssql_sources = std::move(mssql);
    cfg.mongo_sources = std::move(mongo);
    return static_cast<int>(cfg.mariadb_sources.size() + cfg.mssql_sources.size() + cfg.mongo_sources.size());
}

int reload_connections(PGconn* pg, AppConfig& cfg) {
    std::lock_guard<std::mutex> lock(app_config_mutex());
    return reload_connections_nolock(pg, cfg);
}
