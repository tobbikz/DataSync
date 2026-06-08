#include "connections.hpp"

#include <nlohmann/json.hpp>

#include <cstdlib>

namespace {

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

MariaDbSource row_to_mariadb(PGresult* res, int row) {
    MariaDbSource src;
    src.conn_id = PQgetvalue(res, row, 0);
    src.host = PQgetvalue(res, row, 2) ? PQgetvalue(res, row, 2) : "localhost";
    src.port = parse_port(PQgetvalue(res, row, 3), 3306);
    src.db_name = PQgetvalue(res, row, 4) ? PQgetvalue(res, row, 4) : "";
    src.user = PQgetvalue(res, row, 5) ? PQgetvalue(res, row, 5) : "";
    src.password = PQgetvalue(res, row, 6) ? PQgetvalue(res, row, 6) : "";
    return src;
}

MssqlSource row_to_mssql(PGresult* res, int row) {
    MssqlSource src;
    src.conn_id = PQgetvalue(res, row, 0);
    src.host = PQgetvalue(res, row, 2) ? PQgetvalue(res, row, 2) : "localhost";
    src.port = parse_port(PQgetvalue(res, row, 3), 1433);
    src.db_name = PQgetvalue(res, row, 4) ? PQgetvalue(res, row, 4) : "";
    src.user = PQgetvalue(res, row, 5) ? PQgetvalue(res, row, 5) : "";
    src.password = PQgetvalue(res, row, 6) ? PQgetvalue(res, row, 6) : "";
    return src;
}

MongoSource row_to_mongo(PGresult* res, int row) {
    MongoSource src;
    src.conn_id = PQgetvalue(res, row, 0);
    src.host = PQgetvalue(res, row, 2) ? PQgetvalue(res, row, 2) : "localhost";
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

int reload_connections(PGconn* pg, AppConfig& cfg) {
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        return 0;
    }

    static const char* kSql = R"(
        SELECT alias, db_engine::text, host, port::text, db_name, username, password, extras::text
        FROM cdc_catalog.connections
        WHERE active = true
        ORDER BY alias
    )";

    PGresult* res = PQexec(pg, kSql);
    if (!res) {
        return 0;
    }
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return 0;
    }

    const int rows = PQntuples(res);
    if (rows == 0) {
        PQclear(res);
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
    return rows;
}
