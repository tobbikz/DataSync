#include "connection_test.hpp"

#include "mariadb_conn.hpp"
#include "mariadb_preflight.hpp"
#include "obs_log.hpp"

#ifdef HAVE_FREETDS
#include "mssql_conn.hpp"
#include "mssql_preflight.hpp"
#endif

#ifdef HAVE_MONGOC
#include "mongo_conn.hpp"
#include "mongo_preflight.hpp"
#endif

#include <chrono>
#include <iostream>
#include <map>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

using nlohmann::json;

namespace {

constexpr const char* kComponent = "connection_test";

struct ConnectionReport {
    std::string conn_id;
    std::string engine;
    std::string target;
    bool reachable{false};
    bool ok{false};
    std::vector<std::string> errors;
    std::vector<std::string> warnings;
    std::map<std::string, std::string> facts;
    long long duration_ms{0};
};

/** Fold any of the three engine preflight results into the shared report. */
template <typename PreflightResult>
void absorb(ConnectionReport& report, const PreflightResult& part) {
    if (!part.ok) {
        report.ok = false;
    }
    report.errors.insert(report.errors.end(), part.errors.begin(), part.errors.end());
    report.warnings.insert(report.warnings.end(), part.warnings.begin(), part.warnings.end());
    for (const auto& [key, value] : part.facts) {
        report.facts[key] = value;
    }
}

std::string target_label(const std::string& host, std::uint16_t port, const std::string& db) {
    std::string label = host + ":" + std::to_string(port);
    if (!db.empty()) {
        label += "/" + db;
    }
    return label;
}

class Stopwatch {
  public:
    long long elapsed_ms() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - started_)
            .count();
    }

  private:
    std::chrono::steady_clock::time_point started_{std::chrono::steady_clock::now()};
};

ConnectionReport test_mariadb(const MariaDbSource& src) {
    ConnectionReport report;
    report.conn_id = src.conn_id;
    report.engine = "mariadb";
    report.target = target_label(src.host, src.port, src.db_name);
    const Stopwatch watch;
    try {
        MariaDbConn conn(src);
        report.reachable = true;
        report.ok = true;
        absorb(report, check_mariadb_load_ready(conn.handle));
        absorb(report, check_mariadb_cdc_ready(conn.handle));
    } catch (const std::exception& ex) {
        report.errors.push_back(std::string("connect failed: ") + ex.what());
    }
    report.duration_ms = watch.elapsed_ms();
    return report;
}

#ifdef HAVE_FREETDS
ConnectionReport test_mssql(const MssqlSource& src, PGconn* catalog_pg) {
    ConnectionReport report;
    report.conn_id = src.conn_id;
    report.engine = "mssql";
    report.target = target_label(src.host, src.port, src.db_name);
    const Stopwatch watch;
    try {
        MssqlConn conn(src);
        report.reachable = true;
        report.ok = true;
        absorb(report, check_mssql_load_ready(conn, src.db_name));
        absorb(report, check_mssql_cdc_ready(conn, src.db_name));
        absorb(report, check_mssql_agent_ready(conn, src.db_name));
        absorb(report, check_mssql_catalog_capture_instances(catalog_pg, src.conn_id));
    } catch (const std::exception& ex) {
        report.errors.push_back(std::string("connect failed: ") + ex.what());
    }
    report.duration_ms = watch.elapsed_ms();
    return report;
}
#endif

#ifdef HAVE_MONGOC
ConnectionReport test_mongo(const MongoSource& src) {
    ConnectionReport report;
    report.conn_id = src.conn_id;
    report.engine = "mongodb";
    report.target = target_label(src.host, src.port, src.db_name);
    const Stopwatch watch;
    try {
        MongoConn conn(src);
        report.reachable = true;
        report.ok = true;
        absorb(report, check_mongo_cdc_ready(conn, src));
    } catch (const std::exception& ex) {
        report.errors.push_back(std::string("connect failed: ") + ex.what());
    }
    report.duration_ms = watch.elapsed_ms();
    return report;
}
#endif

/** Engine present in connections but missing from this build. */
ConnectionReport unsupported_engine(
    const std::string& conn_id,
    const std::string& engine,
    const std::string& target,
    const std::string& missing_dependency) {
    ConnectionReport report;
    report.conn_id = conn_id;
    report.engine = engine;
    report.target = target;
    report.errors.push_back(
        "binary was built without " + missing_dependency + " support: cannot test this connection");
    return report;
}

json to_json(const ConnectionReport& report) {
    json out;
    out["conn_id"] = report.conn_id;
    out["engine"] = report.engine;
    out["target"] = report.target;
    out["reachable"] = report.reachable;
    out["ok"] = report.ok;
    out["duration_ms"] = report.duration_ms;
    out["errors"] = report.errors;
    out["warnings"] = report.warnings;
    out["facts"] = report.facts;
    return out;
}

void log_report(PGconn* log_pg, const std::string& batch_id, const ConnectionReport& report) {
    json context;
    context["engine"] = report.engine;
    context["target"] = report.target;
    context["reachable"] = report.reachable;
    context["duration_ms"] = report.duration_ms;
    context["facts"] = report.facts;
    if (!report.errors.empty()) {
        context["errors"] = report.errors;
    }
    if (!report.warnings.empty()) {
        context["warnings"] = report.warnings;
    }

    LogLevel level = LogLevel::Info;
    std::string message = "connection test passed";
    if (!report.ok) {
        level = LogLevel::Error;
        message = "connection test failed";
    } else if (!report.warnings.empty()) {
        level = LogLevel::Warning;
        message = "connection test passed with warnings";
    }

    log_write(log_pg, {
        .level = level,
        .component = kComponent,
        .message = message,
        .batch_id = batch_id,
        .conn_id = report.conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = context,
    });
}

bool selected(const std::optional<std::string>& filter, const std::string& conn_id) {
    return !filter || *filter == conn_id;
}

}  // namespace

int run_test_connection_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& conn_id_filter) {
    std::vector<ConnectionReport> reports;

    for (const auto& src : cfg.mariadb_sources) {
        if (selected(conn_id_filter, src.conn_id)) {
            reports.push_back(test_mariadb(src));
        }
    }
    for (const auto& src : cfg.mssql_sources) {
        if (!selected(conn_id_filter, src.conn_id)) {
            continue;
        }
#ifdef HAVE_FREETDS
        reports.push_back(test_mssql(src, log_pg));
#else
        reports.push_back(unsupported_engine(
            src.conn_id, "mssql", target_label(src.host, src.port, src.db_name), "FreeTDS"));
#endif
    }
    for (const auto& src : cfg.mongo_sources) {
        if (!selected(conn_id_filter, src.conn_id)) {
            continue;
        }
#ifdef HAVE_MONGOC
        reports.push_back(test_mongo(src));
#else
        reports.push_back(unsupported_engine(
            src.conn_id, "mongodb", target_label(src.host, src.port, src.db_name), "libmongoc"));
#endif
    }

    if (reports.empty()) {
        const std::string reason = conn_id_filter
            ? "unknown conn_id: " + *conn_id_filter
            : "no active connections in cdc_catalog.connections";
        std::cerr << reason << '\n';
        log_write(log_pg, {
            .level = LogLevel::Warning,
            .component = kComponent,
            .message = "connection test found nothing to check",
            .batch_id = batch_id,
            .conn_id = conn_id_filter,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"reason", reason}},
        });
        return 2;
    }

    json out;
    out["batch_id"] = batch_id;
    out["connections"] = json::array();
    int failed = 0;
    int warned = 0;
    for (const auto& report : reports) {
        log_report(log_pg, batch_id, report);
        out["connections"].push_back(to_json(report));
        if (!report.ok) {
            failed += 1;
        }
        warned += static_cast<int>(report.warnings.size());
    }
    out["checked"] = static_cast<int>(reports.size());
    out["failed"] = failed;
    out["warnings"] = warned;
    std::cout << out.dump(2) << '\n';

    log_write(log_pg, {
        .level = failed > 0 ? LogLevel::Error : LogLevel::Info,
        .component = kComponent,
        .message = failed > 0 ? "connection test completed with failures" : "connection test completed",
        .batch_id = batch_id,
        .conn_id = conn_id_filter,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"checked", reports.size()}, {"failed", failed}, {"warnings", warned}},
    });

    return failed > 0 ? 1 : 0;
}
