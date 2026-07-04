#include "cdc_reconcile.hpp"

#include "reconcile_enrichments.hpp"
#include "schema_migrate.hpp"
#include "capture_common.hpp"
#include "config.hpp"
#include "kafka_topics.hpp"
#ifdef HAVE_RDKAFKA
#include "kafka_lag.hpp"
#include "kafka_table_lag.hpp"
#endif
#include "mariadb_conn.hpp"
#include "mariadb_schema.hpp"
#include "mongo_lake.hpp"
#include "mssql_lake.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"
#include "pipeline_defaults.hpp"

#ifdef HAVE_FREETDS
#include "mssql_conn.hpp"
#endif
#ifdef HAVE_MONGOC
#include "mongo_conn.hpp"
#endif

#include <openssl/evp.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <functional>
#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

std::atomic<bool> g_reconcile_shutdown{false};

void on_reconcile_signal(int) {
    g_reconcile_shutdown.store(true);
}

struct ReconcileRuntime {
    int row_abs_tolerance{50};
    double row_pct_tolerance{0.01};
    int row_warn_abs_tolerance{10};
    double row_warn_pct_tolerance{0.005};
    long long large_table_min_rows{100000};
    long long large_table_abs_cap{5000};
    int apply_lag_warn_seconds{600};
    int apply_lag_fail_seconds{1800};
    int pk_sample_size{100};
    int max_tables{0};
    bool enabled{true};
    int interval_hours{4};
    long long kafka_lag_warn{100};
    long long kafka_lag_fail{10000};
    int capture_lag_warn_seconds{300};
    int capture_lag_fail_seconds{900};
    int apply_inactive_seconds{3600};
};

ReconcileRuntime load_reconcile_runtime(RuntimeConfig& runtime, PGconn* pg, const std::string& conn_id) {
    runtime.reload(pg);
    ReconcileRuntime cfg;
    cfg.row_abs_tolerance = pipeline_defaults::kReconcileRowAbsTolerance;
    cfg.row_pct_tolerance = pipeline_defaults::kReconcileRowPctTolerance;
    cfg.row_warn_abs_tolerance = pipeline_defaults::kReconcileRowWarnAbsTolerance;
    cfg.row_warn_pct_tolerance = pipeline_defaults::kReconcileRowWarnPctTolerance;
    cfg.large_table_min_rows = pipeline_defaults::kReconcileLargeTableMinRows;
    cfg.large_table_abs_cap = pipeline_defaults::kReconcileLargeTableAbsCap;
    cfg.apply_lag_warn_seconds = pipeline_defaults::kReconcileApplyLagWarnSeconds;
    cfg.apply_lag_fail_seconds = pipeline_defaults::kReconcileApplyLagFailSeconds;
    cfg.pk_sample_size = pipeline_defaults::kReconcilePkSampleSize;
    cfg.max_tables = pipeline_defaults::kReconcileMaxTables;
    cfg.enabled = pipeline_defaults::kReconcileEnabled;
    cfg.interval_hours = runtime.get_int(
        "reconcile_interval_hours",
        pipeline_defaults::kReconcileIntervalHoursDefault,
        "cdc_kafka_reconcile",
        conn_id);
    cfg.kafka_lag_warn = pipeline_defaults::kReconcileKafkaLagWarn;
    cfg.kafka_lag_fail = pipeline_defaults::kReconcileKafkaLagFail;
    cfg.capture_lag_warn_seconds = pipeline_defaults::kReconcileCaptureLagWarnSeconds;
    cfg.capture_lag_fail_seconds = pipeline_defaults::kReconcileCaptureLagFailSeconds;
    cfg.apply_inactive_seconds = pipeline_defaults::kApplyInactiveSeconds;
    return cfg;
}

std::optional<std::string> fetch_max_ts_lake(
    PGconn* pg,
    const CatalogReconcileRow& row,
    const std::string& column) {
    const std::string fq =
        pg_ident(row.lake_schema) + "." + pg_ident(row.lake_table);
    const std::string sql = "SELECT MAX(" + pg_ident(column) + ")::text FROM " + fq;
    PGresult* res = PQexec(pg, sql.c_str());
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return std::nullopt;
    }
    if (PQgetisnull(res, 0, 0)) {
        PQclear(res);
        return std::nullopt;
    }
    const char* val = PQgetvalue(res, 0, 0);
    std::string out = val ? val : "";
    PQclear(res);
    return out.empty() ? std::nullopt : std::optional<std::string>(out);
}

std::optional<std::string> fetch_max_ts_mariadb(
    MYSQL* mysql,
    const CatalogReconcileRow& row,
    const std::string& column) {
    auto esc_id = [](const std::string& id) {
        std::string out;
        for (char c : id) {
            out += (c == '`') ? "``" : std::string(1, c);
        }
        return out;
    };
    std::ostringstream sql;
    sql << "SELECT MAX(`" << esc_id(column) << "`) FROM `" << esc_id(row.source_schema) << "`.`"
        << esc_id(row.source_table) << "`";
    if (mysql_query(mysql, sql.str().c_str()) != 0) {
        return std::nullopt;
    }
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return std::nullopt;
    }
    MYSQL_ROW r = mysql_fetch_row(res);
    std::optional<std::string> out;
    if (r && r[0]) {
        out = r[0];
    }
    mysql_free_result(res);
    return out;
}

int mariadb_column_count(MYSQL* mysql, const std::string& schema, const std::string& table) {
    auto esc_id = [](const std::string& id) {
        std::string out;
        for (char c : id) {
            out += (c == '`') ? "``" : std::string(1, c);
        }
        return out;
    };
    std::ostringstream sql;
    sql << "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='" << esc_id(schema)
        << "' AND table_name='" << esc_id(table) << "'";
    if (mysql_query(mysql, sql.str().c_str()) != 0) {
        return -1;
    }
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return -1;
    }
    MYSQL_ROW r = mysql_fetch_row(res);
    const int count = r && r[0] ? std::atoi(r[0]) : -1;
    mysql_free_result(res);
    return count;
}

std::optional<std::string> reconcile_catalog_skip_reason(const CatalogReconcileRow& row) {
    if (!row.active) {
        return "inactive";
    }
    if (!row.cdc_enabled) {
        return "cdc_disabled";
    }
    if (row.needs_full_load) {
        return "needs_full_load";
    }
    if (!row.has_pk) {
        return "no_pk";
    }
    if (row.catalog_status == "skipped") {
        return "catalog_skipped";
    }
    if (row.catalog_status == "disabled") {
        return "catalog_disabled";
    }
    return std::nullopt;
}

void populate_lake_keys(CatalogReconcileRow& row) {
    if (row.db_engine == "mongodb") {
        row.source_schema = mongo_catalog_source_schema(row.source_database, row.source_schema);
    }
    if (row.db_engine == "mssql") {
        row.lake_schema = mssql_pg_schema_name(row.source_database, row.source_schema);
        row.lake_table = mssql_pg_table_name(row.source_table);
    } else if (row.db_engine == "mongodb") {
        row.lake_schema = mongo_pg_schema_name(row.source_database);
        row.lake_table = mongo_pg_table_name(row.source_table);
    } else {
        row.lake_schema = row.source_schema;
        row.lake_table = row.source_table;
    }
}

/** Advisory lock namespace — avoids collision with full-load locks keyed by catalog_id. */
constexpr long long kReconcileAdvisoryLockPrefix = 0x5243524E00000000LL;

long long reconcile_conn_advisory_key(const std::string& conn_id) {
    const std::hash<std::string> hasher;
    const unsigned long long mix = hasher(std::string("reconcile:") + conn_id);
    return kReconcileAdvisoryLockPrefix
           | static_cast<long long>(mix & 0x0000FFFFFFFFFFFFULL);
}

bool try_acquire_reconcile_conn_lock(PGconn* pg, long long key) {
    const std::string key_str = std::to_string(key);
    const char* vals[] = {key_str.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT pg_try_advisory_lock($1::bigint)",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return false;
    }
    const bool acquired = PQgetvalue(res, 0, 0)[0] == 't';
    PQclear(res);
    return acquired;
}

/** Block until reconcile advisory lock is acquired (retry every 5s). */
bool acquire_reconcile_conn_lock(
    PGconn* pg,
    const std::string& conn_id,
    long long* key_out,
    std::atomic<bool>* shutdown) {
    const long long key = reconcile_conn_advisory_key(conn_id);
    while (true) {
        if (shutdown && shutdown->load()) {
            return false;
        }
        if (try_acquire_reconcile_conn_lock(pg, key)) {
            if (key_out) {
                *key_out = key;
            }
            return true;
        }
        for (int i = 0; i < 5; ++i) {
            if (shutdown && shutdown->load()) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

/** Block until full reconcile-loop advisory lock is acquired. */
bool acquire_reconcile_loop_lock(PGconn* pg, long long* key_out, std::atomic<bool>* shutdown) {
    const long long key = pipeline_defaults::kReconcileLoopAdvisoryLockKey;
    while (true) {
        if (shutdown && shutdown->load()) {
            return false;
        }
        if (try_acquire_reconcile_conn_lock(pg, key)) {
            if (key_out) {
                *key_out = key;
            }
            return true;
        }
        for (int i = 0; i < 5; ++i) {
            if (shutdown && shutdown->load()) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

void release_reconcile_conn_lock(PGconn* pg, long long key) {
    const std::string key_str = std::to_string(key);
    const char* vals[] = {key_str.c_str()};
    pg_exec_params_simple(pg, "SELECT pg_advisory_unlock($1::bigint)", 1, vals);
}

struct ReconcileConnLockGuard {
    PGconn* pg{nullptr};
    long long key{0};
    bool held{false};

    ReconcileConnLockGuard(PGconn* pg_in, long long key_in) : pg(pg_in), key(key_in), held(true) {}

    ~ReconcileConnLockGuard() {
        if (held && pg) {
            release_reconcile_conn_lock(pg, key);
        }
    }

    ReconcileConnLockGuard(const ReconcileConnLockGuard&) = delete;
    ReconcileConnLockGuard& operator=(const ReconcileConnLockGuard&) = delete;
};

struct ResumableCycleRun {
    long long run_id{0};
    std::string batch_id;
    std::string reconcile_mode;
    nlohmann::json completed_conns{nlohmann::json::array()};
};

/** Fail stale cycle runs (keep results). Remove legacy per-conn running rows only. */
void cleanup_reconcile_runs_at_cycle_start(PGconn* pg) {
    const std::string stale_hours =
        std::to_string(pipeline_defaults::kReconcileCycleStaleAgeHours);
    const char* stale_vals[] = {
        pipeline_defaults::kReconcileCycleConnId,
        stale_hours.c_str()};
    PGresult* stale_res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.reconciliation_run
        SET status = 'fail',
            finished_at = COALESCE(finished_at, now()),
            context = COALESCE(context, '{}'::jsonb)
                    || jsonb_build_object('aborted_stale', true, 'scope', 'cycle')
        WHERE conn_id = $1
          AND status = 'running'
          AND started_at < now() - ($2::int || ' hours')::interval
        )",
        2,
        nullptr,
        stale_vals,
        nullptr,
        nullptr,
        0);
    if (stale_res) {
        PQclear(stale_res);
    }

    const char* legacy_vals[] = {pipeline_defaults::kReconcileCycleConnId};
    PGresult* legacy_res = PQexecParams(
        pg,
        R"(
        DELETE FROM cdc_catalog.reconciliation_run
        WHERE status = 'running'
          AND conn_id <> $1
        )",
        1,
        nullptr,
        legacy_vals,
        nullptr,
        nullptr,
        0);
    if (legacy_res) {
        PQclear(legacy_res);
    }
}

/** Return in-progress cycle run to resume after daemon restart (never DELETE running *). */
std::optional<ResumableCycleRun> find_resumable_cycle_run(PGconn* pg) {
    const char* vals[] = {pipeline_defaults::kReconcileCycleConnId};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT run_id, batch_id, COALESCE(reconcile_mode, 'full') AS reconcile_mode,
               COALESCE(context, '{}'::jsonb) AS context
        FROM cdc_catalog.reconciliation_run
        WHERE conn_id = $1
          AND status = 'running'
        ORDER BY run_id DESC
        LIMIT 1
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        if (res) {
            PQclear(res);
        }
        return std::nullopt;
    }

    ResumableCycleRun out;
    out.run_id = std::atoll(PQgetvalue(res, 0, 0));
    const char* batch = PQgetvalue(res, 0, 1);
    out.batch_id = batch ? batch : "";
    const char* mode = PQgetvalue(res, 0, 2);
    out.reconcile_mode = mode && mode[0] ? mode : pipeline_defaults::kReconcileModeLake;
    if (out.reconcile_mode == pipeline_defaults::kReconcileModeFull) {
        out.reconcile_mode = pipeline_defaults::kReconcileModeLake;
    }
    try {
        const char* ctx = PQgetvalue(res, 0, 3);
        const auto context = nlohmann::json::parse(ctx ? ctx : "{}");
        if (context.contains("completed_conns") && context["completed_conns"].is_array()) {
            out.completed_conns = context["completed_conns"];
        }
    } catch (...) {
    }
    PQclear(res);
    if (out.run_id <= 0 || out.batch_id.empty()) {
        return std::nullopt;
    }
    return out;
}

bool completed_conns_contains(const nlohmann::json& completed_conns, const std::string& conn_id) {
    if (!completed_conns.is_array()) {
        return false;
    }
    for (const auto& entry : completed_conns) {
        if (entry.is_string() && entry.get<std::string>() == conn_id) {
            return true;
        }
    }
    return false;
}

struct CycleStatusDecision {
    std::string status{"ok"};
    std::string strict_status{"ok"};
    bool sla_met{false};
    double ok_rate{0.0};
    int actionable_tables{0};
};

/** Cycle status with optional SLA pass (ok_rate on actionable = checked - skip). */
CycleStatusDecision eval_cycle_run_status(
    bool cycle_interrupted,
    int cycle_exit,
    int tables_checked,
    int tables_ok,
    int tables_warn,
    int tables_fail,
    int tables_skip) {
    CycleStatusDecision out;
    out.actionable_tables = std::max(0, tables_checked - tables_skip);
    if (out.actionable_tables > 0) {
        out.ok_rate = static_cast<double>(tables_ok) / static_cast<double>(out.actionable_tables);
    } else if (tables_checked > 0) {
        out.actionable_tables = tables_checked;
        out.ok_rate = static_cast<double>(tables_ok) / static_cast<double>(tables_checked);
    }

    if (cycle_interrupted || cycle_exit != 0) {
        out.strict_status = "fail";
        out.status = "fail";
        return out;
    }

    if (tables_fail > 0) {
        out.strict_status = "fail";
    } else if (tables_warn > 0) {
        out.strict_status = "warn";
    } else {
        out.strict_status = "ok";
    }

    const double pass = pipeline_defaults::kReconcileCycleOkRatePass;
    out.sla_met = out.ok_rate >= pass;

    if (tables_fail == 0 && tables_warn == 0) {
        out.status = "ok";
        out.sla_met = true;
        return out;
    }

    if (tables_fail == 0 && tables_warn > 0) {
        out.status = "warn";
        return out;
    }

    // Residual FAIL rows: pass cycle when OK rate meets SLA (e.g. >= 97% actionable).
    if (out.sla_met) {
        out.status = "ok";
    } else {
        out.status = "fail";
    }
    return out;
}

/** Drop orphan runs for conn_id (legacy CLI). Caller must hold the reconcile advisory lock. */
void abort_orphan_reconcile_runs(PGconn* pg, const std::string& conn_id) {
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        DELETE FROM cdc_catalog.reconciliation_run
        WHERE conn_id = $1
          AND status = 'running'
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

void abort_stale_reconcile_runs(PGconn* pg, const std::string& conn_id) {
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.reconciliation_run
        SET status = 'fail',
            finished_at = COALESCE(finished_at, now()),
            context = COALESCE(context, '{}'::jsonb)
                    || jsonb_build_object('aborted_stale', true)
        WHERE conn_id = $1
          AND status = 'running'
          AND started_at < now() - interval '2 hours'
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

void refresh_reconcile_run_progress(
    PGconn* pg,
    long long run_id,
    const nlohmann::json& patch_context) {
    const std::string run_id_str = std::to_string(run_id);
    const std::string patch_json = patch_context.dump();
    const char* vals[] = {patch_json.c_str(), run_id_str.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        WITH stats AS (
            SELECT
                COUNT(*)::int AS checked,
                COUNT(*) FILTER (WHERE status = 'ok')::int AS ok,
                COUNT(*) FILTER (WHERE status = 'warn')::int AS warn,
                COUNT(*) FILTER (WHERE status = 'fail')::int AS fail,
                COUNT(*) FILTER (WHERE status = 'skip')::int AS skip
            FROM cdc_catalog.reconciliation_result
            WHERE run_id = $2::bigint
        )
        UPDATE cdc_catalog.reconciliation_run run
        SET tables_checked = stats.checked,
            tables_ok = stats.ok,
            tables_warn = stats.warn,
            tables_fail = stats.fail,
            context = (
                CASE
                    WHEN jsonb_typeof(COALESCE(run.context, '{}'::jsonb)) = 'object'
                    THEN COALESCE(run.context, '{}'::jsonb)
                    ELSE '{}'::jsonb
                END
            ) || $1::jsonb || jsonb_build_object('tables_skip', stats.skip)
        FROM stats
        WHERE run.run_id = $2::bigint
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

int count_catalog_tables(PGconn* pg, const std::optional<std::string>& conn_id) {
    if (conn_id && !conn_id->empty()) {
        const char* vals[] = {conn_id->c_str()};
        PGresult* res = PQexecParams(
            pg,
            "SELECT COUNT(*)::int FROM cdc_catalog.catalog WHERE conn_id = $1",
            1,
            nullptr,
            vals,
            nullptr,
            nullptr,
            0);
        int count = 0;
        if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
            count = std::atoi(PQgetvalue(res, 0, 0));
        }
        if (res) {
            PQclear(res);
        }
        return count;
    }
    PGresult* res = PQexec(pg, "SELECT COUNT(*)::int FROM cdc_catalog.catalog");
    int count = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        count = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return count;
}

int count_total_stale_tables(PGconn* pg) {
    PGresult* res = PQexec(
        pg,
        R"(
        SELECT COUNT(*)::int
        FROM cdc_catalog.v_apply_stale v
        JOIN cdc_catalog.catalog c ON c.catalog_id = v.catalog_id
        )");
    int count = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        count = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return count;
}

std::string mssql_brack(const std::string& name) {
    std::string out = "[";
    for (char c : name) {
        out += (c == ']') ? "]]" : std::string(1, c);
    }
    out += "]";
    return out;
}

std::string md5_hex(const std::string& payload) {
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_len = 0;
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        return {};
    }
    if (EVP_DigestInit_ex(ctx, EVP_md5(), nullptr) != 1 ||
        EVP_DigestUpdate(ctx, payload.data(), payload.size()) != 1 ||
        EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1) {
        EVP_MD_CTX_free(ctx);
        return {};
    }
    EVP_MD_CTX_free(ctx);
    std::ostringstream oss;
    for (unsigned int i = 0; i < digest_len; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
    }
    return oss.str();
}

std::string status_rank(const std::string& a, const std::string& b) {
    auto rank = [](const std::string& s) {
        if (s == "fail") {
            return 3;
        }
        if (s == "warn") {
            return 2;
        }
        if (s == "ok") {
            return 1;
        }
        return 0;
    };
    return rank(a) >= rank(b) ? a : b;
}

std::string eval_row_count_status(long long source_rows, long long lake_rows, const ReconcileRuntime& cfg) {
    if (source_rows < 0 || lake_rows < 0) {
        return "skip";
    }
    const long long delta = source_rows - lake_rows;
    const long long abs_delta = std::llabs(delta);
    const double pct =
        source_rows > 0 ? static_cast<double>(abs_delta) / static_cast<double>(source_rows)
                        : (abs_delta > 0 ? 1.0 : 0.0);

    if (source_rows >= cfg.large_table_min_rows) {
        if (pct > cfg.row_pct_tolerance && abs_delta > cfg.large_table_abs_cap) {
            return "fail";
        }
        if (pct > cfg.row_warn_pct_tolerance && abs_delta > cfg.row_warn_abs_tolerance) {
            return "warn";
        }
        return "ok";
    }

    if (abs_delta > cfg.row_abs_tolerance || pct > cfg.row_pct_tolerance) {
        return "fail";
    }
    if (abs_delta > cfg.row_warn_abs_tolerance || pct > cfg.row_warn_pct_tolerance) {
        return "warn";
    }
    return "ok";
}

std::string classify_row_drift(long long source_rows, long long lake_rows) {
    if (source_rows < 0 || lake_rows < 0) {
        return "unknown";
    }
    const long long delta = source_rows - lake_rows;
    if (delta == 0) {
        return "none";
    }
    if (delta > 0) {
        return "source_ahead";
    }
    return "append_zombie";
}

std::string eval_apply_lag_status(int apply_lag_seconds, const ReconcileRuntime& cfg) {
    if (apply_lag_seconds < 0) {
        return "skip";
    }
    if (apply_lag_seconds > cfg.apply_lag_fail_seconds) {
        return "fail";
    }
    if (apply_lag_seconds > cfg.apply_lag_warn_seconds) {
        return "warn";
    }
    return "ok";
}

std::string eval_kafka_lag_status(long long kafka_lag, const ReconcileRuntime& cfg) {
    if (kafka_lag < 0) {
        return "skip";
    }
    if (kafka_lag > cfg.kafka_lag_fail) {
        return "fail";
    }
    if (kafka_lag > cfg.kafka_lag_warn) {
        return "warn";
    }
    return "ok";
}

// Bucketed topics shared partition watermarks; apply_batch_stats now stores exact table lag.
// Never let kafka alone drive overall=fail/warn when row counts did not fail.
std::string cap_kafka_for_overall(const std::string& row_status, const std::string& kafka_status) {
    if (kafka_status == "skip") {
        return "skip";
    }
    if (row_status == "skip") {
        return "skip";
    }
    if (row_status != "fail") {
        if (kafka_status == "fail" || kafka_status == "warn") {
            return "ok";
        }
    }
    return kafka_status;
}

// PK sample compares first N rows by PK order; mismatch with matching counts is often type/format drift.
// Never let pk_checksum alone drive overall=fail when row counts did not fail.
std::string cap_pk_checksum_for_overall(const std::string& row_status, bool pk_match) {
    if (pk_match) {
        return "ok";
    }
    if (row_status == "skip" || row_status == "ok") {
        return "ok";
    }
    if (row_status == "warn") {
        return "warn";
    }
    return "fail";
}

// Per-table MSSQL LSN can look stale on quiet tables; never let capture alone drive overall
// warn/fail when row counts did not fail and apply is not failing (mirrors cap_kafka_for_overall).
std::string cap_capture_for_overall(
    const std::string& row_status,
    const std::string& apply_lag_status,
    const std::string& capture_status) {
    if (capture_status == "skip") {
        return "skip";
    }
    if (row_status == "skip") {
        return "skip";
    }
    if (row_status != "fail" && apply_lag_status != "fail") {
        if (capture_status == "fail" || capture_status == "warn") {
            return "ok";
        }
    }
    return capture_status;
}

std::string eval_capture_lag_status(int capture_lag_seconds, const ReconcileRuntime& cfg) {
    if (capture_lag_seconds < 0) {
        return "skip";
    }
    if (capture_lag_seconds > cfg.capture_lag_fail_seconds) {
        return "fail";
    }
    if (capture_lag_seconds > cfg.capture_lag_warn_seconds) {
        return "warn";
    }
    return "ok";
}

std::vector<CatalogReconcileRow> fetch_reconcile_catalog(
    PGconn* pg,
    const std::string& conn_id,
    int max_tables) {
    std::ostringstream sql;
    sql << R"(
        SELECT catalog_id, conn_id, db_engine::text, source_database, source_schema, source_table,
               pk_columns, status::text, active, cdc_enabled, needs_full_load, has_pk,
               hot, COALESCE(last_error, ''), COALESCE(engine_meta::text, '{}')
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
        ORDER BY source_schema, source_table
    )";
    if (max_tables > 0) {
        sql << " LIMIT " << max_tables;
    }

    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        sql.str().c_str(),
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    std::vector<CatalogReconcileRow> rows;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("reconcile catalog query failed");
    }

    for (int i = 0; i < PQntuples(res); ++i) {
        CatalogReconcileRow row;
        row.catalog_id = PQgetisnull(res, i, 0) ? 0 : std::atoll(PQgetvalue(res, i, 0));
        row.conn_id = PQgetvalue(res, i, 1) ? PQgetvalue(res, i, 1) : "";
        row.db_engine = PQgetvalue(res, i, 2) ? PQgetvalue(res, i, 2) : "";
        row.source_database = PQgetvalue(res, i, 3) ? PQgetvalue(res, i, 3) : "";
        row.source_schema = PQgetvalue(res, i, 4) ? PQgetvalue(res, i, 4) : "";
        row.source_table = PQgetvalue(res, i, 5) ? PQgetvalue(res, i, 5) : "";
        row.pk_columns = PQgetvalue(res, i, 6) ? PQgetvalue(res, i, 6) : "";
        row.catalog_status = PQgetvalue(res, i, 7) ? PQgetvalue(res, i, 7) : "";
        row.active = !PQgetisnull(res, i, 8) && PQgetvalue(res, i, 8)[0] == 't';
        row.cdc_enabled = !PQgetisnull(res, i, 9) && PQgetvalue(res, i, 9)[0] == 't';
        row.needs_full_load = !PQgetisnull(res, i, 10) && PQgetvalue(res, i, 10)[0] == 't';
        row.has_pk = !PQgetisnull(res, i, 11) && PQgetvalue(res, i, 11)[0] == 't';
        row.hot = !PQgetisnull(res, i, 12) && PQgetvalue(res, i, 12)[0] == 't';
        row.last_error = PQgetvalue(res, i, 13) ? PQgetvalue(res, i, 13) : "";
        row.engine_meta_json = PQgetvalue(res, i, 14) ? PQgetvalue(res, i, 14) : "{}";
        populate_lake_keys(row);
        rows.push_back(std::move(row));
    }
    PQclear(res);
    return rows;
}

long long pg_table_count(PGconn* pg, const std::string& schema, const std::string& table) {
    const char* vals[] = {schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT COUNT(*)::bigint
        FROM information_schema.tables t
        WHERE t.table_schema = $1 AND t.table_name = $2
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return -1;
    }
    const char* exists_str = PQgetvalue(res, 0, 0);
    const long long exists = exists_str ? std::atoll(exists_str) : -1;
    PQclear(res);
    if (exists <= 0) {
        return 0;
    }

    const std::string fq = pg_ident(schema) + "." + pg_ident(table);
    PGresult* cnt = PQexec(pg, ("SELECT COUNT(*)::bigint FROM " + fq).c_str());
    if (!cnt || PQresultStatus(cnt) != PGRES_TUPLES_OK || PQntuples(cnt) < 1) {
        if (cnt) {
            PQclear(cnt);
        }
        return -1;
    }
    const char* n_str = PQgetvalue(cnt, 0, 0);
    const long long n = n_str ? std::atoll(n_str) : -1;
    PQclear(cnt);
    return n;
}

long long mariadb_table_count(MYSQL* mysql, const std::string& schema, const std::string& table) {
    std::ostringstream sql;
    auto esc_id = [](const std::string& id) {
        std::string out;
        for (char c : id) {
            out += (c == '`') ? "``" : std::string(1, c);
        }
        return out;
    };
    sql << "SELECT COUNT(*) FROM `" << esc_id(schema) << "`.`" << esc_id(table) << "`";
    if (mysql_query(mysql, sql.str().c_str()) != 0) {
        return -1;
    }
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return -1;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    long long count = row && row[0] ? std::atoll(row[0]) : -1;
    mysql_free_result(res);
    return count;
}

#ifdef HAVE_FREETDS
long long mssql_table_count(MssqlConn& mssql, const std::string& database, const std::string& schema, const std::string& table) {
    mssql.use_database(database);
    const auto result = mssql.query(
        "SELECT COUNT_BIG(*) FROM " + mssql_brack(schema) + "." + mssql_brack(table));
    if (result.rows.empty() || result.rows[0].empty()) {
        return -1;
    }
    return result.rows[0][0].text.empty() ? -1 : std::atoll(result.rows[0][0].text.c_str());
}
#endif

#ifdef HAVE_MONGOC
long long mongo_collection_count(MongoConn& mongo, const std::string& database, const std::string& collection) {
    bson_t reply;
    bson_error_t error;
    bson_init(&reply);
    bson_t* cmd_bson = BCON_NEW("count", BCON_UTF8(collection.c_str()));
    if (!cmd_bson) {
        bson_destroy(&reply);
        return -1;
    }
    const bool ok = mongoc_client_command_simple(
        mongo.client, database.c_str(), cmd_bson, nullptr, &reply, &error);
    bson_destroy(cmd_bson);
    if (!ok) {
        bson_destroy(&reply);
        return -1;
    }
    long long count = -1;
    bson_iter_t iter;
    if (bson_iter_init_find(&iter, &reply, "n") && BSON_ITER_HOLDS_INT64(&iter)) {
        count = bson_iter_int64(&iter);
    } else if (bson_iter_init_find(&iter, &reply, "n") && BSON_ITER_HOLDS_INT32(&iter)) {
        count = bson_iter_int32(&iter);
    }
    bson_destroy(&reply);
    return count;
}
#endif

struct ApplyMeta {
    int apply_lag_seconds{-1};
    int seconds_since_last_apply{-1};
    std::string apply_status;
    std::string kafka_topic;
    int kafka_partition{-1};
    long long kafka_offset{-1};
};

struct LatestApplyBatchSlice {
    long long table_lag{-1};
    bool is_inactive{false};
    bool is_stale{false};
    long long parse_skipped{0};
    long long dropped_unrecoverable{0};
    std::optional<long long> reconcile_row_delta;
};

LatestApplyBatchSlice fetch_latest_apply_batch_slice(PGconn* pg, long long catalog_id) {
    LatestApplyBatchSlice out;
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT
            kafka_consumer_lag,
            is_inactive,
            is_stale,
            COALESCE(parse_skipped, 0),
            COALESCE(dropped_unrecoverable, 0),
            reconcile_row_delta
        FROM cdc_catalog.apply_batch_stats
        WHERE catalog_id = $1::bigint
        ORDER BY logged_at DESC
        LIMIT 1
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    if (!PQgetisnull(res, 0, 0)) {
        out.table_lag = std::atoll(PQgetvalue(res, 0, 0));
    }
    if (!PQgetisnull(res, 0, 1)) {
        out.is_inactive = (PQgetvalue(res, 0, 1)[0] == 't');
    }
    if (!PQgetisnull(res, 0, 2)) {
        out.is_stale = (PQgetvalue(res, 0, 2)[0] == 't');
    }
    if (!PQgetisnull(res, 0, 3)) {
        out.parse_skipped = std::atoll(PQgetvalue(res, 0, 3));
    }
    if (!PQgetisnull(res, 0, 4)) {
        out.dropped_unrecoverable = std::atoll(PQgetvalue(res, 0, 4));
    }
    if (!PQgetisnull(res, 0, 5)) {
        out.reconcile_row_delta = std::atoll(PQgetvalue(res, 0, 5));
    }
    PQclear(res);
    return out;
}

ApplyMeta fetch_apply_meta(PGconn* pg, long long catalog_id) {
    ApplyMeta out;
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT
            apply_lag_seconds,
            status::text,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            extract(epoch FROM (now() - last_applied_at))::integer
        FROM cdc_catalog.apply_position
        WHERE catalog_id = $1::bigint
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    out.apply_lag_seconds = PQgetisnull(res, 0, 0) ? -1 : std::atoi(PQgetvalue(res, 0, 0));
    out.apply_status = PQgetvalue(res, 0, 1) ? PQgetvalue(res, 0, 1) : "";
    if (PQgetvalue(res, 0, 2) && PQgetvalue(res, 0, 2)[0]) {
        out.kafka_topic = PQgetvalue(res, 0, 2);
    }
    if (PQgetvalue(res, 0, 3) && PQgetvalue(res, 0, 3)[0]) {
        out.kafka_partition = std::atoi(PQgetvalue(res, 0, 3));
    }
    if (PQgetvalue(res, 0, 4) && PQgetvalue(res, 0, 4)[0]) {
        out.kafka_offset = std::atoll(PQgetvalue(res, 0, 4));
    }
    if (PQgetvalue(res, 0, 5) && PQgetvalue(res, 0, 5)[0]) {
        out.seconds_since_last_apply = std::atoi(PQgetvalue(res, 0, 5));
    }
    PQclear(res);
    return out;
}

bool is_apply_inactive_table(const ApplyMeta& meta, int inactive_seconds) {
    if (meta.seconds_since_last_apply >= 0 && meta.seconds_since_last_apply > inactive_seconds) {
        return true;
    }
    return false;
}

int fetch_capture_lag_seconds(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    const CatalogReconcileRow& row) {
    if (db_engine == "mssql") {
        const char* vals[] = {
            conn_id.c_str(),
            row.source_database.c_str(),
            row.source_schema.c_str(),
            row.source_table.c_str()};
        PGresult* res = PQexecParams(
            pg,
            R"(
            SELECT GREATEST(0, extract(epoch FROM (now() - updated_at))::integer)
            FROM cdc_catalog.cdc_mssql_lsn
            WHERE conn_id = $1
              AND database = $2
              AND schema_name = $3
              AND table_name = $4
            )",
            4,
            nullptr,
            vals,
            nullptr,
            nullptr,
            0);
        int lag = -1;
        if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
            lag = std::atoi(PQgetvalue(res, 0, 0));
        }
        if (res) {
            PQclear(res);
        }
        return lag;
    }
    if (db_engine == "mongodb") {
        const char* vals[] = {
            conn_id.c_str(),
            row.source_database.c_str(),
            row.source_table.c_str()};
        PGresult* res = PQexecParams(
            pg,
            R"(
            SELECT GREATEST(0, extract(epoch FROM (now() - updated_at))::integer)
            FROM cdc_catalog.cdc_mongo_resume
            WHERE conn_id = $1 AND database = $2 AND collection = $3
            )",
            3,
            nullptr,
            vals,
            nullptr,
            nullptr,
            0);
        int lag = -1;
        if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
            lag = std::atoi(PQgetvalue(res, 0, 0));
        }
        if (res) {
            PQclear(res);
        }
        return lag;
    }
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT COALESCE(capture_lag_seconds, 0)
        FROM cdc_catalog.capture_position
        WHERE conn_id = $1
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    int lag = -1;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        lag = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return lag;
}

long long insert_reconcile_run(
    PGconn* pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::string& reconcile_mode,
    const nlohmann::json& context) {
    const std::string context_json = context.dump();
    const char* vals[] = {batch_id.c_str(), conn_id.c_str(), reconcile_mode.c_str(), context_json.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        INSERT INTO cdc_catalog.reconciliation_run (batch_id, conn_id, status, reconcile_mode, context)
        VALUES ($1, $2, 'running', $3, $4::jsonb)
        RETURNING run_id
        )",
        4,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        std::string detail = "reconcile run insert failed";
        if (res) {
            const char* err = PQresultErrorMessage(res);
            if (err && err[0]) {
                detail += ": ";
                detail += err;
            }
            PQclear(res);
        } else if (pg) {
            const char* err = PQerrorMessage(pg);
            if (err && err[0]) {
                detail += ": ";
                detail += err;
            }
        }
        throw std::runtime_error(detail);
    }
    const long long run_id = std::atoll(PQgetvalue(res, 0, 0));
    PQclear(res);
    return run_id;
}

void finish_reconcile_run(
    PGconn* pg,
    long long run_id,
    const std::string& status,
    int tables_checked,
    int tables_ok,
    int tables_warn,
    int tables_fail,
    int stale_tables,
    const nlohmann::json& context) {
    const std::string run_id_str = std::to_string(run_id);
    const std::string checked = std::to_string(tables_checked);
    const std::string ok = std::to_string(tables_ok);
    const std::string warn = std::to_string(tables_warn);
    const std::string fail = std::to_string(tables_fail);
    const std::string stale = std::to_string(stale_tables);
    const std::string context_json = context.dump();
    const char* vals[] = {
        status.c_str(),
        checked.c_str(),
        ok.c_str(),
        warn.c_str(),
        fail.c_str(),
        stale.c_str(),
        context_json.c_str(),
        run_id_str.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.reconciliation_run
        SET finished_at = now(),
            status = $1,
            tables_checked = $2::int,
            tables_ok = $3::int,
            tables_warn = $4::int,
            tables_fail = $5::int,
            stale_tables = $6::int,
            context = (
                CASE
                    WHEN jsonb_typeof(COALESCE(context, '{}'::jsonb)) = 'object'
                    THEN COALESCE(context, '{}'::jsonb)
                    ELSE '{}'::jsonb
                END
            ) || $7::jsonb
        WHERE run_id = $8::bigint
        )",
        8,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

void insert_reconcile_result(
    PGconn* pg,
    long long run_id,
    const CatalogReconcileRow& row,
    long long source_rows,
    long long lake_rows,
    const std::string& row_count_status,
    const ApplyMeta& apply_meta,
    const std::string& overall_status,
    const nlohmann::json& checks) {
    const std::string run_id_str = std::to_string(run_id);
    const std::string catalog_id_str = std::to_string(row.catalog_id);
    const std::string source_str = source_rows >= 0 ? std::to_string(source_rows) : "";
    const std::string lake_str = lake_rows >= 0 ? std::to_string(lake_rows) : "";
    const std::string delta_str =
        (source_rows >= 0 && lake_rows >= 0) ? std::to_string(source_rows - lake_rows) : "";
    const std::string lag_str =
        apply_meta.apply_lag_seconds >= 0 ? std::to_string(apply_meta.apply_lag_seconds) : "";
    const std::string checks_json = checks.dump();
    const char* vals[] = {
        run_id_str.c_str(),
        catalog_id_str.c_str(),
        row.conn_id.c_str(),
        row.source_schema.c_str(),
        row.source_table.c_str(),
        source_str.empty() ? nullptr : source_str.c_str(),
        lake_str.empty() ? nullptr : lake_str.c_str(),
        delta_str.empty() ? nullptr : delta_str.c_str(),
        row_count_status.c_str(),
        lag_str.empty() ? nullptr : lag_str.c_str(),
        apply_meta.apply_status.empty() ? nullptr : apply_meta.apply_status.c_str(),
        overall_status.c_str(),
        checks_json.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        INSERT INTO cdc_catalog.reconciliation_result (
            run_id, catalog_id, conn_id, source_schema, source_table,
            source_row_count, lake_row_count, row_count_delta, row_count_status,
            apply_lag_seconds, apply_status, status, checks
        ) VALUES (
            $1::bigint, $2::bigint, $3, $4, $5,
            NULLIF($6, '')::bigint, NULLIF($7, '')::bigint, NULLIF($8, '')::bigint, $9,
            NULLIF($10, '')::int, $11, $12, $13::jsonb
        )
        ON CONFLICT (run_id, conn_id, source_schema, source_table) DO UPDATE SET
            source_row_count = EXCLUDED.source_row_count,
            lake_row_count = EXCLUDED.lake_row_count,
            row_count_delta = EXCLUDED.row_count_delta,
            row_count_status = EXCLUDED.row_count_status,
            apply_lag_seconds = EXCLUDED.apply_lag_seconds,
            apply_status = EXCLUDED.apply_status,
            status = EXCLUDED.status,
            checks = EXCLUDED.checks
        )",
        13,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

int count_stale_tables(PGconn* pg, const std::string& conn_id) {
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT COUNT(*)::int
        FROM cdc_catalog.v_apply_stale v
        JOIN cdc_catalog.catalog c ON c.catalog_id = v.catalog_id
        WHERE v.conn_id = $1
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    int count = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        count = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return count;
}

std::optional<bool> pk_checksum_match_mariadb(
    MYSQL* mysql,
    PGconn* lake_pg,
    const CatalogReconcileRow& row,
    int sample_size,
    bool random_sample = false) {
    if (sample_size <= 0) {
        return std::nullopt;
    }
    const auto pk_cols = split_pk_columns(row.pk_columns);
    if (pk_cols.empty()) {
        return std::nullopt;
    }
    auto esc_id = [](const std::string& id) {
        std::string out;
        for (char c : id) {
            out += (c == '`') ? "``" : std::string(1, c);
        }
        return out;
    };
    std::ostringstream order_by;
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) {
            order_by << ", ";
        }
        order_by << "`" << esc_id(pk_cols[i]) << "`";
    }
    std::ostringstream src_sql;
    src_sql << "SELECT " << order_by.str() << " FROM `" << esc_id(row.source_schema) << "`.`" << esc_id(row.source_table)
            << "` ORDER BY " << (random_sample ? "RAND()" : order_by.str()) << " LIMIT " << sample_size;
    if (mysql_query(mysql, src_sql.str().c_str()) != 0) {
        return std::nullopt;
    }
    MYSQL_RES* src_res = mysql_store_result(mysql);
    if (!src_res) {
        return std::nullopt;
    }
    std::ostringstream src_payload;
    MYSQL_ROW src_row;
    while ((src_row = mysql_fetch_row(src_res)) != nullptr) {
        unsigned long* lengths = mysql_fetch_lengths(src_res);
        for (unsigned int i = 0; i < mysql_num_fields(src_res); ++i) {
            if (i > 0) {
                src_payload << '\x1f';
            }
            src_payload << (src_row[i] ? std::string(src_row[i], lengths[i]) : "");
        }
        src_payload << '\n';
    }
    mysql_free_result(src_res);

    std::ostringstream lake_cols;
    std::ostringstream lake_order;
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) {
            lake_cols << ", ";
            lake_order << ", ";
        }
        lake_cols << pg_ident(pk_cols[i]);
        lake_order << pg_ident(pk_cols[i]);
    }
    std::ostringstream lake_sql;
    lake_sql << "SELECT " << lake_cols.str() << " FROM " << pg_ident(row.lake_schema) << "."
             << pg_ident(row.lake_table) << " ORDER BY "
             << (random_sample ? "RANDOM()" : lake_order.str()) << " LIMIT " << sample_size;
    PGresult* lake_res = PQexec(lake_pg, lake_sql.str().c_str());
    if (!lake_res || PQresultStatus(lake_res) != PGRES_TUPLES_OK) {
        if (lake_res) {
            PQclear(lake_res);
        }
        return std::nullopt;
    }
    std::ostringstream lake_payload;
    for (int i = 0; i < PQntuples(lake_res); ++i) {
        for (int j = 0; j < PQnfields(lake_res); ++j) {
            if (j > 0) {
                lake_payload << '\x1f';
            }
            const char* val = PQgetvalue(lake_res, i, j);
            lake_payload << (val ? val : "");
        }
        lake_payload << '\n';
    }
    PQclear(lake_res);
    return md5_hex(src_payload.str()) == md5_hex(lake_payload.str());
}

#ifdef HAVE_FREETDS
std::optional<bool> pk_checksum_match_mssql(
    MssqlConn& mssql,
    PGconn* lake_pg,
    const CatalogReconcileRow& row,
    int sample_size) {
    if (sample_size <= 0) {
        return std::nullopt;
    }
    const auto pk_cols = split_pk_columns(row.pk_columns);
    if (pk_cols.empty()) {
        return std::nullopt;
    }
    mssql.use_database(row.source_database);
    std::ostringstream order_by;
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) {
            order_by << ", ";
        }
        order_by << mssql_brack(pk_cols[i]);
    }
    std::ostringstream src_sql;
    src_sql << "SELECT " << order_by.str() << " FROM " << mssql_brack(row.source_schema) << "."
            << mssql_brack(row.source_table) << " ORDER BY " << order_by.str() << " OFFSET 0 ROWS FETCH NEXT "
            << sample_size << " ROWS ONLY";
    const auto src_result = mssql.query(src_sql.str());
    std::ostringstream src_payload;
    for (const auto& src_row : src_result.rows) {
        for (std::size_t i = 0; i < src_row.size(); ++i) {
            if (i > 0) {
                src_payload << '\x1f';
            }
            src_payload << src_row[i].text;
        }
        src_payload << '\n';
    }

    std::ostringstream lake_cols;
    std::ostringstream lake_order;
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) {
            lake_cols << ", ";
            lake_order << ", ";
        }
        lake_cols << pg_ident(pk_cols[i]);
        lake_order << pg_ident(pk_cols[i]);
    }
    std::ostringstream lake_sql;
    lake_sql << "SELECT " << lake_cols.str() << " FROM " << pg_ident(row.lake_schema) << "."
             << pg_ident(row.lake_table) << " ORDER BY " << lake_order.str() << " LIMIT " << sample_size;
    PGresult* lake_res = PQexec(lake_pg, lake_sql.str().c_str());
    if (!lake_res || PQresultStatus(lake_res) != PGRES_TUPLES_OK) {
        if (lake_res) {
            PQclear(lake_res);
        }
        return std::nullopt;
    }
    std::ostringstream lake_payload;
    for (int i = 0; i < PQntuples(lake_res); ++i) {
        for (int j = 0; j < PQnfields(lake_res); ++j) {
            if (j > 0) {
                lake_payload << '\x1f';
            }
            const char* val = PQgetvalue(lake_res, i, j);
            lake_payload << (val ? val : "");
        }
        lake_payload << '\n';
    }
    PQclear(lake_res);
    return md5_hex(src_payload.str()) == md5_hex(lake_payload.str());
}
#endif

#ifdef HAVE_MONGOC
std::optional<bool> pk_checksum_match_mongo(
    MongoConn& mongo,
    PGconn* lake_pg,
    const CatalogReconcileRow& row,
    int sample_size) {
    if (sample_size <= 0) {
        return std::nullopt;
    }
    mongoc_collection_t* coll = mongo.collection(row.source_database, row.source_table);
    if (!coll) {
        return std::nullopt;
    }
    bson_t query = BSON_INITIALIZER;
    bson_t opts = BSON_INITIALIZER;
    bson_t sort_doc;
    bson_init(&sort_doc);
    BSON_APPEND_INT32(&sort_doc, "_id", 1);
    BSON_APPEND_DOCUMENT(&opts, "sort", &sort_doc);
    BSON_APPEND_INT32(&opts, "limit", sample_size);
    mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(coll, &query, &opts, nullptr);
    bson_destroy(&sort_doc);
    bson_destroy(&query);
    bson_destroy(&opts);
    mongoc_collection_destroy(coll);

    std::ostringstream src_payload;
    const bson_t* doc = nullptr;
    while (mongoc_cursor_next(cursor, &doc)) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "_id")) {
            if (BSON_ITER_HOLDS_OID(&iter)) {
                char oidstr[25];
                bson_oid_to_string(bson_iter_oid(&iter), oidstr);
                src_payload << oidstr;
            } else if (BSON_ITER_HOLDS_UTF8(&iter)) {
                src_payload << bson_iter_utf8(&iter, nullptr);
            } else if (BSON_ITER_HOLDS_INT32(&iter)) {
                src_payload << bson_iter_int32(&iter);
            } else if (BSON_ITER_HOLDS_INT64(&iter)) {
                src_payload << bson_iter_int64(&iter);
            }
        }
        src_payload << '\n';
    }
    mongoc_cursor_destroy(cursor);

    std::ostringstream lake_sql;
    lake_sql << "SELECT mongo_id FROM " << pg_ident(row.lake_schema) << "." << pg_ident(row.lake_table)
             << " ORDER BY mongo_id LIMIT " << sample_size;
    PGresult* lake_res = PQexec(lake_pg, lake_sql.str().c_str());
    if (!lake_res || PQresultStatus(lake_res) != PGRES_TUPLES_OK) {
        if (lake_res) {
            PQclear(lake_res);
        }
        return std::nullopt;
    }
    std::ostringstream lake_payload;
    for (int i = 0; i < PQntuples(lake_res); ++i) {
        const char* val = PQgetvalue(lake_res, i, 0);
        lake_payload << (val ? val : "") << '\n';
    }
    PQclear(lake_res);
    return md5_hex(src_payload.str()) == md5_hex(lake_payload.str());
}
#endif

}  // namespace

int run_reconcile_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    std::atomic<bool>* shutdown,
    const ReconcileCycleScope* cycle) {
    const bool cycle_mode = cycle != nullptr && cycle->run_id >= 0;
    const std::string batch_id = cycle_mode ? cycle->batch_id : make_batch_id();
    const std::string db_engine = conn_engine(cfg, conn_id);

    RuntimeConfig runtime;
    const ReconcileRuntime rcfg = load_reconcile_runtime(runtime, log_pg, conn_id);

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "reconcile",
        .message = cycle_mode ? "reconcile conn started (cycle)" : "reconcile started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"db_engine", db_engine}, {"enabled", rcfg.enabled}, {"cycle_mode", cycle_mode}},
    });

    if (!rcfg.enabled) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "reconcile",
            .message = "reconcile skipped: disabled in runtime_config",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
        });
        return 0;
    }

    std::optional<ReconcileConnLockGuard> reconcile_lock;
    if (!cycle_mode) {
        long long reconcile_lock_key = 0;
        if (!acquire_reconcile_conn_lock(log_pg, conn_id, &reconcile_lock_key, shutdown)) {
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "reconcile",
                .message = "reconcile skipped: shutdown before advisory lock acquired",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
            });
            return 0;
        }
        reconcile_lock.emplace(log_pg, reconcile_lock_key);
        abort_stale_reconcile_runs(log_pg, conn_id);
        abort_orphan_reconcile_runs(log_pg, conn_id);
    }

    const int max_tables = rcfg.max_tables > 0 ? rcfg.max_tables : 0;
    const auto tables = fetch_reconcile_catalog(log_pg, conn_id, max_tables);
    const int stale_tables = count_stale_tables(log_pg, conn_id);
    const std::string kReconcileMode = pipeline_defaults::kReconcileModeLake;
    const bool pipeline_lake_mode = true;
    const bool pk_sample = false;

    long long run_id = cycle_mode ? cycle->run_id : 0;
    if (!cycle_mode) {
        run_id = insert_reconcile_run(
            log_pg,
            batch_id,
            conn_id,
            kReconcileMode,
            {{"db_engine", db_engine},
             {"mode", kReconcileMode},
             {"scope", "conn"},
             {"tables_planned", static_cast<int>(tables.size())},
             {"catalog_tables_total", static_cast<int>(tables.size())},
             {"stale_tables", stale_tables}});
    }

    PgConn lake_pg(cfg.datalake.conn_string());

    int tables_ok = 0;
    int tables_warn = 0;
    int tables_fail = 0;
    int tables_skip = 0;
    int table_errors = 0;
    int tables_processed = 0;

    for (const auto& row : tables) {
        runtime.reload(log_pg);
        try {
            if (const auto skip_reason = reconcile_catalog_skip_reason(row)) {
                nlohmann::json checks = nlohmann::json::object();
                checks["reconcile_mode"] = kReconcileMode;
                checks["skip_reason"] = *skip_reason;
                checks["catalog_status"] = row.catalog_status;
                checks["active"] = row.active;
                checks["cdc_enabled"] = row.cdc_enabled;
                checks["needs_full_load"] = row.needs_full_load;
                checks["has_pk"] = row.has_pk;
                insert_reconcile_result(
                    log_pg, run_id, row, -1, -1, "skip", ApplyMeta{}, "skip", checks);
                tables_skip += 1;
                if (cycle_mode) {
                    tables_processed += 1;
                    if (tables_processed % pipeline_defaults::kReconcileProgressRefreshEveryNTables == 0) {
                        refresh_reconcile_run_progress(
                            log_pg, run_id, {{"current_conn", conn_id}});
                    }
                }
                continue;
            }

            const auto table_t0 = std::chrono::steady_clock::now();
            const ApplyMeta apply_meta = fetch_apply_meta(log_pg, row.catalog_id);
            const LatestApplyBatchSlice batch_slice =
                fetch_latest_apply_batch_slice(log_pg, row.catalog_id);
            const std::string lag_status = eval_apply_lag_status(apply_meta.apply_lag_seconds, rcfg);
            const bool apply_inactive_table =
                is_apply_inactive_table(apply_meta, rcfg.apply_inactive_seconds);
            const auto prev_snapshot =
                reconcile_enrichments::fetch_prev_reconcile_snapshot(log_pg, row.catalog_id);
            const auto events_7d = reconcile_enrichments::fetch_apply_events_aggregate(
                log_pg,
                row.catalog_id,
                pipeline_defaults::kReconcileEventsLookbackSeconds);
            const auto tier_decision = reconcile_enrichments::eval_reconcile_tier(
                row, apply_inactive_table, events_7d.has_flow, prev_snapshot);

            long long source_rows = -1;
            long long lake_rows = -1;
            std::string row_status = tier_decision.skip_row_count ? "ok" : "skip";
            std::string drift_kind = tier_decision.skip_row_count ? "none" : "pipeline_only";

            const bool has_recent_apply_flow = events_7d.has_flow;
            const int capture_lag_seconds = fetch_capture_lag_seconds(log_pg, conn_id, db_engine, row);

            std::string capture_status_raw;
            std::optional<std::string> capture_skip_reason;
            if (apply_inactive_table && row_status == "ok") {
                capture_status_raw = "skip";
                capture_skip_reason = "capture_stale_idle_table";
            } else {
                capture_status_raw = eval_capture_lag_status(capture_lag_seconds, rcfg);
                if (has_recent_apply_flow && row_status == "ok" && lag_status == "ok" &&
                    (capture_status_raw == "warn" || capture_status_raw == "fail")) {
                    capture_skip_reason = "capture_stale_with_recent_apply_flow";
                }
            }
            const std::string capture_for_overall =
                cap_capture_for_overall(row_status, lag_status, capture_status_raw);
            std::string overall = status_rank(status_rank(row_status, lag_status), capture_for_overall);
            if (apply_meta.apply_status == "quarantined") {
                overall = status_rank(overall, "fail");
            }

            nlohmann::json checks = nlohmann::json::object();
            checks["reconcile_mode"] = kReconcileMode;
            checks["pipeline_lake_only"] = pipeline_lake_mode;
            checks["metadata_only_reconcile"] = true;
            checks["capture_lag_seconds"] = capture_lag_seconds;
            checks["capture_lag_status"] = capture_status_raw;
            checks["capture_lag_status_raw"] = capture_status_raw;
            checks["capture_lag_status_overall"] = capture_for_overall;
            checks["row_count_status"] = row_status;
            checks["apply_lag_status"] = lag_status;
            checks["drift_kind"] = drift_kind;
            checks["seconds_since_last_apply"] = apply_meta.seconds_since_last_apply;
            checks["apply_inactive_table"] = apply_inactive_table;
            checks["reconcile_tier"] = tier_decision.tier;
            if (!tier_decision.skip_reason.empty()) {
                checks["reconcile_tier_skip_reason"] = tier_decision.skip_reason;
            }
            if (capture_skip_reason) {
                checks["capture_lag_skip_reason"] = *capture_skip_reason;
            }
            if (pipeline_lake_mode) {
                checks["row_count_scope"] = "pipeline_only";
            }
            if (source_rows >= 0) {
                checks["source_row_count"] = source_rows;
            }
            if (lake_rows >= 0) {
                checks["lake_row_count"] = lake_rows;
            }
            if (source_rows >= 0 && lake_rows >= 0) {
                checks["row_count_delta"] = source_rows - lake_rows;
            }
            if (!row.last_error.empty()) {
                checks["catalog_last_error"] = row.last_error;
            }

            bool kafka_inactive_table = false;
#ifdef HAVE_RDKAFKA
            long long kafka_lag_probe = -1;
            long long kafka_partition_lag_probe = -1;
            if (batch_slice.table_lag >= 0) {
                kafka_lag_probe = batch_slice.table_lag;
            }
            kafka_inactive_table = batch_slice.is_inactive || apply_inactive_table;
            const std::string kafka_status_raw = kafka_inactive_table
                ? "skip"
                : eval_kafka_lag_status(kafka_lag_probe, rcfg);
            const std::string kafka_for_overall =
                cap_kafka_for_overall(row_status, kafka_status_raw);
            checks["kafka_consumer_lag"] = kafka_inactive_table ? 0 : kafka_lag_probe;
            checks["kafka_consumer_lag_probe"] = kafka_lag_probe;
            checks["kafka_partition_lag_probe"] = kafka_partition_lag_probe;
            checks["kafka_lag_kind"] = batch_slice.table_lag >= 0 ? "table_exact" : "metadata_missing";
            checks["kafka_inactive_table"] = kafka_inactive_table;
            if (kafka_inactive_table) {
                checks["kafka_lag_skip_reason"] = batch_slice.is_inactive
                    ? "inactive_table_slice"
                    : "inactive_table_no_recent_apply";
            }
            checks["kafka_lag_status_raw"] = kafka_status_raw;
            checks["kafka_lag_status"] = kafka_status_raw;
            checks["kafka_lag_status_overall"] = kafka_for_overall;
            checks["kafka_topic"] = apply_meta.kafka_topic;
            checks["kafka_partition"] = apply_meta.kafka_partition;
            checks["kafka_offset"] = apply_meta.kafka_offset;
            overall = status_rank(overall, kafka_for_overall);
#else
            checks["kafka_consumer_lag"] = nullptr;
#endif

            const std::string event_loss_status = reconcile_enrichments::eval_event_loss_status(
                batch_slice.parse_skipped, batch_slice.dropped_unrecoverable);
            const std::string slice_stale_status = reconcile_enrichments::eval_slice_stale_status(
                batch_slice.is_stale, apply_meta.apply_status, lag_status);
            checks["parse_skipped"] = batch_slice.parse_skipped;
            checks["dropped_unrecoverable"] = batch_slice.dropped_unrecoverable;
            checks["event_loss_status"] = event_loss_status;
            checks["slice_is_stale"] = batch_slice.is_stale;
            checks["slice_stale_status"] = slice_stale_status;
            if (batch_slice.reconcile_row_delta) {
                checks["hist_reconcile_row_delta"] = *batch_slice.reconcile_row_delta;
            }
            const bool apply_position_unhealthy =
                apply_meta.apply_status == "stale" || apply_meta.apply_status == "lagging" ||
                apply_meta.apply_status == "gap_detected" || apply_meta.apply_status == "failed" ||
                apply_meta.apply_status == "quarantined" || lag_status == "fail" ||
                lag_status == "warn";
            checks["slice_stale_apply_position_confirm"] =
                batch_slice.is_stale && apply_position_unhealthy;
            overall = status_rank(overall, event_loss_status);
            overall = status_rank(overall, slice_stale_status);

            bool static_gap_candidate = false;
            {
                const bool pipeline_healthy =
                    lag_status != "fail" && capture_for_overall != "fail";
                static_gap_candidate =
                    pipeline_healthy && row_status == "fail" &&
                    (drift_kind == "source_ahead" || drift_kind == "append_zombie");
                checks["static_gap_detected"] = static_gap_candidate;
                if (static_gap_candidate && kafka_inactive_table) {
                    checks["static_gap_reason"] = "pipeline_healthy_row_drift_inactive_table";
                }
            }

            FreshnessInput freshness_in;

            auto enrichment = reconcile_enrichments::build_enrichment_bundle(
                log_pg,
                lake_pg.raw,
                row,
                rcfg.row_warn_abs_tolerance,
                pipeline_defaults::kReconcileFreshnessLagWarnMinutes,
                pipeline_defaults::kReconcileFreshnessLagFailMinutes,
                pipeline_defaults::kReconcileEventsLookbackSeconds,
                freshness_in,
                source_rows,
                lake_rows,
                row_status,
                drift_kind,
                lag_status,
                apply_meta.apply_status,
                overall,
                apply_inactive_table,
                static_gap_candidate,
                !row.last_error.empty());
            reconcile_enrichments::apply_enrichment_to_checks(checks, enrichment);

            if (enrichment.timing_skew_suspected && row_status == "warn") {
                overall = status_rank(status_rank("ok", lag_status), capture_for_overall);
#ifdef HAVE_RDKAFKA
                overall = status_rank(overall, kafka_for_overall);
#endif
                checks["row_count_status_effective"] = "ok";
                checks["reconcile_timing_skew_downgraded"] = true;
            }
            if (enrichment.freshness.status == "fail") {
                overall = status_rank(overall, "fail");
            } else if (enrichment.freshness.status == "warn") {
                overall = status_rank(overall, "warn");
            }
            if (prev_snapshot && !prev_snapshot->status.empty()) {
                checks["status_flipped"] = prev_snapshot->status != overall;
            }
            checks["overall_status"] = overall;
            const long long abs_delta_final =
                source_rows >= 0 && lake_rows >= 0 ? std::llabs(source_rows - lake_rows) : 0;
            checks["recommended_action"] = reconcile_enrichments::derive_recommended_action(
                overall,
                row_status,
                drift_kind,
                abs_delta_final,
                checks.value("suggest_full_load", false),
                apply_meta.apply_status == "quarantined");
            if (event_loss_status == "fail") {
                checks["semaphore_reason"] = "dropped_unrecoverable";
                checks["recommended_action"] = "investigate_pipeline";
            } else if (event_loss_status == "warn") {
                checks["semaphore_reason"] = "parse_skipped";
            } else if (slice_stale_status == "fail") {
                checks["semaphore_reason"] = "slice_stale_confirmed";
                if (checks["recommended_action"] == "none") {
                    checks["recommended_action"] = "investigate_pipeline";
                }
            } else if (batch_slice.is_stale && slice_stale_status == "warn") {
                checks["semaphore_reason"] = "slice_stale";
            }
            checks["root_cause_label"] = reconcile_enrichments::derive_root_cause_label(
                overall,
                row_status,
                drift_kind,
                checks.value("semaphore_reason", std::string{}),
                static_gap_candidate,
                enrichment.timing_skew_suspected,
                apply_meta.apply_status == "quarantined",
                event_loss_status,
                slice_stale_status);

            const auto table_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      std::chrono::steady_clock::now() - table_t0)
                                      .count();
            checks["table_duration_ms"] = table_ms;
            if (cycle_mode && cycle && cycle->table_timings_ms) {
                reconcile_enrichments::append_table_timing_ms(*cycle->table_timings_ms, table_ms);
            }

            insert_reconcile_result(
                log_pg, run_id, row, source_rows, lake_rows, row_status, apply_meta, overall, checks);

            // Reconcile outcomes are persisted in reconciliation_result only.
            // Do not mark catalog.status failed or needs_full_load — that blocks CDC gap recovery.
            if (overall == "ok" && row_status == "ok") {
                mark_catalog_reconcile_healed(log_pg, row.catalog_id);
            }

            if (overall == "fail") {
                tables_fail += 1;
            } else if (overall == "warn") {
                tables_warn += 1;
            } else if (overall == "ok") {
                tables_ok += 1;
            }

            log_write(log_pg, {
                .level = overall == "fail" ? LogLevel::Warning : LogLevel::Info,
                .component = "reconcile",
                .message = "table reconcile completed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = row.source_schema,
                .source_table = row.source_table,
                .context = {
                    {"status", overall},
                    {"source_row_count", source_rows},
                    {"lake_row_count", lake_rows},
                    {"row_count_delta", source_rows >= 0 && lake_rows >= 0 ? source_rows - lake_rows : 0},
                    {"row_count_status", row_status},
                    {"drift_kind", drift_kind},
                    {"static_gap_detected", checks.value("static_gap_detected", false)},
                    {"apply_lag_seconds", apply_meta.apply_lag_seconds},
                    {"apply_lag_status", lag_status},
                    {"capture_lag_seconds", capture_lag_seconds},
                    {"capture_lag_status", capture_status_raw},
                    {"capture_lag_status_overall", capture_for_overall},
                    {"reconcile_mode", kReconcileMode},
                    {"kafka_consumer_lag", checks.value("kafka_consumer_lag", nlohmann::json())},
                    {"kafka_lag_status", checks.value("kafka_lag_status", nlohmann::json("skip"))},
                },
            });
        } catch (const std::exception& ex) {
            table_errors += 1;
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "reconcile",
                .message = "table reconcile failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = row.source_schema,
                .source_table = row.source_table,
                .context = {{"error", ex.what()}},
            });
        }

        if (cycle_mode) {
            tables_processed += 1;
            if (tables_processed % pipeline_defaults::kReconcileProgressRefreshEveryNTables == 0) {
                refresh_reconcile_run_progress(
                    log_pg,
                    run_id,
                    {{"current_conn", conn_id}});
            }
        }
    }

    std::string run_status = "ok";
    if (tables_fail > 0 || table_errors > 0) {
        run_status = "fail";
    } else if (tables_warn > 0) {
        run_status = "warn";
    }

    if (!cycle_mode) {
        finish_reconcile_run(
            log_pg,
            run_id,
            run_status,
            static_cast<int>(tables.size()),
            tables_ok,
            tables_warn,
            tables_fail,
            stale_tables,
            {{"table_errors", table_errors},
             {"tables_skip", tables_skip},
             {"db_engine", db_engine},
             {"mode", kReconcileMode},
             {"scope", "conn"},
             {"catalog_tables_total", static_cast<int>(tables.size())}});
    }

    log_write(log_pg, {
        .level = run_status == "fail" ? LogLevel::Warning : LogLevel::Info,
        .component = "reconcile",
        .message = run_status == "fail" ? "reconcile conn completed with errors"
                                        : "reconcile conn completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"run_id", run_id},
            {"mode", kReconcileMode},
            {"scope", cycle_mode ? "cycle" : "conn"},
            {"status", run_status},
            {"tables_checked", static_cast<int>(tables.size())},
            {"tables_ok", tables_ok},
            {"tables_warn", tables_warn},
            {"tables_fail", tables_fail},
            {"tables_skip", tables_skip},
            {"stale_tables", stale_tables},
            {"table_errors", table_errors},
        },
    });

    return run_status == "fail" ? 1 : 0;
}

int run_reconcile_loop(
    const AppConfig& cfg,
    PGconn* log_pg,
    bool once,
    std::atomic<bool>* external_shutdown) {
    run_startup_schema_migrate(log_pg);

    std::atomic<bool>* shutdown = external_shutdown ? external_shutdown : &g_reconcile_shutdown;
    if (!external_shutdown) {
        std::signal(SIGINT, on_reconcile_signal);
        std::signal(SIGTERM, on_reconcile_signal);
    }

    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const int retention_days =
        runtime.get_int("logs_retention_days", pipeline_defaults::kLogsRetentionDaysDefault, "global");
    purge_logs(log_pg, retention_days);

    std::vector<std::string> conn_ids;
    for (const auto& s : cfg.mariadb_sources) {
        conn_ids.push_back(s.conn_id);
    }
    for (const auto& s : cfg.mssql_sources) {
        conn_ids.push_back(s.conn_id);
    }
    for (const auto& s : cfg.mongo_sources) {
        conn_ids.push_back(s.conn_id);
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "reconcile",
        .message = "reconcile-loop started",
        .batch_id = make_batch_id(),
        .conn_id = std::nullopt,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"conn_ids", static_cast<int>(conn_ids.size())}},
    });

    while (!shutdown->load()) {
        runtime.reload(log_pg);

        long long loop_lock_key = 0;
        if (!acquire_reconcile_loop_lock(log_pg, &loop_lock_key, shutdown)) {
            break;
        }
        ReconcileConnLockGuard loop_lock(log_pg, loop_lock_key);

        cleanup_reconcile_runs_at_cycle_start(log_pg);

        nlohmann::json conn_ids_json = nlohmann::json::array();
        for (const auto& conn_id : conn_ids) {
            conn_ids_json.push_back(conn_id);
        }

        const int catalog_total = count_catalog_tables(log_pg, std::nullopt);
        const int stale_total = count_total_stale_tables(log_pg);
        nlohmann::json completed_conns = nlohmann::json::array();

        std::string cycle_reconcile_mode = pipeline_defaults::kReconcileModeLake;

        long long cycle_run_id = 0;
        std::string batch_id;
        bool cycle_resumed = false;
        if (const auto resumable = find_resumable_cycle_run(log_pg)) {
            cycle_run_id = resumable->run_id;
            batch_id = resumable->batch_id;
            completed_conns = resumable->completed_conns;
            if (!resumable->reconcile_mode.empty() &&
                resumable->reconcile_mode == pipeline_defaults::kReconcileModeLake) {
                cycle_reconcile_mode = resumable->reconcile_mode;
            }
            cycle_resumed = true;
            refresh_reconcile_run_progress(
                log_pg,
                cycle_run_id,
                {{"resumed", true},
                 {"catalog_tables_total", catalog_total},
                 {"stale_tables", stale_total},
                 {"conn_count", static_cast<int>(conn_ids.size())},
                 {"conn_ids", conn_ids_json},
                 {"completed_conns", completed_conns}});
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "reconcile",
                .message = "reconcile cycle resumed",
                .batch_id = batch_id,
                .conn_id = std::nullopt,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"run_id", cycle_run_id},
                    {"completed_conns", completed_conns},
                    {"result_count_pending", true},
                },
            });
        } else {
            batch_id = make_batch_id();
            cycle_run_id = insert_reconcile_run(
                log_pg,
                batch_id,
                pipeline_defaults::kReconcileCycleConnId,
                cycle_reconcile_mode,
                {{"mode", cycle_reconcile_mode},
                 {"scope", "cycle"},
                 {"conn_count", static_cast<int>(conn_ids.size())},
                 {"conn_ids", conn_ids_json},
                 {"catalog_tables_total", catalog_total},
                 {"stale_tables", stale_total},
                 {"completed_conns", completed_conns}});
        }

        int cycle_exit = 0;
        bool cycle_interrupted = false;
        const auto cycle_t0 = std::chrono::steady_clock::now();
        std::vector<long long> cycle_table_timings;

        for (const auto& conn_id : conn_ids) {
            if (completed_conns_contains(completed_conns, conn_id)) {
                continue;
            }
            if (shutdown->load()) {
                cycle_interrupted = true;
                break;
            }

            refresh_reconcile_run_progress(
                log_pg,
                cycle_run_id,
                {{"current_conn", conn_id},
                 {"completed_conns", completed_conns}});

            ReconcileCycleScope cycle_scope{
                cycle_run_id,
                batch_id,
                &cycle_table_timings,
                cycle_reconcile_mode};
            try {
                const int conn_rc = run_reconcile_cli(cfg, log_pg, conn_id, shutdown, &cycle_scope);
                if (conn_rc != 0) {
                    cycle_exit = 1;
                }
            } catch (const std::exception& ex) {
                cycle_exit = 1;
                log_write(log_pg, {
                    .level = LogLevel::Error,
                    .component = "reconcile",
                    .message = "reconcile-loop conn failed",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = std::nullopt,
                    .source_table = std::nullopt,
                    .context = {{"error", ex.what()}, {"run_id", cycle_run_id}},
                });
            }

            completed_conns.push_back(conn_id);
            refresh_reconcile_run_progress(
                log_pg,
                cycle_run_id,
                {{"current_conn", nullptr},
                 {"completed_conns", completed_conns}});
        }

        refresh_reconcile_run_progress(log_pg, cycle_run_id, {});

        const std::string cycle_run_id_str = std::to_string(cycle_run_id);
        const char* stats_vals[] = {cycle_run_id_str.c_str()};
        PGresult* stats_res = PQexecParams(
            log_pg,
            R"(
            SELECT tables_checked, tables_ok, tables_warn, tables_fail, stale_tables,
                   COALESCE((context->>'tables_skip')::int, 0) AS tables_skip
            FROM cdc_catalog.reconciliation_run
            WHERE run_id = $1::bigint
            )",
            1,
            nullptr,
            stats_vals,
            nullptr,
            nullptr,
            0);
        int tables_checked = 0;
        int tables_ok = 0;
        int tables_warn = 0;
        int tables_fail = 0;
        int stale_tables = stale_total;
        int tables_skip = 0;
        if (stats_res && PQresultStatus(stats_res) == PGRES_TUPLES_OK && PQntuples(stats_res) > 0) {
            tables_checked = std::atoi(PQgetvalue(stats_res, 0, 0));
            tables_ok = std::atoi(PQgetvalue(stats_res, 0, 1));
            tables_warn = std::atoi(PQgetvalue(stats_res, 0, 2));
            tables_fail = std::atoi(PQgetvalue(stats_res, 0, 3));
            stale_tables = std::atoi(PQgetvalue(stats_res, 0, 4));
            tables_skip = std::atoi(PQgetvalue(stats_res, 0, 5));
        }
        if (stats_res) {
            PQclear(stats_res);
        }

        std::string cycle_status = "ok";
        const auto cycle_decision = eval_cycle_run_status(
            cycle_interrupted,
            cycle_exit,
            tables_checked,
            tables_ok,
            tables_warn,
            tables_fail,
            tables_skip);
        cycle_status = cycle_decision.status;

        if (cycle_interrupted) {
            refresh_reconcile_run_progress(
                log_pg,
                cycle_run_id,
                {{"interrupted", true},
                 {"completed_conns", completed_conns},
                 {"conn_ids", conn_ids_json},
                 {"resumed", cycle_resumed}});
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "reconcile",
                .message = "reconcile cycle interrupted, will resume on next start",
                .batch_id = batch_id,
                .conn_id = std::nullopt,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"run_id", cycle_run_id},
                    {"scope", "cycle"},
                    {"tables_checked", tables_checked},
                    {"completed_conns", completed_conns},
                },
            });
        } else {
            const long long cycle_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                                    std::chrono::steady_clock::now() - cycle_t0)
                                                    .count();
            const nlohmann::json cycle_sla =
                reconcile_enrichments::build_cycle_sla_json(cycle_table_timings, cycle_duration_ms);
            reconcile_enrichments::upsert_daily_snapshots(log_pg, cycle_run_id);

            finish_reconcile_run(
                log_pg,
                cycle_run_id,
                cycle_status,
                tables_checked,
                tables_ok,
                tables_warn,
                tables_fail,
                stale_tables,
                {{"mode", "full"},
                 {"scope", "cycle"},
                 {"conn_count", static_cast<int>(conn_ids.size())},
                 {"conn_ids", conn_ids_json},
                 {"completed_conns", completed_conns},
                 {"tables_skip", tables_skip},
                 {"interrupted", false},
                 {"resumed", cycle_resumed},
                 {"cycle_sla", cycle_sla},
                 {"cycle_status", cycle_status},
                 {"cycle_strict_status", cycle_decision.strict_status},
                 {"cycle_ok_rate", cycle_decision.ok_rate},
                 {"cycle_actionable_tables", cycle_decision.actionable_tables},
                 {"cycle_sla_threshold", pipeline_defaults::kReconcileCycleOkRatePass},
                 {"cycle_sla_met", cycle_decision.sla_met},
                 {"cycle_status_sla_adjusted",
                  cycle_decision.strict_status != cycle_decision.status}});

            log_write(log_pg, {
                .level = cycle_status == "fail" ? LogLevel::Warning : LogLevel::Info,
                .component = "reconcile",
                .message = cycle_decision.strict_status != cycle_decision.status
                    ? "reconcile cycle completed (SLA pass with residual fail tables)"
                    : cycle_status == "fail" ? "reconcile cycle completed with errors"
                                               : "reconcile cycle completed",
                .batch_id = batch_id,
                .conn_id = std::nullopt,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"run_id", cycle_run_id},
                    {"scope", "cycle"},
                    {"status", cycle_status},
                    {"tables_checked", tables_checked},
                    {"tables_ok", tables_ok},
                    {"tables_warn", tables_warn},
                    {"tables_fail", tables_fail},
                    {"tables_skip", tables_skip},
                    {"interrupted", false},
                },
            });
        }

        loop_lock.held = false;
        release_reconcile_conn_lock(log_pg, loop_lock_key);

        if (once || shutdown->load()) {
            break;
        }
        const int interval_hours =
            runtime.get_int(
                "reconcile_interval_hours",
                pipeline_defaults::kReconcileIntervalHoursDefault,
                "cdc_kafka_reconcile",
                "");
        const int sleep_sec = std::max(60, interval_hours * 3600);
        for (int i = 0; i < sleep_sec && !shutdown->load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "reconcile",
        .message = "reconcile-loop stopped",
        .batch_id = make_batch_id(),
        .conn_id = std::nullopt,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
    });
    return 0;
}
