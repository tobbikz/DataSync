#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <string>

struct MssqlCaptureStats {
    std::string batch_id;
    int events_published{0};
    int errors{0};
    int tables{0};
    long long duration_ms{0};
};

#ifdef HAVE_FREETDS
#include "mssql_conn.hpp"

/** Seed cdc_mssql_lsn to max LSN at full-load start for one table (skip if row exists). */
bool seed_mssql_cdc_lsn_for_table_if_absent(
    PGconn* log_pg,
    MssqlConn& mssql,
    const std::string& conn_id,
    const std::string& database,
    const std::string& schema,
    const std::string& table);

/** Force cdc_mssql_lsn to current max LSN (T0) when full load starts; overwrites stale positions. */
bool seed_mssql_cdc_lsn_t0_for_table(
    PGconn* log_pg,
    MssqlConn& mssql,
    const std::string& conn_id,
    const std::string& database,
    const std::string& schema,
    const std::string& table);
#endif

/** Seed all catalog tables for conn; skips tables that already have LSN rows. */
int seed_mssql_cdc_lsn_for_conn(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id);

MssqlCaptureStats run_mssql_kafka_capture_slice(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    int worker_id = 0,
    int worker_count = 1);
