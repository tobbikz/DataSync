#pragma once

#include "mariadb_conn.hpp"

#include <string>

/** MariaDB source-side watermark table for F03/F08 incremental snapshots. */
namespace mariadb_signal {

inline constexpr const char* kSchema = "datasync";
inline constexpr const char* kTable = "datasync_signal";

/** CREATE DATABASE/SCHEMA-less: table in current db as `datasync_signal` (simple lab path). */
void ensure_signal_table(MariaDbConn& conn);

/** Insert open/close watermark; returns signal row id. kind: open|close. */
long long insert_watermark(
    MariaDbConn& conn,
    const std::string& catalog_key,
    const std::string& kind,
    const std::string& gtid_executed);

/** @@GLOBAL.gtid_binlog_pos or gtid_current_pos — best-effort string. */
std::string read_gtid_executed(MariaDbConn& conn);

}  // namespace mariadb_signal
