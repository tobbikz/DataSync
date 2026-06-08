#pragma once

#include <libpq-fe.h>
#include <mysql/mysql.h>

#include <optional>
#include <string>

struct BinlogPosition {
    std::string file;
    long long position{0};
};

// SHOW MASTER STATUS → insert cdc_catalog.capture_position (T0 at full-load start; never overwrite).
bool capture_binlog_position_t0_if_absent(PGconn* pg, MYSQL* mysql, const std::string& conn_id);

// Legacy name — always upserts (capture checkpoint advance only).
void capture_binlog_position_t0(PGconn* pg, MYSQL* mysql, const std::string& conn_id);

std::optional<BinlogPosition> fetch_binlog_position(PGconn* pg, const std::string& conn_id);

// Updates binlog file:pos in capture_position (legacy mariadb_cdc path)
void upsert_binlog_position(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& binlog_file,
    long long binlog_position);
