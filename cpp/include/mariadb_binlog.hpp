#pragma once

#include <libpq-fe.h>
#include <mysql/mysql.h>

#include <optional>
#include <string>
#include <vector>

struct BinlogPosition {
    std::string file;
    long long position{0};
};

struct BinlogFileEntry {
    std::string name;
    long long file_size{0};
};

struct MasterBinlogStatus {
    std::string file;
    long long position{0};
};

/** Numeric suffix from e.g. mariadb2-bin.013283 → 13283 (for ordering). */
std::optional<long long> binlog_file_sequence_number(const std::string& filename);

std::vector<BinlogFileEntry> fetch_binary_logs(MYSQL* mysql);

MasterBinlogStatus fetch_master_binlog_status(MYSQL* mysql);

bool binlog_position_caught_up(const BinlogPosition& cursor, const MasterBinlogStatus& master);

bool binlog_cursor_is_behind(const BinlogPosition& cursor, const MasterBinlogStatus& master);

/** When cursor is at/past EOF, move to the next binlog file (position 4). Preserves history drain. */
bool advance_binlog_cursor_to_next_file(MYSQL* mysql, BinlogPosition& cursor);

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
