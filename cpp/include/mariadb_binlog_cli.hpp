#pragma once

#include "config.hpp"
#include "mariadb_binlog.hpp"

#include <functional>
#include <libpq-fe.h>
#include <string>
#include <vector>

// Reads remote binlog via mariadb-binlog -v (mariadb_rpl workaround for MariaDB 12.x).
struct BinlogCliStats {
    long long events{0};
    long long upserts{0};
    long long deletes{0};
    long long last_position{0};
    std::string last_file;
    int exit_code{-1};
    std::string stderr_tail;
    bool cli_missing{false};
};

/** Resolve mariadb-binlog (or mysqlbinlog) on PATH; empty if not installed. */
std::string find_mariadb_binlog_binary();

using BinlogRowHandler = std::function<void(
    const std::string& schema,
    const std::string& table,
    const std::string& op,
    const std::vector<std::string>& col_values,
    const std::vector<std::string>* before_col_values,
    long long event_position,
    const std::optional<long long>& tx_id)>;

using BinlogTxHandler = std::function<void(
    const std::string& tx_event,
    long long tx_id,
    long long event_position)>;

BinlogCliStats read_remote_binlog_cli(
    const MariaDbSource& source,
    const BinlogPosition& start,
    int max_seconds,
    int max_events,
    const BinlogRowHandler& on_row,
    const std::function<bool()>& should_stop,
    const BinlogTxHandler& on_tx = {});
