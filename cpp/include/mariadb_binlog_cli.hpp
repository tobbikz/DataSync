#pragma once

#include "config.hpp"
#include "mariadb_cdc.hpp"

#include <functional>
#include <libpq-fe.h>
#include <string>

struct BinlogPosition;

// Reads remote binlog via mariadb-binlog -v (mariadb_rpl workaround for MariaDB 12.x).
struct BinlogCliStats {
    long long events{0};
    long long upserts{0};
    long long deletes{0};
    long long last_position{0};
    std::string last_file;
};

using BinlogRowHandler = std::function<void(
    const std::string& schema,
    const std::string& table,
    const std::string& op,
    const std::vector<std::string>& col_values)>;

BinlogCliStats read_remote_binlog_cli(
    const MariaDbSource& source,
    const BinlogPosition& start,
    int max_seconds,
    int max_events,
    const BinlogRowHandler& on_row,
    const std::function<bool()>& should_stop);
