#pragma once

#include "mariadb_binlog_cli.hpp"

#include <functional>
#include <string>

/**
 * S09 dispatcher: `auto` tries native then CLI; `cli` / `native` force one path.
 * `binlog_mode` from CdcConfig.
 */
BinlogCliStats read_remote_binlog(
    const MariaDbSource& source,
    const BinlogPosition& start,
    int max_seconds,
    int max_events,
    const BinlogRowHandler& on_row,
    const std::function<bool()>& should_stop,
    const BinlogTxHandler& on_tx,
    const std::string& binlog_mode,
    const BinlogQueryHandler& on_query = {});
