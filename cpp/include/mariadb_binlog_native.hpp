#pragma once

#include "mariadb_binlog_cli.hpp"

#include <functional>
#include <string>

/**
 * S09: in-process binlog via Connector/C RPL (HAVE_MARIADB_RPL).
 * Throws on unavailable build or open/fetch/decode failure — caller may fall back to CLI.
 */
BinlogCliStats read_remote_binlog_native(
    const MariaDbSource& source,
    const BinlogPosition& start,
    int max_seconds,
    int max_events,
    const BinlogRowHandler& on_row,
    const std::function<bool()>& should_stop,
    const BinlogTxHandler& on_tx = {});

/** True when this binary was built with mariadb_rpl.h. */
bool mariadb_binlog_native_available();
