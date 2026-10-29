#include "mariadb_binlog_read.hpp"

#include "mariadb_binlog_native.hpp"

#include <atomic>
#include <stdexcept>
#include <string>

namespace {
std::atomic<bool> g_native_auto_failed{false};
}

BinlogCliStats read_remote_binlog(
    const MariaDbSource& source,
    const BinlogPosition& start,
    int max_seconds,
    int max_events,
    const BinlogRowHandler& on_row,
    const std::function<bool()>& should_stop,
    const BinlogTxHandler& on_tx,
    const std::string& binlog_mode,
    const BinlogQueryHandler& on_query) {
    const std::string mode = binlog_mode.empty() ? "auto" : binlog_mode;

    auto run_cli = [&]() {
        return read_remote_binlog_cli(
            source, start, max_seconds, max_events, on_row, should_stop, on_tx, on_query);
    };

    if (mode == "cli") {
        return run_cli();
    }

    if (mode == "native") {
        return read_remote_binlog_native(
            source, start, max_seconds, max_events, on_row, should_stop, on_tx);
    }

    if (mariadb_binlog_native_available() && !g_native_auto_failed.load()) {
        try {
            return read_remote_binlog_native(
                source, start, max_seconds, max_events, on_row, should_stop, on_tx);
        } catch (const std::exception&) {
            g_native_auto_failed.store(true);
        }
    }
    return run_cli();
}
