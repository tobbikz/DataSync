#include "mariadb_binlog_native.hpp"

#include "mariadb_conn.hpp"

#include <chrono>
#include <stdexcept>
#include <string>

#ifdef HAVE_MARIADB_RPL
#include <mysql/mysql.h>
#if __has_include(<mysql/mariadb_rpl.h>)
#include <mysql/mariadb_rpl.h>
#elif __has_include(<mariadb/mariadb_rpl.h>)
#include <mariadb/mariadb_rpl.h>
#else
#undef HAVE_MARIADB_RPL
#endif
#endif

bool mariadb_binlog_native_available() {
#ifdef HAVE_MARIADB_RPL
    return true;
#else
    return false;
#endif
}

#ifdef HAVE_MARIADB_RPL

BinlogCliStats read_remote_binlog_native(
    const MariaDbSource& source,
    const BinlogPosition& start,
    int max_seconds,
    int max_events,
    const BinlogRowHandler& on_row,
    const std::function<bool()>& should_stop,
    const BinlogTxHandler& on_tx) {
    (void)on_row;
    (void)max_events;

    BinlogCliStats stats;
    stats.last_file = start.file;
    stats.last_position = start.position;

    if (start.file.empty() || start.position <= 0) {
        throw std::runtime_error("native binlog: invalid start position");
    }

    MariaDbConn mariadb(source);
    if (!mariadb.handle) {
        throw std::runtime_error("native binlog: MariaDB connect failed");
    }

    MARIADB_RPL* rpl = mariadb_rpl_init(mariadb.handle);
    if (!rpl) {
        throw std::runtime_error("native binlog: mariadb_rpl_init failed");
    }

    if (mariadb_rpl_optionsv(rpl, MARIADB_RPL_FILENAME, start.file.c_str()) ||
        mariadb_rpl_optionsv(
            rpl, MARIADB_RPL_START_POSITION, static_cast<unsigned long>(start.position)) ||
        mariadb_rpl_optionsv(rpl, MARIADB_RPL_SERVER_ID, static_cast<unsigned long>(0xDABA))) {
        const std::string err = mysql_error(mariadb.handle);
        mariadb_rpl_free(rpl);
        throw std::runtime_error("native binlog: rpl options failed: " + err);
    }
    if (mariadb_rpl_open(rpl) != 0) {
        const std::string err = mysql_error(mariadb.handle);
        mariadb_rpl_free(rpl);
        throw std::runtime_error("native binlog: rpl open failed: " + err);
    }

    // S09: drain QUERY / tx control events natively; hand off to CLI when row images appear
    // (portable row-image decode is still Connector/C + MariaDB 12 fragile).
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(std::max(1, max_seconds));
    bool saw_rows = false;
    while (std::chrono::steady_clock::now() < deadline) {
        if (should_stop && should_stop()) {
            break;
        }
        MARIADB_RPL_EVENT* ev = mariadb_rpl_fetch(rpl, nullptr);
        if (!ev) {
            break;
        }
        if (ev->next_event_pos > 0) {
            stats.last_position = static_cast<long long>(ev->next_event_pos);
        }
        const auto et = ev->event_type;
        if (et == QUERY_EVENT) {
            if (on_tx) {
                // Statement text is version-specific; still advance cursor for tx markers.
            }
        } else if (
            et == WRITE_ROWS_EVENT_V1 || et == UPDATE_ROWS_EVENT_V1 || et == DELETE_ROWS_EVENT_V1 ||
            et == WRITE_ROWS_EVENT || et == UPDATE_ROWS_EVENT || et == DELETE_ROWS_EVENT) {
            saw_rows = true;
            mariadb_rpl_free_event(ev);
            break;
        }
        mariadb_rpl_free_event(ev);
    }
    mariadb_rpl_free(rpl);

    if (saw_rows) {
        throw std::runtime_error(
            "native binlog: row events require CLI decode under binlog_mode=auto");
    }
    return stats;
}

#else

BinlogCliStats read_remote_binlog_native(
    const MariaDbSource&,
    const BinlogPosition&,
    int,
    int,
    const BinlogRowHandler&,
    const std::function<bool()>&,
    const BinlogTxHandler&) {
    throw std::runtime_error(
        "native binlog unavailable: rebuild with mariadb_rpl.h (HAVE_MARIADB_RPL)");
}

#endif
