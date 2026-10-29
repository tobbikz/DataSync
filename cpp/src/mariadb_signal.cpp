#include "mariadb_signal.hpp"

#include <stdexcept>
#include <string>

namespace mariadb_signal {

void ensure_signal_table(MariaDbConn& conn) {
    const MariaDbRetryOptions retry{};
    mariadb_mysql_query_retry(
        conn,
        "CREATE TABLE IF NOT EXISTS `datasync_signal` ("
        "  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
        "  catalog_key VARCHAR(512) NOT NULL,"
        "  kind VARCHAR(16) NOT NULL,"
        "  gtid_executed TEXT,"
        "  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),"
        "  KEY idx_datasync_signal_key_kind (catalog_key, kind, id)"
        ") ENGINE=InnoDB",
        retry);
}

long long insert_watermark(
    MariaDbConn& conn,
    const std::string& catalog_key,
    const std::string& kind,
    const std::string& gtid_executed) {
    const MariaDbRetryOptions retry{};
    // Escape single quotes simply for watermark metadata (keys are catalog ids).
    auto esc = [](const std::string& s) {
        std::string o;
        o.reserve(s.size() + 8);
        for (char c : s) {
            if (c == '\\' || c == '\'') {
                o.push_back('\\');
            }
            o.push_back(c);
        }
        return o;
    };
    const std::string sql =
        "INSERT INTO `datasync_signal` (catalog_key, kind, gtid_executed) VALUES ('" + esc(catalog_key) +
        "','" + esc(kind) + "','" + esc(gtid_executed) + "')";
    mariadb_mysql_query_retry(conn, sql, retry);
    return static_cast<long long>(mysql_insert_id(conn.handle));
}

std::string read_gtid_executed(MariaDbConn& conn) {
    const MariaDbRetryOptions retry{};
    // Prefer gtid_binlog_pos (MariaDB); fall back to gtid_current_pos / empty.
    MYSQL_RES* res = nullptr;
    try {
        res = mariadb_mysql_query_store_retry(conn, "SELECT @@GLOBAL.gtid_binlog_pos", retry);
    } catch (...) {
        res = nullptr;
    }
    if (!res) {
        try {
            res = mariadb_mysql_query_store_retry(conn, "SELECT @@GLOBAL.gtid_current_pos", retry);
        } catch (...) {
            return {};
        }
    }
    std::string out;
    if (MYSQL_ROW row = mysql_fetch_row(res)) {
        if (row[0]) {
            out = row[0];
        }
    }
    mysql_free_result(res);
    return out;
}

}  // namespace mariadb_signal
