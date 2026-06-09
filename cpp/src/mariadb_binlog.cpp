#include "mariadb_binlog.hpp"

#include "mariadb_schema.hpp"

#include <optional>
#include <stdexcept>
#include <vector>

namespace {

std::string mariadb_scalar_safe(MYSQL* mysql, const char* sql) {
    if (mysql_query(mysql, sql) != 0) {
        return {};
    }
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return {};
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    std::string out;
    if (row && row[0]) {
        out = row[0];
    }
    mysql_free_result(res);
    return out;
}

struct MasterStatus {
    std::string binlog_file;
    long long binlog_position{0};
    std::string gtid;
};

MasterStatus read_master_status(MYSQL* mysql) {
    if (mysql_query(mysql, "SHOW MASTER STATUS") != 0) {
        throw std::runtime_error(std::string("SHOW MASTER STATUS failed: ") + mysql_error(mysql));
    }

    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        throw std::runtime_error(std::string("SHOW MASTER STATUS store_result failed: ") + mysql_error(mysql));
    }

    MYSQL_ROW row = mysql_fetch_row(res);
    if (!row || !row[0]) {
        mysql_free_result(res);
        throw std::runtime_error("SHOW MASTER STATUS returned no row (binlog disabled?)");
    }

    MasterStatus out;
    out.binlog_file = row[0];
    out.binlog_position = row[1] ? std::atoll(row[1]) : 0;
    if (row[2]) {
        out.gtid = row[2];
    }
    mysql_free_result(res);

    if (out.gtid.empty()) {
        out.gtid = mariadb_scalar_safe(mysql, "SELECT @@gtid_binlog_state");
    }
    return out;
}

void upsert_capture_position_full(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& gtid_set,
    const std::string& binlog_file,
    long long binlog_position,
    const std::string& server_uuid) {
    const std::string pos_str = std::to_string(binlog_position);
    const char* vals[] = {
        conn_id.c_str(),
        gtid_set.c_str(),
        binlog_file.c_str(),
        pos_str.c_str(),
        server_uuid.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        INSERT INTO cdc_catalog.capture_position
            (conn_id, gtid_set, binlog_file, binlog_position, server_uuid, status, last_error, updated_at)
        VALUES ($1, $2, $3, $4::bigint, $5, 'healthy', NULL, now())
        ON CONFLICT (conn_id) DO UPDATE SET
            gtid_set = EXCLUDED.gtid_set,
            binlog_file = EXCLUDED.binlog_file,
            binlog_position = EXCLUDED.binlog_position,
            server_uuid = EXCLUDED.server_uuid,
            status = 'healthy'::cdc_catalog.cdc_health_status,
            last_error = NULL,
            updated_at = now()
        )",
        5,
        vals);
}

}  // namespace

bool capture_binlog_position_t0_if_absent(PGconn* pg, MYSQL* mysql, const std::string& conn_id) {
    const char* vals[] = {conn_id.c_str()};
    PGresult* chk = PQexecParams(
        pg,
        R"(
        SELECT 1 FROM cdc_catalog.capture_position
        WHERE conn_id = $1
          AND binlog_file IS NOT NULL
          AND length(trim(binlog_file)) > 0
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!chk || PQresultStatus(chk) != PGRES_TUPLES_OK) {
        if (chk) {
            PQclear(chk);
        }
        throw std::runtime_error("failed to read capture_position");
    }
    const bool exists = PQntuples(chk) > 0;
    PQclear(chk);
    if (exists) {
        return false;
    }
    capture_binlog_position_t0(pg, mysql, conn_id);
    return true;
}

void capture_binlog_position_t0(PGconn* pg, MYSQL* mysql, const std::string& conn_id) {
    const MasterStatus master = read_master_status(mysql);
    std::string uuid = mariadb_scalar_safe(mysql, "SELECT @@server_uuid");
    if (uuid.empty()) {
        uuid = mariadb_scalar_safe(mysql, "SELECT @@server_uid");
    }
    upsert_capture_position_full(
        pg, conn_id, master.gtid, master.binlog_file, master.binlog_position, uuid);
}

std::optional<BinlogPosition> fetch_binlog_position(PGconn* pg, const std::string& conn_id) {
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT binlog_file, binlog_position
        FROM cdc_catalog.capture_position
        WHERE conn_id = $1
          AND binlog_file IS NOT NULL
          AND length(trim(binlog_file)) > 0
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("failed to read capture_position");
    }

    if (PQntuples(res) == 0) {
        PQclear(res);
        return std::nullopt;
    }

    BinlogPosition pos;
    pos.file = PQgetvalue(res, 0, 0);
    pos.position = std::atoll(PQgetvalue(res, 0, 1));
    PQclear(res);
    return pos;
}

void upsert_binlog_position(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& binlog_file,
    long long binlog_position) {
    const std::string pos_str = std::to_string(binlog_position);
    const char* vals[] = {conn_id.c_str(), binlog_file.c_str(), pos_str.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        INSERT INTO cdc_catalog.capture_position
            (conn_id, gtid_set, binlog_file, binlog_position, status, last_event_ts, updated_at)
        VALUES ($1, '', $2, $3::bigint, 'healthy', now(), now())
        ON CONFLICT (conn_id) DO UPDATE SET
            binlog_file = EXCLUDED.binlog_file,
            binlog_position = EXCLUDED.binlog_position,
            last_event_ts = now(),
            updated_at = now()
        )",
        3,
        vals);
}

std::optional<long long> binlog_file_sequence_number(const std::string& filename) {
    const auto dot = filename.rfind('.');
    if (dot == std::string::npos || dot + 1 >= filename.size()) {
        return std::nullopt;
    }
    try {
        return std::stoll(filename.substr(dot + 1));
    } catch (...) {
        return std::nullopt;
    }
}

std::vector<BinlogFileEntry> fetch_binary_logs(MYSQL* mysql) {
    std::vector<BinlogFileEntry> out;
    if (!mysql || mysql_query(mysql, "SHOW BINARY LOGS") != 0) {
        return out;
    }
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return out;
    }
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        if (!row[0]) {
            continue;
        }
        BinlogFileEntry entry;
        entry.name = row[0];
        entry.file_size = row[1] ? std::atoll(row[1]) : 0;
        out.push_back(std::move(entry));
    }
    mysql_free_result(res);
    return out;
}

MasterBinlogStatus fetch_master_binlog_status(MYSQL* mysql) {
    MasterBinlogStatus out;
    if (!mysql || mysql_query(mysql, "SHOW MASTER STATUS") != 0) {
        return out;
    }
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return out;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row && row[0]) {
        out.file = row[0];
        out.position = row[1] ? std::atoll(row[1]) : 0;
    }
    mysql_free_result(res);
    return out;
}

bool binlog_position_caught_up(const BinlogPosition& cursor, const MasterBinlogStatus& master) {
    if (master.file.empty()) {
        return false;
    }
    if (cursor.file == master.file) {
        return cursor.position >= master.position;
    }
    const auto cur_seq = binlog_file_sequence_number(cursor.file);
    const auto master_seq = binlog_file_sequence_number(master.file);
    if (cur_seq && master_seq) {
        return *cur_seq > *master_seq ||
               (*cur_seq == *master_seq && cursor.position >= master.position);
    }
    return false;
}

bool binlog_cursor_is_behind(const BinlogPosition& cursor, const MasterBinlogStatus& master) {
    if (master.file.empty()) {
        return false;
    }
    if (cursor.file == master.file) {
        return cursor.position < master.position;
    }
    const auto cur_seq = binlog_file_sequence_number(cursor.file);
    const auto master_seq = binlog_file_sequence_number(master.file);
    if (cur_seq && master_seq) {
        return *cur_seq < *master_seq;
    }
    return cursor.file < master.file;
}

bool advance_binlog_cursor_to_next_file(MYSQL* mysql, BinlogPosition& cursor) {
    const auto logs = fetch_binary_logs(mysql);
    if (logs.empty()) {
        return false;
    }

    int current_idx = -1;
    for (std::size_t i = 0; i < logs.size(); ++i) {
        if (logs[i].name == cursor.file) {
            current_idx = static_cast<int>(i);
            break;
        }
    }
    if (current_idx < 0) {
        throw std::runtime_error(
            "binlog file no longer on server (purged?): " + cursor.file +
            " — run recovery or re-seed capture_position from SHOW MASTER STATUS");
    }

    const long long file_size = logs[static_cast<std::size_t>(current_idx)].file_size;
    const bool at_or_past_eof = file_size > 0 && cursor.position >= file_size;

    if (!at_or_past_eof) {
        return false;
    }

    if (current_idx + 1 >= static_cast<int>(logs.size())) {
        return false;
    }

    cursor.file = logs[static_cast<std::size_t>(current_idx + 1)].name;
    cursor.position = 4;
    return true;
}
