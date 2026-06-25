#pragma once

#include "config.hpp"

#ifdef HAVE_FREETDS
#include <sybdb.h>
#endif

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

struct MssqlRetryOptions {
    int max_attempts{0};
    int base_ms{500};
    int max_ms{60000};
};

#ifdef HAVE_FREETDS

struct MssqlCell {
    std::string text;
    std::vector<uint8_t> bytes;
    bool is_binary{false};
};

using MssqlRow = std::vector<MssqlCell>;
using MssqlResult = std::vector<MssqlRow>;

struct MssqlQueryResult {
    std::vector<std::string> columns;
    MssqlResult rows;
};

struct MssqlConn {
    DBPROCESS* handle{nullptr};

    explicit MssqlConn(const MssqlSource& src);
    ~MssqlConn();

    MssqlConn(const MssqlConn&) = delete;
    MssqlConn& operator=(const MssqlConn&) = delete;

    void reconnect();
    void use_database(const std::string& database);
    void exec(const std::string& sql);
    MssqlQueryResult query(const std::string& sql);
    MssqlQueryResult query_retry(const std::string& sql, const MssqlRetryOptions& opts);

  private:
    MssqlSource source_;
    std::string current_database_;
};

void mssql_sleep_retry_backoff(int attempt, const MssqlRetryOptions& opts);
void mssql_drain_results(DBPROCESS* db);
bool mssql_run_dbsql_retry(MssqlConn& conn, const std::string& sql, const MssqlRetryOptions& opts);

/** Execute literal SQL via dbcmd + dbsqlexec (safe when SQL contains '%'). */
bool run_dbsql(DBPROCESS* db, const std::string& sql);

/** Make FreeTDS/MSSQL text safe for PostgreSQL UTF-8 COPY (Latin-1 uplift + valid UTF-8 pass-through). */
std::string sanitize_mssql_text_for_pg(const std::string& value);

/** FreeTDS dbconvert: srctype + srclen + desttype + dest + destlen (7 args). */
inline void mssql_cell_to_char(
    DBPROCESS* db, int col, char* buf, DBINT buf_len) {
    const int col_type = dbcoltype(db, col);
    const DBINT srclen = dbdatlen(db, col);
    dbconvert(
        db,
        col_type,
        dbdata(db, col),
        srclen,
        SYBCHAR,
        reinterpret_cast<BYTE*>(buf),
        buf_len);
}

#endif

std::string trim_mssql_text(std::string value);

const MssqlSource* find_mssql_source(const AppConfig& cfg, const std::string& conn_id);
