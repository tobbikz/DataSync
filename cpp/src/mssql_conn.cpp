#include "mssql_conn.hpp"

#include "pipeline_defaults.hpp"

#include <algorithm>
#include <cctype>
#include <climits>
#include <mutex>
#include <sstream>
#include <thread>
#include <unistd.h>

std::string sanitize_mssql_text_for_pg(const std::string& in) {
    auto append_codepoint = [](std::string& out, char32_t cp) {
        if (cp <= 0x7F) {
            out.push_back(static_cast<char>(cp));
        } else if (cp <= 0x7FF) {
            out.push_back(static_cast<char>(0xC0 | ((cp >> 6) & 0x1F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else if (cp <= 0xFFFF) {
            out.push_back(static_cast<char>(0xE0 | ((cp >> 12) & 0x0F)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else {
            out.push_back(static_cast<char>(0xF0 | ((cp >> 18) & 0x07)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        }
    };

    std::string out;
    out.reserve(in.size());
    for (std::size_t i = 0; i < in.size();) {
        const unsigned char b = static_cast<unsigned char>(in[i]);
        if (b == 0x00) {
            ++i;
            continue;
        }
        if (b < 0x80) {
            out.push_back(static_cast<char>(b));
            ++i;
            continue;
        }
        int seq_len = 0;
        if ((b & 0xE0) == 0xC0) {
            seq_len = 2;
        } else if ((b & 0xF0) == 0xE0) {
            seq_len = 3;
        } else if ((b & 0xF8) == 0xF0) {
            seq_len = 4;
        }
        if (seq_len > 0 && i + static_cast<std::size_t>(seq_len) <= in.size()) {
            bool valid = true;
            for (int j = 1; j < seq_len; ++j) {
                if ((static_cast<unsigned char>(in[i + static_cast<std::size_t>(j)]) & 0xC0) != 0x80) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                out.append(in, i, static_cast<std::size_t>(seq_len));
                i += static_cast<std::size_t>(seq_len);
                continue;
            }
        }
        append_codepoint(out, static_cast<char32_t>(b));
        ++i;
    }
    return out;
}

std::string trim_mssql_text(std::string value) {
    while (!value.empty() && (value.back() == ' ' || value.back() == '\0')) {
        value.pop_back();
    }
    std::size_t start = 0;
    while (start < value.size() && value[start] == ' ') {
        ++start;
    }
    if (start > 0) {
        value.erase(0, start);
    }
    return value;
}

const MssqlSource* find_mssql_source(const AppConfig& cfg, const std::string& conn_id) {
    for (const auto& m : cfg.mssql_sources) {
        if (m.conn_id == conn_id) {
            return &m;
        }
    }
    return nullptr;
}

#ifdef HAVE_FREETDS

namespace {

thread_local std::string g_last_mssql_dblib_error;

std::once_flag g_dbinit_once;

void ensure_db_library_init() {
    std::call_once(g_dbinit_once, [] {
        if (dbinit() == FAIL) {
            throw std::runtime_error("dbinit failed");
        }
        dberrhandle(
            [](DBPROCESS*, int, int, int, char* dberrstr, char*) -> int {
                if (dberrstr && dberrstr[0]) {
                    g_last_mssql_dblib_error = dberrstr;
                }
                return INT_CANCEL;
            });
    });
}

std::string format_dberror(DBPROCESS* db) {
    if (!g_last_mssql_dblib_error.empty()) {
        return g_last_mssql_dblib_error;
    }
    if (!db) {
        return "null DBPROCESS";
    }
    char err_buf[1024];
    err_buf[0] = '\0';
    const int len = dbstrlen(db);
    if (len > 0) {
        dbstrcpy(db, 1, std::min(len, static_cast<int>(sizeof(err_buf) - 1)), err_buf);
        if (err_buf[0]) {
            return err_buf;
        }
    }
    return "unknown DB-Library error";
}

bool run_dbsql_impl(DBPROCESS* db, const std::string& sql) {
    g_last_mssql_dblib_error.clear();
    if (dbcmd(db, sql.c_str()) == FAIL) {
        throw std::runtime_error("MSSQL dbcmd failed: " + format_dberror(db));
    }
    if (dbsqlexec(db) == FAIL) {
        throw std::runtime_error("MSSQL dbsqlexec failed: " + format_dberror(db));
    }
    return true;
}

void set_mssql_login_port(LOGINREC* login, std::uint16_t port) {
#if defined(DBSETLPORT)
    DBSETLPORT(login, static_cast<int>(port));
#elif defined(DBSETPORT)
    dbsetlshort(login, static_cast<int>(port), DBSETPORT);
#else
    (void)login;
    (void)port;
#endif
}

void set_mssql_login_encryption(LOGINREC* login) {
    DBSETLENCRYPT(login, TRUE);
#if defined(DBSETLENCRYPTION)
    DBSETLENCRYPTION(login, "require");
#endif
}

DBPROCESS* open_mssql_handle(const MssqlSource& src) {
    ensure_db_library_init();

    LOGINREC* login = dblogin();
    if (!login) {
        throw std::runtime_error("dblogin failed");
    }
    DBSETLUSER(login, src.user.c_str());
    DBSETLPWD(login, src.password.c_str());
    DBSETLHOST(login, src.host.c_str());
    DBSETLAPP(login, "DataSync");
    set_mssql_login_encryption(login);
    set_mssql_login_port(login, src.port);
    DBSETLVERSION(login, DBVERSION_74);

    DBPROCESS* db = dbopen(login, src.host.c_str());
    dbloginfree(login);
    if (!db) {
        const std::string detail =
            g_last_mssql_dblib_error.empty() ? "dbopen returned null" : g_last_mssql_dblib_error;
        throw std::runtime_error(
            "MSSQL connect failed conn_id=" + src.conn_id + " server=" + src.host + ":" +
            std::to_string(src.port) + " — " + detail);
    }
    return db;
}

MssqlQueryResult mssql_query_impl(DBPROCESS* handle, const std::string& sql) {
    run_dbsql_impl(handle, sql);
    if (dbresults(handle) == FAIL) {
        throw std::runtime_error("MSSQL query dbresults failed: " + format_dberror(handle));
    }

    MssqlQueryResult out;
    const int ncols = dbnumcols(handle);
    out.columns.reserve(static_cast<std::size_t>(ncols));
    for (int col = 1; col <= ncols; ++col) {
        const char* name = dbcolname(handle, col);
        out.columns.push_back(name ? trim_mssql_text(name) : "");
    }

    while (true) {
        const int rc = dbnextrow(handle);
        if (rc == NO_MORE_ROWS) {
            break;
        }
        if (rc == FAIL) {
            throw std::runtime_error("MSSQL query dbnextrow failed: " + format_dberror(handle));
        }
        MssqlRow row;
        row.reserve(static_cast<std::size_t>(ncols));
        for (int col = 1; col <= ncols; ++col) {
            MssqlCell cell;
            if (!dbdata(handle, col)) {
                row.push_back(cell);
                continue;
            }
            const int col_type = dbcoltype(handle, col);
            if (col_type == SYBBINARY || col_type == SYBVARBINARY || col_type == SYBIMAGE) {
                const char* data = reinterpret_cast<const char*>(dbdata(handle, col));
                const DBINT len = dbdatlen(handle, col);
                if (data && len > 0) {
                    cell.bytes.assign(data, data + len);
                    cell.is_binary = true;
                }
            } else {
                char buf[4096];
                mssql_cell_to_char(handle, col, buf, static_cast<DBINT>(sizeof(buf) - 1));
                buf[sizeof(buf) - 1] = '\0';
                cell.text = trim_mssql_text(buf);
            }
            row.push_back(std::move(cell));
        }
        out.rows.push_back(std::move(row));
    }
    while (true) {
        const int r = dbresults(handle);
        if (r == NO_MORE_RESULTS) {
            break;
        }
        if (r == FAIL) {
            throw std::runtime_error("MSSQL query drain failed: " + format_dberror(handle));
        }
    }
    return out;
}

int mssql_retry_attempt_limit(int configured) {
    return configured <= 0 ? INT_MAX : std::max(1, configured);
}

bool mssql_error_is_transient(const std::string& err) {
    std::string lower = err;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
    return lower.find("timeout") != std::string::npos || lower.find("timed out") != std::string::npos ||
           lower.find("connection") != std::string::npos || lower.find("communication link") != std::string::npos ||
           lower.find("broken pipe") != std::string::npos || lower.find("dead") != std::string::npos ||
           lower.find("network") != std::string::npos || lower.find("dbprocess is dead") != std::string::npos ||
           lower.find("dbprocess dead") != std::string::npos;
}

}  // namespace

void mssql_drain_results(DBPROCESS* db) {
    if (!db) {
        return;
    }
    while (true) {
        const int rc = dbresults(db);
        if (rc == NO_MORE_RESULTS) {
            break;
        }
        if (rc == FAIL) {
            break;
        }
        while (dbnextrow(db) != NO_MORE_ROWS) {
        }
    }
}

void mssql_sleep_retry_backoff(int attempt, const MssqlRetryOptions& opts) {
    const int capped = std::min(attempt, 10);
    const int delay_ms = std::min(opts.max_ms, opts.base_ms * (1 << capped));
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
}

bool mssql_run_dbsql_retry(MssqlConn& conn, const std::string& sql, const MssqlRetryOptions& opts) {
    const int attempts = mssql_retry_attempt_limit(opts.max_attempts);
    for (int attempt = 0; attempt < attempts; ++attempt) {
        try {
            return run_dbsql_impl(conn.handle, sql);
        } catch (const std::exception& ex) {
            mssql_drain_results(conn.handle);
            if (!mssql_error_is_transient(ex.what()) || attempt + 1 >= attempts) {
                throw;
            }
            conn.reconnect();
            mssql_sleep_retry_backoff(attempt, opts);
        }
    }
    return false;
}

bool run_dbsql(DBPROCESS* db, const std::string& sql) {
    return run_dbsql_impl(db, sql);
}

MssqlConn::MssqlConn(const MssqlSource& src) : source_(src) {
    handle = open_mssql_handle(source_);
}

MssqlConn::~MssqlConn() {
    if (handle) {
        dbclose(handle);
        handle = nullptr;
    }
}

void MssqlConn::reconnect() {
    if (handle) {
        dbclose(handle);
        handle = nullptr;
    }
    handle = open_mssql_handle(source_);
    if (!current_database_.empty()) {
        use_database(current_database_);
    }
}

void MssqlConn::use_database(const std::string& database) {
    const std::string db = trim_mssql_text(database);
    if (dbuse(handle, db.c_str()) == FAIL) {
        throw std::runtime_error("MSSQL USE failed: " + db + " — " + format_dberror(handle));
    }
    current_database_ = db;
}

void MssqlConn::exec(const std::string& sql) {
    run_dbsql(handle, sql);
    mssql_drain_results(handle);
}

MssqlQueryResult MssqlConn::query(const std::string& sql) {
    return mssql_query_impl(handle, sql);
}

MssqlQueryResult MssqlConn::query_retry(const std::string& sql, const MssqlRetryOptions& opts) {
    const int attempts = mssql_retry_attempt_limit(opts.max_attempts);
    for (int attempt = 0; attempt < attempts; ++attempt) {
        try {
            return mssql_query_impl(handle, sql);
        } catch (const std::exception& ex) {
            mssql_drain_results(handle);
            if (!mssql_error_is_transient(ex.what()) || attempt + 1 >= attempts) {
                throw;
            }
            reconnect();
            mssql_sleep_retry_backoff(attempt, opts);
        }
    }
    throw std::runtime_error("MSSQL query_retry exhausted attempts");
}

#endif
