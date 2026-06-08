#include "mssql_conn.hpp"

#include <sstream>

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

void ensure_db_library_init() {
    static bool initialized = false;
    if (initialized) {
        return;
    }
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
    initialized = true;
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

bool run_dbsql(DBPROCESS* db, const std::string& sql) {
    g_last_mssql_dblib_error.clear();
    if (dbfcmd(db, "%s", sql.c_str()) == FAIL) {
        throw std::runtime_error("MSSQL dbcmd failed: " + format_dberror(db));
    }
    if (dbsqlexec(db) == FAIL) {
        throw std::runtime_error("MSSQL dbsqlexec failed: " + format_dberror(db));
    }
    return true;
}

}  // namespace

MssqlConn::MssqlConn(const MssqlSource& src) {
    ensure_db_library_init();

    LOGINREC* login = dblogin();
    if (!login) {
        throw std::runtime_error("dblogin failed");
    }
    DBSETLUSER(login, src.user.c_str());
    DBSETLPWD(login, src.password.c_str());
    DBSETLHOST(login, src.host.c_str());
    DBSETLAPP(login, "DataSync");
    DBSETLENCRYPT(login, FALSE);
    DBSETLVERSION(login, DBVERSION_74);
#ifdef DBSETLPORT
    DBSETLPORT(login, static_cast<int>(src.port));
#endif

    const std::string server =
#ifdef DBSETLPORT
        src.host;
#else
        src.host + "," + std::to_string(src.port);
#endif
    handle = dbopen(login, server.c_str());
    dbloginfree(login);
    if (!handle) {
        throw std::runtime_error(
            "MSSQL connect failed conn_id=" + src.conn_id + " server=" + src.host + ":" +
            std::to_string(src.port));
    }
}

MssqlConn::~MssqlConn() {
    if (handle) {
        dbclose(handle);
    }
}

void MssqlConn::use_database(const std::string& database) {
    const std::string db = trim_mssql_text(database);
    if (dbuse(handle, db.c_str()) == FAIL) {
        throw std::runtime_error("MSSQL USE failed: " + db + " — " + format_dberror(handle));
    }
}

void MssqlConn::exec(const std::string& sql) {
    run_dbsql(handle, sql);
    while (dbresults(handle) != NO_MORE_RESULTS) {
    }
}

MssqlQueryResult MssqlConn::query(const std::string& sql) {
    run_dbsql(handle, sql);
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
    while (dbresults(handle) != NO_MORE_RESULTS) {
    }
    return out;
}

#endif
