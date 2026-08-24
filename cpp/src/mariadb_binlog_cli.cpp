#include "mariadb_binlog_cli.hpp"

#include "mariadb_binlog.hpp"
#include "mariadb_datetime.hpp"

#include <chrono>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <map>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace {

std::string shell_single_quote(const std::string& value) {
    std::string out = "'";
    for (char c : value) {
        if (c == '\'') {
            out += "'\\''";
        } else {
            out += c;
        }
    }
    out += "'";
    return out;
}

std::string trim(std::string s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
        s.erase(s.begin());
    }
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
        s.pop_back();
    }
    return s;
}

bool parse_table_ref(const std::string& line, std::string& schema, std::string& table) {
    static const std::regex re(R"(`(.*?)`\.`(.*?)`)");
    std::smatch m;
    if (!std::regex_search(line, m, re)) {
        return false;
    }
    schema = m[1].str();
    table = m[2].str();
    return true;
}

bool parse_at_column(const std::string& line, int& index, std::string& value) {
    static const std::regex re(R"(@(\d+)=(.*))");
    std::smatch m;
    if (!std::regex_search(line, m, re)) {
        return false;
    }
    index = std::stoi(m[1].str()) - 1;
    value = trim(m[2].str());
    return true;
}

std::string sql_literal_from_binlog_value(const std::string& raw) {
    if (raw == "NULL") {
        return "NULL";
    }
    if (raw.size() >= 2 && raw.front() == '\'' && raw.back() == '\'') {
        const std::string inner = raw.substr(1, raw.size() - 2);
        const std::string fixed = fix_date_separators(inner);
        if (fixed != inner) {
            return pg_escape_literal(fixed);
        }
        return raw;
    }
    return raw;
}

std::string read_file_tail(const std::string& path, std::size_t max_chars) {
    std::ifstream in(path);
    if (!in) {
        return {};
    }
    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (content.size() <= max_chars) {
        return content;
    }
    return content.substr(content.size() - max_chars);
}

bool path_executable(const std::string& path) {
    return !path.empty() && access(path.c_str(), X_OK) == 0;
}

}  // namespace

std::string find_mariadb_binlog_binary() {
    if (const char* override_path = std::getenv("MARIADB_BINLOG_PATH")) {
        const std::string custom(override_path);
        if (path_executable(custom)) {
            return custom;
        }
    }
    for (const char* name : {"mariadb-binlog", "mysqlbinlog"}) {
        FILE* which = popen(("command -v " + std::string(name) + " 2>/dev/null").c_str(), "r");
        if (!which) {
            continue;
        }
        char buf[512];
        std::string path;
        if (fgets(buf, sizeof(buf), which) != nullptr) {
            path = trim(buf);
        }
        pclose(which);
        if (path_executable(path)) {
            return path;
        }
    }
    return {};
}

BinlogCliStats read_remote_binlog_cli(
    const MariaDbSource& source,
    const BinlogPosition& start,
    int max_seconds,
    int max_events,
    const BinlogRowHandler& on_row,
    const std::function<bool()>& should_stop,
    const BinlogTxHandler& on_tx) {
    BinlogCliStats stats;
    stats.last_file = start.file;
    stats.last_position = start.position;

    const std::string binlog_bin = find_mariadb_binlog_binary();
    if (binlog_bin.empty()) {
        stats.cli_missing = true;
        stats.stderr_tail =
            "mariadb-binlog not found in PATH (install mariadb-client or set MARIADB_BINLOG_PATH)";
        return stats;
    }

    const char* tmpdir = std::getenv("TMPDIR");
    const std::string tmp_prefix = tmpdir ? (std::string(tmpdir) + "/datasync_binlog_err_XXXXXX") : "/tmp/datasync_binlog_err_XXXXXX";
    char err_path_template[1024];
    std::strncpy(err_path_template, tmp_prefix.c_str(), sizeof(err_path_template) - 1);
    err_path_template[sizeof(err_path_template) - 1] = '\0';
    const int err_fd = mkstemp(err_path_template);
    if (err_fd == -1) {
        throw std::runtime_error("failed to create temp file for binlog stderr");
    }
    close(err_fd);
    const std::string err_path(err_path_template);

    const bool binlog_ssl =
        std::getenv("MARIADB_BINLOG_SSL") != nullptr && std::getenv("MARIADB_BINLOG_SSL")[0] == '1';

    std::ostringstream cmd;
    cmd << "timeout " << std::max(1, max_seconds) << " " << shell_single_quote(binlog_bin)
        << " --read-from-remote-server --skip-gtid-strict-mode";
    if (!binlog_ssl) {
        cmd << " --skip-ssl";
    }
    cmd << " -h " << shell_single_quote(source.host) << " -P " << source.port << " -u "
        << shell_single_quote(source.user) << " -p" << shell_single_quote(source.password)
        << " --start-position=" << start.position << " --base64-output=DECODE-ROWS -v "
        << shell_single_quote(start.file) << " 2>" << shell_single_quote(err_path);

    FILE* pipe = popen(cmd.str().c_str(), "r");
    if (!pipe) {
        unlink(err_path.c_str());
        throw std::runtime_error("failed to start mariadb-binlog");
    }

    try {
    const auto run_start = std::chrono::steady_clock::now();
    char buffer[8192];
    std::string pending_schema;
    std::string pending_table;
    std::string pending_op;
    bool in_where = false;
    bool in_set = false;
    std::map<int, std::string> where_cols;
    std::map<int, std::string> set_cols;
    std::optional<long long> current_tx_id;

    auto flush_row = [&]() {
        if (pending_schema.empty() || pending_table.empty() || pending_op.empty()) {
            where_cols.clear();
            set_cols.clear();
            in_where = false;
            in_set = false;
            return;
        }

        std::map<int, std::string> chosen = set_cols;
        if (pending_op == "DELETE") {
            chosen = where_cols;
        } else if (pending_op == "UPDATE") {
            // SET carries changed columns only; merge WHERE so PK/unchanged cols are present.
            for (const auto& [idx, val] : where_cols) {
                if (!chosen.count(idx)) {
                    chosen[idx] = val;
                }
            }
        } else if (chosen.empty()) {
            chosen = where_cols;
        }

        if (chosen.empty()) {
            pending_op.clear();
            where_cols.clear();
            set_cols.clear();
            in_where = false;
            in_set = false;
            return;
        }

        const int max_idx = chosen.rbegin()->first;
        std::vector<std::string> values(static_cast<std::size_t>(max_idx + 1));
        for (const auto& [idx, val] : chosen) {
            if (idx >= 0 && idx < static_cast<int>(values.size())) {
                values[static_cast<std::size_t>(idx)] = sql_literal_from_binlog_value(val);
            }
        }

        const std::vector<std::string>* before_values = nullptr;
        std::vector<std::string> before_vec;
        if (pending_op == "UPDATE" && !where_cols.empty()) {
            const int before_max = where_cols.rbegin()->first;
            before_vec.assign(static_cast<std::size_t>(before_max + 1), std::string{});
            for (const auto& [idx, val] : where_cols) {
                if (idx >= 0 && idx < static_cast<int>(before_vec.size())) {
                    before_vec[static_cast<std::size_t>(idx)] = sql_literal_from_binlog_value(val);
                }
            }
            before_values = &before_vec;
        }

        on_row(pending_schema, pending_table, pending_op, values, before_values, stats.last_position, current_tx_id);
        stats.events += 1;
        if (pending_op == "DELETE") {
            stats.deletes += 1;
        } else {
            stats.upserts += 1;
        }

        pending_op.clear();
        where_cols.clear();
        set_cols.clear();
        in_where = false;
        in_set = false;
    };

    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        if (should_stop && should_stop()) {
            break;
        }
        if (stats.events >= max_events) {
            break;
        }
        if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - run_start).count() >=
            max_seconds) {
            break;
        }

        std::string line = trim(buffer);
        if (line.empty()) {
            continue;
        }

        if (line.rfind("# at ", 0) == 0) {
            const std::string pos_str = trim(line.substr(5));
            const auto space = pos_str.find(' ');
            const std::string num = space == std::string::npos ? pos_str : pos_str.substr(0, space);
            try {
                stats.last_position = std::stoll(num);
            } catch (...) {
            }
            continue;
        }

        if (line.rfind("### INSERT INTO ", 0) == 0) {
            flush_row();
            pending_op = "INSERT";
            parse_table_ref(line.substr(16), pending_schema, pending_table);
            in_set = true;
            in_where = false;
            continue;
        }
        if (line.rfind("### UPDATE ", 0) == 0) {
            flush_row();
            pending_op = "UPDATE";
            parse_table_ref(line.substr(11), pending_schema, pending_table);
            in_set = false;
            in_where = false;
            continue;
        }
        if (line.rfind("### DELETE FROM ", 0) == 0) {
            flush_row();
            pending_op = "DELETE";
            parse_table_ref(line.substr(16), pending_schema, pending_table);
            in_set = false;
            in_where = true;
            continue;
        }
        if (line == "### SET") {
            in_set = true;
            in_where = false;
            continue;
        }
        if (line == "### WHERE") {
            in_where = true;
            in_set = false;
            continue;
        }
        if (line.rfind("###   @", 0) == 0) {
            int idx = 0;
            std::string val;
            if (!parse_at_column(line.substr(4), idx, val)) {
                continue;
            }
            if (in_set) {
                set_cols[idx] = val;
            } else if (in_where) {
                where_cols[idx] = val;
            } else if (pending_op == "INSERT") {
                set_cols[idx] = val;
            }
            continue;
        }

        if (line == "BEGIN") {
            if (on_tx) {
                on_tx("begin", current_tx_id.value_or(0LL), stats.last_position);
            }
            continue;
        }
        if (line.rfind("Xid = ", 0) == 0) {
            try {
                const long long xid = std::stoll(trim(line.substr(6)));
                current_tx_id = xid;
                if (on_tx) {
                    on_tx("commit", xid, stats.last_position);
                }
            } catch (...) {
            }
            continue;
        }

        if (line.rfind("###", 0) == 0 && line.find("@") == std::string::npos) {
            flush_row();
        }
    }

    flush_row();
    } catch (...) {
        pclose(pipe);
        unlink(err_path.c_str());
        throw;
    }
    const int pipe_rc = pclose(pipe);
    if (WIFEXITED(pipe_rc)) {
        stats.exit_code = WEXITSTATUS(pipe_rc);
    } else if (WIFSIGNALED(pipe_rc)) {
        stats.exit_code = 128 + WTERMSIG(pipe_rc);
    }
    stats.stderr_tail = read_file_tail(err_path, 1500);
    (void)unlink(err_path.c_str());
    return stats;
}
