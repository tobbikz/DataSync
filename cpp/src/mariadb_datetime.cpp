#include "mariadb_datetime.hpp"

#include "mariadb_schema.hpp"

#include <cctype>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <unordered_map>

std::string epoch_to_timestamptz(long long secs) {
    const std::time_t t = static_cast<std::time_t>(secs);
    std::tm tm{};
    gmtime_r(&t, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S+00");
    return oss.str();
}

std::string epoch_ms_to_timestamptz(long long ms) {
    const long long sec = ms / 1000;
    const int milli = static_cast<int>(ms >= 0 ? ms % 1000 : -ms % 1000);
    const std::string base = epoch_to_timestamptz(sec);
    const auto plus = base.rfind('+');
    if (plus == std::string::npos) {
        return base;
    }
    std::ostringstream oss;
    oss << base.substr(0, plus) << '.' << std::setfill('0') << std::setw(3) << milli << base.substr(plus);
    return oss.str();
}

std::string epoch_to_date(long long secs) {
    const std::time_t t = static_cast<std::time_t>(secs);
    std::tm tm{};
    gmtime_r(&t, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d");
    return oss.str();
}

bool is_invalid_sql_date(const std::string& s) {
    return s.size() < 10 || s[4] != '-' || s[7] != '-' || s.substr(5, 2) == "00" || s.substr(8, 2) == "00";
}

std::string fix_date_separators(std::string s) {
    if (s.size() >= 10 && s[4] == ':' && s[7] == ':') {
        s[4] = '-';
        s[7] = '-';
    }
    return s;
}

namespace {

int month_from_abbr(const std::string& mon) {
    static const std::unordered_map<std::string, int> kMonths = {
        {"Jan", 1},  {"Feb", 2},  {"Mar", 3},  {"Apr", 4},  {"May", 5},  {"Jun", 6},
        {"Jul", 7},  {"Aug", 8},  {"Sep", 9},  {"Oct", 10}, {"Nov", 11}, {"Dec", 12},
    };
    const auto it = kMonths.find(mon);
    return it == kMonths.end() ? 0 : it->second;
}

bool looks_like_mssql_datetime(const std::string& s) {
    if (s.size() < 12) {
        return false;
    }
    if (s.rfind("AM") == s.size() - 2 || s.rfind("PM") == s.size() - 2) {
        return true;
    }
    return std::isalpha(static_cast<unsigned char>(s[0])) && std::isalpha(static_cast<unsigned char>(s[1])) &&
           std::isalpha(static_cast<unsigned char>(s[2]));
}

}  // namespace

std::string mssql_datetime_to_iso(const std::string& raw) {
    std::string s = raw;
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
        s.erase(s.begin());
    }
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
        s.pop_back();
    }
    if (s.empty()) {
        return s;
    }

    bool has_ampm = false;
    bool pm = false;
    if (s.size() >= 2 && s.back() == 'M') {
        if (s[s.size() - 2] == 'A') {
            has_ampm = true;
            s.resize(s.size() - 2);
        } else if (s[s.size() - 2] == 'P') {
            has_ampm = true;
            pm = true;
            s.resize(s.size() - 2);
        }
        while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
            s.pop_back();
        }
    }

    std::istringstream iss(s);
    std::string mon;
    int day = 0;
    int year = 0;
    iss >> mon >> day >> year;
    if (!iss || month_from_abbr(mon) == 0 || day <= 0 || year <= 0) {
        return raw;
    }

    std::string time_part;
    iss >> time_part;
    if (time_part.empty()) {
        return raw;
    }

    int hour = 0;
    int minute = 0;
    int second = 0;
    long long frac = 0;
    char sep1 = 0;
    char sep2 = 0;
    char sep3 = 0;
    std::istringstream ts(time_part);
    ts >> hour >> sep1 >> minute >> sep2 >> second;
    if (ts >> sep3 >> frac) {
        (void)sep3;
    }
    if (sep1 != ':' || sep2 != ':' || minute < 0 || minute > 59 || second < 0 || second > 59) {
        return raw;
    }
    if (has_ampm) {
        if (hour < 1 || hour > 12) {
            return raw;
        }
        if (hour == 12) {
            hour = pm ? 12 : 0;
        } else if (pm) {
            hour += 12;
        }
    } else if (hour < 0 || hour > 23) {
        return raw;
    }

    const int month = month_from_abbr(mon);
    std::ostringstream out;
    out << std::setfill('0') << year << '-' << std::setw(2) << month << '-' << std::setw(2) << day << ' ' << std::setw(2)
        << hour << ':' << std::setw(2) << minute << ':' << std::setw(2) << second;
    if (frac > 0) {
        out << '.' << frac;
    }
    out << "+00";
    return out.str();
}

std::string normalize_text_for_pg(const std::string& s, const std::string& pg_type) {
    if (pg_type == "DATE") {
        const std::string d = fix_date_separators(s);
        if (is_invalid_sql_date(d)) {
            return {};
        }
        return d;
    }
    if (pg_type == "TIMESTAMPTZ" || pg_type == "TIMESTAMP") {
        std::string t = looks_like_mssql_datetime(s) ? mssql_datetime_to_iso(s) : fix_date_separators(s);
        if (pg_type == "TIMESTAMP") {
            if (const auto pos = t.rfind('+'); pos != std::string::npos) {
                t = t.substr(0, pos);
            }
            while (!t.empty() && std::isspace(static_cast<unsigned char>(t.back()))) {
                t.pop_back();
            }
            return t;
        }
        if (t.find('+') == std::string::npos && t.find('Z') == std::string::npos) {
            t += "+00";
        }
        return t;
    }
    if (pg_type == "BYTEA") {
        return s;
    }
    return sanitize_mariadb_text_for_pg(s);
}

std::string pg_escape_literal(const std::string& value) {
    std::string out = "'";
    for (char c : value) {
        out += (c == '\'') ? "''" : std::string(1, c);
    }
    out += "'";
    return out;
}

std::string normalize_pg_sql_literal(const std::string& sql_lit, const std::string& pg_type) {
    if (sql_lit == "NULL" || sql_lit.empty()) {
        return "NULL";
    }
    if (pg_type != "DATE" && pg_type != "TIMESTAMPTZ" && pg_type != "TIMESTAMP") {
        return sql_lit;
    }
    if (sql_lit.size() < 2 || sql_lit.front() != '\'' || sql_lit.back() != '\'') {
        return sql_lit;
    }
    const std::string inner = sql_lit.substr(1, sql_lit.size() - 2);
    const std::string norm = normalize_text_for_pg(inner, pg_type);
    if (pg_type == "DATE" && norm.empty()) {
        return "NULL";
    }
    return pg_escape_literal(norm);
}
