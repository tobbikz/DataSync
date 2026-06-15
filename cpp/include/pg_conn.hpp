#pragma once

#include <libpq-fe.h>

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

/** Decode libpq bytea (hex \\x... or raw binary) to bytes. */
inline std::vector<std::uint8_t> pg_bytea_to_bytes(const char* data, int len) {
    if (!data || len <= 0) {
        return {};
    }
    if (len >= 2 && data[0] == '\\' && data[1] == 'x') {
        const int hex_len = len - 2;
        if (hex_len <= 0 || (hex_len % 2) != 0) {
            return {};
        }
        std::vector<std::uint8_t> out;
        out.reserve(static_cast<std::size_t>(hex_len / 2));
        for (int i = 2; i + 1 < len; i += 2) {
            char hex_pair[3] = {data[i], data[i + 1], '\0'};
            out.push_back(static_cast<std::uint8_t>(std::strtoul(hex_pair, nullptr, 16)));
        }
        return out;
    }
    return std::vector<std::uint8_t>(data, data + len);
}

struct PgConn {
    PGconn* raw{nullptr};

    explicit PgConn(const std::string& conninfo) {
        raw = PQconnectdb(conninfo.c_str());
        if (!raw) {
            throw std::runtime_error("PostgreSQL connect failed: PQconnectdb returned null");
        }
        if (PQstatus(raw) != CONNECTION_OK) {
            const std::string err = PQerrorMessage(raw);
            PQfinish(raw);
            throw std::runtime_error("PostgreSQL connect failed: " + err);
        }
        PGresult* tz = PQexec(raw, "SET TIME ZONE 'UTC'");
        if (!tz || PQresultStatus(tz) != PGRES_COMMAND_OK) {
            if (tz) {
                PQclear(tz);
            }
            const std::string err = PQerrorMessage(raw);
            PQfinish(raw);
            throw std::runtime_error("PostgreSQL SET TIME ZONE failed: " + err);
        }
        PQclear(tz);
    }

    ~PgConn() {
        if (raw) {
            PQfinish(raw);
        }
    }

    PgConn(const PgConn&) = delete;
    PgConn& operator=(const PgConn&) = delete;
};

void pg_exec(PGconn* pg, const std::string& sql);
void pg_exec_params_simple(PGconn* pg, const char* sql, int n, const char* const* vals);
