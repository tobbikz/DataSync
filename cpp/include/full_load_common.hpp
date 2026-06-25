#pragma once

#include "obs_log.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <mutex>
#include <string>
#include <string_view>

namespace full_load {

long long elapsed_ms(const std::chrono::steady_clock::time_point& start);
std::string utc_now_ts();
std::string utc_now_date();
std::string csv_escape(const std::string& value);

long long lake_table_row_count(PGconn* pg, const std::string& schema, const std::string& table);
void acquire_full_load_table_lock(PGconn* pg, long long catalog_id);
void release_full_load_table_lock(PGconn* pg, long long catalog_id);

void log(
    PGconn* log_pg,
    std::mutex* log_mtx,
    std::string_view component,
    LogLevel level,
    const std::string& batch_id,
    const std::string& message,
    const nlohmann::json& context = {},
    const std::string& conn_id = {},
    const std::string& schema = {},
    const std::string& table = {});

}  // namespace full_load
