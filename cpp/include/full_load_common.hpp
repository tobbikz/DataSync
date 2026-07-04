#pragma once

#include "full_load_checkpoint.hpp"
#include "obs_log.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace full_load {

long long elapsed_ms(const std::chrono::steady_clock::time_point& start);
std::string utc_now_ts();
std::string utc_now_date();
std::string csv_escape(const std::string& value);

long long lake_table_row_count(PGconn* pg, const std::string& schema, const std::string& table);

struct TruncateResult {
    bool ok{false};
    long long rows_after{-1};
    int attempts{0};
    std::string error;
};

TruncateResult truncate_lake_table_verified(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    int max_retries = 3);

struct RowCountVerifyResult {
    bool ok{true};
    long long source_rows{0};
    long long lake_rows{0};
    long long diff{0};
    std::string message;
};

RowCountVerifyResult verify_full_load_row_counts(
    long long source_rows,
    long long lake_rows,
    long long rows_loaded);

void acquire_full_load_table_lock(PGconn* pg, long long catalog_id);
void release_full_load_table_lock(PGconn* pg, long long catalog_id);
bool try_acquire_full_load_table_lock(PGconn* pg, long long catalog_id);
bool full_load_table_lock_held_by_other(PGconn* pg, long long catalog_id);

struct CopyCheckpointContext {
    PGconn* app_pg{nullptr};
    PGconn* log_pg{nullptr};
    std::mutex* log_mtx{nullptr};
    long long catalog_id{0};
    int worker_id{0};
    std::string batch_id;
    std::string conn_id;
    std::string source_schema;
    std::string source_table;
    std::string log_component;
    std::optional<long long> source_rows;
    std::vector<std::string> initial_last_pk;
    int progress_log_interval{10};
    int batches_completed{0};
    std::mutex* checkpoint_mtx{nullptr};
};

void save_copy_batch_checkpoint(
    CopyCheckpointContext& ctx,
    const std::vector<std::string>& last_pk_values,
    long long rows_loaded_total);

bool should_resume_from_copy_checkpoint(
    PGconn* app_pg,
    PGconn* lake_pg,
    long long catalog_id,
    const std::string& lake_schema,
    const std::string& lake_table,
    std::vector<FullLoadCheckpoint>& checkpoints_out);

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
