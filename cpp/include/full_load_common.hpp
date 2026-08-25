#pragma once

#include "full_load_checkpoint.hpp"
#include "full_load_slice.hpp"
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

/** Fast existence check for resume (avoids COUNT(*) on huge tables). */
bool lake_table_has_rows(PGconn* pg, const std::string& schema, const std::string& table);

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

struct RowCountVerifyRequest {
    long long source_rows_live{-1};
    long long lake_rows{-1};
    long long rows_loaded{-1};
    std::optional<long long> baseline_source_rows;
    /** CDC replays the load window, so the lake may legitimately exceed the truncate baseline. */
    bool reconciles_load_window{false};
};

struct RowCountVerifyResult {
    bool ok{true};
    /** Row count used as verify reference (baseline snapshot or live source). */
    long long source_rows{0};
    long long source_rows_live{-1};
    long long baseline_source_rows{-1};
    long long lake_rows{0};
    long long diff{0};
    /** Live source minus lake when verify uses baseline (expected CDC backlog). */
    long long pending_cdc_gap{-1};
    std::string verify_mode;
    std::string message;
};

RowCountVerifyResult verify_full_load_row_counts(const RowCountVerifyRequest& request);

RowCountVerifyResult verify_full_load_row_counts(
    long long source_rows,
    long long lake_rows,
    long long rows_loaded);

/** Load truncate-phase baseline and load-window reconciliation from catalog/checkpoints.
 *  engine_replays_load_window: engine resumes CDC from an LSN anchored before the COPY
 *  (MSSQL), as opposed to streaming into Kafka during it (catalog.capture_during_full_load). */
RowCountVerifyRequest build_row_count_verify_request(
    PGconn* catalog_pg,
    long long catalog_id,
    long long source_rows_live,
    long long lake_rows,
    long long rows_loaded,
    bool engine_replays_load_window = false);

/** Structured log context for full-load row count verify checkpoints. */
nlohmann::json row_count_verify_log_context(
    const RowCountVerifyResult& verify,
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
    /** Per-worker rows already in lake when resuming (from copy checkpoint). */
    long long rows_loaded_session_baseline{0};
    int progress_log_interval{10};
    int batches_completed{0};
    std::mutex* checkpoint_mtx{nullptr};
};

void save_copy_batch_checkpoint(
    CopyCheckpointContext& ctx,
    const std::vector<std::string>& last_pk_values,
    long long rows_loaded_total);

/** Worker COPY position as committed in the lake, in lake.full_load_position. */
struct LakeCopyPosition {
    std::vector<std::string> last_pk;
    long long rows_loaded{0};
};

/**
 * Upserts the worker position. Must run inside the transaction that carries the COPY it
 * describes, so that data and position can never disagree.
 */
void record_lake_copy_position(
    PGconn* lake_pg,
    long long catalog_id,
    int worker_id,
    const std::string& batch_id,
    const std::vector<std::string>& last_pk,
    long long rows_loaded);

/**
 * Where a resuming worker has to restart. Absent when the worker never committed a batch,
 * or when the deployment predates lake.full_load_position.
 */
std::optional<LakeCopyPosition> load_lake_copy_position(
    PGconn* lake_pg,
    long long catalog_id,
    int worker_id);

void clear_lake_copy_positions(PGconn* lake_pg, long long catalog_id);

/**
 * Moves a resuming worker onto the lake position when there is one.
 * The catalog checkpoint is written after the COPY commits, so it can be a batch behind;
 * replaying from it is what used to duplicate rows.
 */
void adopt_lake_copy_position(
    CopyCheckpointContext& ctx,
    PGconn* lake_pg,
    std::vector<std::string>& last_pk_values);

/**
 * Persists the per-worker PK ranges before any worker starts.
 * Sampled boundaries depend on row positions, so a resume must reuse the stored split
 * instead of re-sampling a source that may have changed meanwhile.
 */
void save_copy_slice_plan(
    PGconn* app_pg,
    long long catalog_id,
    const std::string& batch_id,
    const std::vector<PkSlice>& slices,
    std::optional<long long> source_rows);

/**
 * Slice plan written by a previous run, empty when there is none.
 * A resumed load keeps this split even if the worker count knob changed since, because
 * re-splitting the table against a different worker count would duplicate or skip rows.
 */
std::vector<PkSlice> slice_plan_from_checkpoints(
    const std::vector<FullLoadCheckpoint>& checkpoints);

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
