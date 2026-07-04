#pragma once

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <string>
#include <vector>

enum class FullLoadPhase {
    Truncate,
    Ddl,
    Copy,
};

struct FullLoadCheckpoint {
    long long catalog_id{0};
    int worker_id{0};
    std::string batch_id;
    FullLoadPhase phase{FullLoadPhase::Truncate};
    nlohmann::json last_pk = nlohmann::json::array();
    long long rows_loaded{0};
    std::optional<long long> source_rows;
};

std::string full_load_phase_to_string(FullLoadPhase phase);
std::optional<FullLoadPhase> full_load_phase_from_string(const std::string& value);

std::optional<FullLoadCheckpoint> load_full_load_checkpoint(
    PGconn* pg,
    long long catalog_id,
    int worker_id);

std::vector<FullLoadCheckpoint> load_full_load_checkpoints(PGconn* pg, long long catalog_id);

void save_full_load_checkpoint(PGconn* pg, const FullLoadCheckpoint& checkpoint);

void clear_full_load_checkpoints(PGconn* pg, long long catalog_id);

std::vector<std::string> last_pk_from_json(const nlohmann::json& last_pk);

nlohmann::json last_pk_to_json(const std::vector<std::string>& last_pk);

/** True if conn has any copy-phase checkpoint (full-load in progress or resumable). */
bool conn_has_active_copy_checkpoints(PGconn* pg, const std::string& conn_id);

/**
 * Reset full_load_in_progress → pending for tables with copy checkpoints so full-load can resume.
 * Returns number of catalog rows updated.
 */
int recover_full_load_for_checkpoint_resume(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::string& batch_id);
