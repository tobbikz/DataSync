#pragma once

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

/** C02/C03: catalog-driven snapshot control (pause/resume/stop/blocking/incremental). */
namespace snapshot_signal {

enum class Action {
    ExecuteIncremental,
    ExecuteBlocking,
    Pause,
    Resume,
    Stop,
    ReloadConfig,
    Log,
    Notify,
};

enum class Status {
    Pending,
    Running,
    Paused,
    Completed,
    Cancelled,
    Failed,
};

struct Signal {
    long long signal_id{0};
    std::optional<std::string> conn_id;
    std::optional<long long> catalog_id;
    Action action{Action::ExecuteIncremental};
    Status status{Status::Pending};
    nlohmann::json payload = nlohmann::json::object();
    std::string last_error;
};

inline std::string action_to_string(Action a) {
    switch (a) {
        case Action::ExecuteIncremental:
            return "execute_incremental";
        case Action::ExecuteBlocking:
            return "execute_blocking";
        case Action::Pause:
            return "pause";
        case Action::Resume:
            return "resume";
        case Action::Stop:
            return "stop";
        case Action::ReloadConfig:
            return "reload_config";
        case Action::Log:
            return "log";
        case Action::Notify:
            return "notify";
    }
    return "execute_incremental";
}

inline std::optional<Action> action_from_string(std::string_view s) {
    if (s == "execute_incremental") {
        return Action::ExecuteIncremental;
    }
    if (s == "execute_blocking") {
        return Action::ExecuteBlocking;
    }
    if (s == "pause") {
        return Action::Pause;
    }
    if (s == "resume") {
        return Action::Resume;
    }
    if (s == "stop") {
        return Action::Stop;
    }
    if (s == "reload_config") {
        return Action::ReloadConfig;
    }
    if (s == "log") {
        return Action::Log;
    }
    if (s == "notify") {
        return Action::Notify;
    }
    return std::nullopt;
}

inline std::string status_to_string(Status s) {
    switch (s) {
        case Status::Pending:
            return "pending";
        case Status::Running:
            return "running";
        case Status::Paused:
            return "paused";
        case Status::Completed:
            return "completed";
        case Status::Cancelled:
            return "cancelled";
        case Status::Failed:
            return "failed";
    }
    return "pending";
}

inline std::optional<Status> status_from_string(std::string_view s) {
    if (s == "pending") {
        return Status::Pending;
    }
    if (s == "running") {
        return Status::Running;
    }
    if (s == "paused") {
        return Status::Paused;
    }
    if (s == "completed") {
        return Status::Completed;
    }
    if (s == "cancelled") {
        return Status::Cancelled;
    }
    if (s == "failed") {
        return Status::Failed;
    }
    return std::nullopt;
}

/** Insert a new pending signal; returns signal_id. */
long long enqueue(
    PGconn* pg,
    Action action,
    const std::optional<std::string>& conn_id,
    const std::optional<long long>& catalog_id,
    const nlohmann::json& payload = nlohmann::json::object());

std::optional<Signal> fetch(PGconn* pg, long long signal_id);

/** Newest pending/running/paused signal matching optional conn/catalog filters. */
std::optional<Signal> latest_active(
    PGconn* pg,
    const std::optional<std::string>& conn_id,
    const std::optional<long long>& catalog_id);

void set_status(
    PGconn* pg,
    long long signal_id,
    Status status,
    const std::optional<std::string>& last_error = std::nullopt);

/**
 * Cooperative control for a running incremental/full load.
 * - Stop → true (caller should abort)
 * - Pause → blocks until Resume/Stop (or shutdown)
 * Returns true when the load must stop.
 */
bool poll_cooperative_stop(
    PGconn* pg,
    const std::string& conn_id,
    long long catalog_id);

/** True when a blocking snapshot is running for this catalog (capture should defer). */
bool blocking_active_for_catalog(PGconn* pg, long long catalog_id);

}  // namespace snapshot_signal
