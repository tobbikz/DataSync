#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

struct DaemonFullLoadOutcome {
    bool ran{false};
    int exit_code{0};
    int pending_tables{0};
    int pending_after{0};
    int tables_loaded{0};
};

// Isolated full-load phase for daemon: fork+exec `DataSync full-load --tier --conn-id`.
// Daemon runs this in a background thread while pre-apply/apply run concurrently.
DaemonFullLoadOutcome run_daemon_full_load_isolated(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& batch_id);

/** True when another thread holds the per-(conn_id,tier) full-load lock (subprocess + onboard). */
bool full_load_tier_busy(const std::string& conn_id, const std::string& tier);

/** RAII holder for per-(conn_id,tier) full-load mutex; use for catchup reload isolation. */
class TierFullLoadLock {
public:
    static std::optional<TierFullLoadLock> try_acquire(
        const std::string& conn_id,
        const std::string& tier);
    TierFullLoadLock(TierFullLoadLock&&) noexcept = default;
    TierFullLoadLock& operator=(TierFullLoadLock&&) noexcept = default;
    TierFullLoadLock(const TierFullLoadLock&) = delete;
    TierFullLoadLock& operator=(const TierFullLoadLock&) = delete;
    bool holds() const { return guard_.owns_lock(); }

private:
    TierFullLoadLock(std::shared_ptr<std::mutex> mu, std::unique_lock<std::mutex> guard)
        : mu_(std::move(mu)), guard_(std::move(guard)) {}
    std::shared_ptr<std::mutex> mu_;
    std::unique_lock<std::mutex> guard_;
};

/** Acquire tier lock; returns nullopt if another full-load/onboard is already running. */
std::optional<DaemonFullLoadOutcome> try_run_daemon_full_load_isolated(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& batch_id);

int run_conn_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier);
