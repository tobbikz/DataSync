#pragma once

#include <libpq-fe.h>

#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>

enum class LogLevel { Debug, Info, Warning, Error };

struct LogEvent {
    LogLevel level{LogLevel::Info};
    std::string component;
    std::string message;
    std::optional<std::string> batch_id{std::nullopt};
    std::optional<std::string> conn_id{std::nullopt};
    std::optional<std::string> source_schema{std::nullopt};
    std::optional<std::string> source_table{std::nullopt};
    nlohmann::json context{nlohmann::json::object()};
};

inline LogEvent make_log(
    LogLevel level,
    std::string component,
    std::string message,
    nlohmann::json context = nlohmann::json::object(),
    std::optional<std::string> batch_id = std::nullopt,
    std::optional<std::string> conn_id = std::nullopt,
    std::optional<std::string> source_schema = std::nullopt,
    std::optional<std::string> source_table = std::nullopt) {
    LogEvent e;
    e.level = level;
    e.component = std::move(component);
    e.message = std::move(message);
    e.batch_id = std::move(batch_id);
    e.conn_id = std::move(conn_id);
    e.source_schema = std::move(source_schema);
    e.source_table = std::move(source_table);
    e.context = std::move(context);
    return e;
}

// Persists to cdc_catalog.logs. Returns false if insert failed (caller may stderr).
bool log_write(PGconn* pg, const LogEvent& event);

// Deletes rows older than retention_days via batched+locked purge_logs_batched.
// Prefer daemon scheduled retention (03:00 CST); safe to call ad-hoc (skips if lock held).
// Returns rows deleted, 0 if skipped/noop, -1 on error.
long long purge_logs(PGconn* pg, int retention_days = 7);

std::string make_batch_id();
