#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <string>

/** C01/C05/C07: signal drain, notifications, CDC config hot-reload. */
namespace control_plane {

void notify(
    PGconn* pg,
    const std::string& channel,
    const std::string& severity,
    const std::string& title,
    const std::string& body,
    const std::optional<std::string>& conn_id = std::nullopt,
    const std::optional<long long>& catalog_id = std::nullopt,
    const nlohmann::json& context = nlohmann::json::object());

/** Reload `cdc` section from DATASYNC_CONFIG / config_path into cfg.cdc. Returns true if changed. */
bool reload_cdc_tuning(AppConfig& cfg, const std::string& config_path);

/**
 * Drain pending control signals: reload_config, log, notify (+ snapshot actions left for full-load).
 * Returns number of signals handled.
 */
int drain_control_signals(PGconn* pg, AppConfig& cfg, const std::string& config_path);

/** A07: true if (catalog_id, event_id) already in apply_dedup. Empty event_id → false. */
bool apply_event_already_seen(
    PGconn* pg,
    long long catalog_id,
    const std::string& event_id);

/** A07: record successful apply; false = already present (duplicate). Empty event_id always ok. */
bool claim_apply_event(
    PGconn* pg,
    long long catalog_id,
    const std::string& event_id,
    const std::string& batch_id);

/** R08: emit lineage event when openlineage enabled by caller. */
void emit_lineage(
    PGconn* pg,
    const std::string& event_type,
    const std::optional<std::string>& conn_id,
    const std::optional<long long>& catalog_id,
    const std::string& run_id,
    const nlohmann::json& inputs = nlohmann::json::array(),
    const nlohmann::json& outputs = nlohmann::json::array(),
    const nlohmann::json& context = nlohmann::json::object());

/** A07b: append audit row when enabled. */
void audit_apply_event(
    PGconn* pg,
    long long catalog_id,
    const std::string& conn_id,
    const std::string& event_id,
    const std::string& op,
    const std::string& schema,
    const std::string& table,
    const std::string& topic,
    int partition,
    long long offset,
    const std::string& batch_id);

}  // namespace control_plane
