#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <string>

struct PreApplyCycleResult {
    nlohmann::json payload = nlohmann::json::object();
    int errors{0};
};

/** Capture all CDC tables for conn once per daemon round. */
int run_conn_capture_slice(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id);

PreApplyCycleResult run_pre_apply_cycle(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id);
