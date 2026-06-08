#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <string>
#include <vector>

struct CatchupCandidate {
    std::string source_schema;
    std::string source_table;
    int lag_s{0};
    long long kafka_backlog{0};
};

struct CatchupResult {
    nlohmann::json payload = nlohmann::json::object();
    std::string error;
};

std::vector<CatchupCandidate> find_catchup_candidates(const AppConfig& cfg, PGconn* log_pg, const std::string& conn_id);

std::vector<CatchupResult> run_catchup_if_needed(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& tier,
    const std::string& batch_id);
