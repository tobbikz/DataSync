#pragma once

#include "config.hpp"

#include <string>
#include <vector>

struct ServiceTier {
    std::string tier_code;
    int sort_order{0};
    int apply_worker_count{1};
};

/** Active tiers from config.json `cdc.tiers` (ordered by sort_order). */
std::vector<ServiceTier> load_service_tiers(const AppConfig& cfg);
