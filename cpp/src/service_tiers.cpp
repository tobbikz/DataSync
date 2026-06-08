#include "service_tiers.hpp"

std::vector<ServiceTier> load_service_tiers(const AppConfig& cfg) {
    std::vector<ServiceTier> out;
    out.reserve(cfg.cdc.tiers.size());
    for (const auto& tier : cfg.cdc.tiers) {
        if (!tier.active) {
            continue;
        }
        ServiceTier row;
        row.tier_code = tier.code;
        row.sort_order = tier.sort_order;
        row.apply_worker_count = tier.apply_workers;
        out.push_back(std::move(row));
    }
    return out;
}
