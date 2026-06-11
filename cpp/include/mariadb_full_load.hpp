#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <optional>
#include <string>

struct FullLoadRunStats {
    int tables_processed{0};
    int tables_success{0};
    int tables_skipped{0};
    int tables_failed{0};
    long long total_rows{0};
};

/** Exit 0 when nothing failed, or at least one table succeeded; 1 only when every processed table failed. */
inline int full_load_process_exit_code(const FullLoadRunStats& stats) {
    if (stats.tables_processed == 0) {
        return 0;
    }
    if (stats.tables_failed == 0) {
        return 0;
    }
    return stats.tables_success > 0 ? 0 : 1;
}

FullLoadRunStats run_mariadb_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& service_tier,
    const std::optional<std::string>& conn_id_filter = std::nullopt);
