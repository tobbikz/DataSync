#include "config.hpp"
#include "control_plane.hpp"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>

static int g_failures = 0;

#define EXPECT(cond)                                                                 \
    do {                                                                             \
        if (!(cond)) {                                                               \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__ << " " #cond << '\n'; \
            ++g_failures;                                                            \
        }                                                                            \
    } while (0)

int main() {
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / "datasync_p2_cdc_reload.json";
    {
        std::ofstream out(tmp);
        out << R"({
  "cdc": {
    "round_idle_seconds": 9,
    "slice_max_seconds": 42,
    "capture_worker_count": 3,
    "apply_dedup": false,
    "apply_audit": true,
    "mssql_replay_on_idle": true,
    "kafka_heartbeat": false
  }
})";
    }

    AppConfig cfg;
    cfg.cdc = default_cdc_config();
    cfg.config_path = tmp.string();
    EXPECT(cfg.cdc.apply_dedup == true);
    EXPECT(control_plane::reload_cdc_tuning(cfg, cfg.config_path));
    EXPECT(cfg.cdc.round_idle_seconds == 9);
    EXPECT(cfg.cdc.slice_max_seconds == 42);
    EXPECT(cfg.cdc.capture_worker_count == 3);
    EXPECT(cfg.cdc.apply_dedup == false);
    EXPECT(cfg.cdc.apply_audit == true);
    EXPECT(cfg.cdc.mssql_replay_on_idle == true);
    EXPECT(cfg.cdc.kafka_heartbeat == false);
    EXPECT(!control_plane::reload_cdc_tuning(cfg, cfg.config_path));

    std::error_code ec;
    fs::remove(tmp, ec);

    if (g_failures) {
        std::cerr << g_failures << " failure(s)\n";
        return 1;
    }
    std::cout << "control_plane_tests OK\n";
    return 0;
}
