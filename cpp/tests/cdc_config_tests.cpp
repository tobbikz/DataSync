#include "config.hpp"

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
    const auto d = default_cdc_config();
    EXPECT(d.apply_dedup == true);
    EXPECT(d.apply_audit == false);
    EXPECT(d.kafka_heartbeat == true);
    EXPECT(d.capture_worker_count >= 1);
    EXPECT(d.binlog_mode == "auto");
    EXPECT(d.kafka_tombstones == true);
    EXPECT(d.reselect_on_apply == false);
    EXPECT(d.cloudevents == false);
    EXPECT(d.tracing == true);
    EXPECT(d.openlineage == false);

    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / "datasync_p3_cdc_config.json";
    {
        std::ofstream out(tmp);
        out << R"({
  "datasync": {"host":"127.0.0.1","port":5432,"database":"x","user":"u","password":"p"},
  "datalake": {"host":"127.0.0.1","port":5432,"database":"y","user":"u","password":"p"},
  "cdc": {
    "round_idle_seconds": 11,
    "capture_worker_count": 4,
    "apply_dedup": true,
    "apply_audit": true,
    "mssql_replay_on_idle": true,
    "kafka_heartbeat": false,
    "binlog_mode": "cli",
    "kafka_tombstones": false,
    "reselect_on_apply": true,
    "cloudevents": true,
    "tracing": false,
    "openlineage": true
  }
})";
    }
    const AppConfig cfg = load_config(tmp.string());
    EXPECT(cfg.cdc.round_idle_seconds == 11);
    EXPECT(cfg.cdc.capture_worker_count == 4);
    EXPECT(cfg.cdc.apply_dedup == true);
    EXPECT(cfg.cdc.apply_audit == true);
    EXPECT(cfg.cdc.mssql_replay_on_idle == true);
    EXPECT(cfg.cdc.kafka_heartbeat == false);
    EXPECT(cfg.cdc.binlog_mode == "cli");
    EXPECT(cfg.cdc.kafka_tombstones == false);
    EXPECT(cfg.cdc.reselect_on_apply == true);
    EXPECT(cfg.cdc.cloudevents == true);
    EXPECT(cfg.cdc.tracing == false);
    EXPECT(cfg.cdc.openlineage == true);
    EXPECT(cfg.config_path == tmp.string());

    std::error_code ec;
    fs::remove(tmp, ec);

    if (g_failures) {
        std::cerr << g_failures << " failure(s)\n";
        return 1;
    }
    std::cout << "cdc_config_tests OK\n";
    return 0;
}
