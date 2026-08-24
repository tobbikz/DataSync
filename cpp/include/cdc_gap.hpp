#pragma once

#include <libpq-fe.h>

#include <mysql.h>

#include <nlohmann/json.hpp>
#include <optional>
#include <string>

struct AppConfig;
struct BinlogGapRebootResult;

enum class GapSide { Capture, Apply };

enum class GapKind {
    BinlogPurged,
    ServerUuidChanged,
    MssqlLsnPurged,
    KafkaOffsetLost,
};

inline const char* gap_side_str(GapSide side) {
    switch (side) {
        case GapSide::Capture:
            return "capture";
        case GapSide::Apply:
            return "apply";
    }
    return "capture";
}

inline const char* gap_kind_str(GapKind kind) {
    switch (kind) {
        case GapKind::BinlogPurged:
            return "binlog_purged";
        case GapKind::ServerUuidChanged:
            return "server_uuid_changed";
        case GapKind::MssqlLsnPurged:
            return "mssql_lsn_purged";
        case GapKind::KafkaOffsetLost:
            return "kafka_offset_lost";
    }
    return "binlog_purged";
}

inline GapKind infer_gap_kind_from_detail(const std::string& detail) {
    if (detail.find("server_uuid") != std::string::npos) {
        return GapKind::ServerUuidChanged;
    }
    if (detail.find("lsn purged") != std::string::npos || detail.find("mssql cdc") != std::string::npos) {
        return GapKind::MssqlLsnPurged;
    }
    if (detail.find("offset") != std::string::npos || detail.find("Offset") != std::string::npos) {
        return GapKind::KafkaOffsetLost;
    }
    return GapKind::BinlogPurged;
}

/** Append-only audit row in cdc_catalog.gap_events. Returns gap_id or 0 on failure. */
long long record_gap_event(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    GapSide side,
    GapKind kind,
    const std::string& detail,
    const nlohmann::json& context,
    const std::string& remediation,
    int tables_flagged,
    const std::string& batch_id,
    std::optional<long long> catalog_id = std::nullopt,
    const std::optional<std::string>& source_schema = std::nullopt,
    const std::optional<std::string>& source_table = std::nullopt);

void mark_gap_event_resolved(PGconn* pg, long long gap_id, const std::string& remediation_note);

void clear_capture_gap_status(PGconn* pg, const std::string& conn_id);

void mark_apply_position_gap_detected(PGconn* pg, long long catalog_id, const std::string& reason);

/** Flag catalog needs_full_load, record gap event, clear apply gap_detected. */
bool recover_apply_table_gap(
    PGconn* pg,
    long long catalog_id,
    const std::string& conn_id,
    const std::string& db_engine,
    GapKind kind,
    const std::string& detail,
    const std::string& batch_id,
    const nlohmann::json& context = nlohmann::json::object());

/** MariaDB capture gap: T0 reset + needs_full_load + gap_events audit. */
BinlogGapRebootResult recover_mariadb_capture_gap(
    PGconn* pg,
    MYSQL* mysql,
    const std::string& conn_id,
    const std::string& batch_id,
    GapKind kind,
    const std::string& detail,
    const nlohmann::json& context = nlohmann::json::object());

struct GapSweepResult {
    int capture_recovered{0};
    int apply_recovered{0};
    int unresolved_capture{0};
    int unresolved_apply{0};
};

/** Daemon sweep: recover capture/apply rows stuck in gap_detected; close audit rows when healthy. */
GapSweepResult run_gap_playbook_sweep(const AppConfig& cfg, PGconn* pg, const std::string& batch_id);

/** Mark gap_events resolved when catalog/capture no longer needs remediation. Returns rows updated. */
int resolve_stale_gap_events(PGconn* pg);
