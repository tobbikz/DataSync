#include "cdc_gap.hpp"
#include "test_assert.hpp"

int main() {
    expect_true(std::string(gap_side_str(GapSide::Capture)) == "capture", "capture side");
    expect_true(std::string(gap_side_str(GapSide::Apply)) == "apply", "apply side");
    expect_true(std::string(gap_kind_str(GapKind::BinlogPurged)) == "binlog_purged", "binlog kind");
    expect_true(
        std::string(gap_kind_str(GapKind::ServerUuidChanged)) == "server_uuid_changed", "uuid kind");
    expect_true(
        infer_gap_kind_from_detail("server_uuid changed abc -> def") == GapKind::ServerUuidChanged,
        "infer uuid");
    expect_true(
        infer_gap_kind_from_detail("cdc lsn purged: auto full-load reboot") == GapKind::MssqlLsnPurged,
        "infer mssql");
    expect_true(
        infer_gap_kind_from_detail("kafka offset out of range") == GapKind::KafkaOffsetLost,
        "infer offset");
    expect_true(
        infer_gap_kind_from_detail("binlog purged: auto full-load reboot") == GapKind::BinlogPurged,
        "infer binlog default");
    return 0;
}
