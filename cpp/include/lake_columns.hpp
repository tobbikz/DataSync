#pragma once

#include <string>
#include <vector>

namespace lake_columns {

constexpr const char* kLoadTimestamp = "_dl_load_timestamp";
constexpr const char* kLoadDate = "_dl_load_date";
constexpr const char* kSourceSystem = "_dl_source_system";
constexpr const char* kSnapshotId = "_dl_snapshot_id";
constexpr const char* kPartitionColumn = "_dl_load_timestamp";

inline std::vector<std::string> expected_lake_pk(const std::vector<std::string>& source_pk_cols) {
    std::vector<std::string> out = source_pk_cols;
    out.emplace_back(kLoadTimestamp);
    return out;
}

inline std::string date_from_timestamptz(const std::string& ts) {
    if (ts.size() >= 10) {
        return ts.substr(0, 10);
    }
    return ts;
}

}  // namespace lake_columns
