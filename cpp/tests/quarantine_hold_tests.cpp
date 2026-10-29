#include "kafka_apply_detail.hpp"
#include "test_assert.hpp"

#include <map>
#include <string>
#include <utility>

int main() {
    using kafka_apply_detail::clamp_commits_before_quarantine_holds;
    using kafka_apply_detail::KafkaTopicPartition;
    using kafka_apply_detail::note_quarantine_hold;

    std::map<KafkaTopicPartition, long long> holds;
    note_quarantine_hold(holds, "conn.b0001", 3, 100);
    note_quarantine_hold(holds, "conn.b0001", 3, 95);  // earlier hold wins
    note_quarantine_hold(holds, "conn.b0001", 3, 120);
    expect_true(holds.at({"conn.b0001", 3}) == 95, "earliest quarantine hold wins");

    std::map<KafkaTopicPartition, long long> proposed{
        {{"conn.b0001", 3}, 150},
        {{"conn.b0002", 1}, 40},
    };
    const auto safe = clamp_commits_before_quarantine_holds(proposed, holds);
    expect_true(safe.count({"conn.b0001", 3}) == 1, "clamped tp present");
    expect_true(safe.at({"conn.b0001", 3}) == 94, "commit capped to hold-1");
    expect_true(safe.at({"conn.b0002", 1}) == 40, "unrelated tp unchanged");

    std::map<KafkaTopicPartition, long long> before_hold{{{"conn.b0001", 3}, 50}};
    const auto safe_before = clamp_commits_before_quarantine_holds(before_hold, holds);
    expect_true(safe_before.at({"conn.b0001", 3}) == 50, "offset already before hold kept");

    std::map<KafkaTopicPartition, long long> hold_zero;
    note_quarantine_hold(hold_zero, "t", 0, 0);
    std::map<KafkaTopicPartition, long long> prop_zero{{{"t", 0}, 10}};
    const auto safe_zero = clamp_commits_before_quarantine_holds(prop_zero, hold_zero);
    expect_true(safe_zero.empty(), "hold at offset 0 blocks all commits on tp");

    return 0;
}
