#include "cdc_routing.hpp"

#include <sstream>

namespace cdc_routing {
namespace {

nlohmann::json json_at_path(const nlohmann::json& root, const std::string& path) {
    if (path.empty() || !root.is_object()) {
        return nullptr;
    }
    const nlohmann::json* cur = &root;
    std::stringstream ss(path);
    std::string part;
    while (std::getline(ss, part, '.')) {
        if (!cur->is_object() || !cur->contains(part)) {
            return nullptr;
        }
        cur = &(*cur)[part];
    }
    return *cur;
}

bool when_matches(const nlohmann::json& when, const CdcEvent& event) {
    if (!when.is_object()) {
        return false;
    }
    if (when.contains("op") && when["op"].is_string()) {
        if (when["op"].get<std::string>() != event.op) {
            return false;
        }
    }
    if (when.contains("path") && when["path"].is_string()) {
        nlohmann::json envelope = {
            {"op", event.op},
            {"before", event.before},
            {"after", event.after},
            {"source_schema", event.schema_name},
            {"source_table", event.table_name},
        };
        const auto got = json_at_path(envelope, when["path"].get<std::string>());
        if (when.contains("equals")) {
            if (got != when["equals"]) {
                return false;
            }
        } else if (got.is_null()) {
            return false;
        }
    }
    // Bare {"op":"d"} already handled; empty when matches nothing useful.
    return when.contains("op") || when.contains("path");
}

}  // namespace

std::string topic_with_content_routing(
    const std::string& base_topic,
    const CdcEvent& event,
    const nlohmann::json& content_routing_rules) {
    if (!content_routing_rules.is_array()) {
        return base_topic;
    }
    for (const auto& rule : content_routing_rules) {
        if (!rule.is_object() || !rule.contains("when")) {
            continue;
        }
        if (!when_matches(rule["when"], event)) {
            continue;
        }
        if (rule.contains("topic") && rule["topic"].is_string()) {
            return rule["topic"].get<std::string>();
        }
        if (rule.contains("topic_suffix") && rule["topic_suffix"].is_string()) {
            return base_topic + rule["topic_suffix"].get<std::string>();
        }
    }
    return base_topic;
}

bool op_filtered_out(const std::string& op, const nlohmann::json& exclude_ops) {
    if (!exclude_ops.is_array() || op.empty()) {
        return false;
    }
    for (const auto& item : exclude_ops) {
        if (item.is_string() && item.get<std::string>() == op) {
            return true;
        }
    }
    return false;
}

}  // namespace cdc_routing
