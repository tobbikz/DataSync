#pragma once

#include "cdc_envelope.hpp"

#include <nlohmann/json.hpp>

#include <string>

namespace cdc_routing {

/**
 * T05: apply content_routing rules; returns topic (may append topic_suffix).
 * Rule shape: {"when":{"op":"d"} or {"path":"after.status","equals":"x"}, "topic_suffix":".x"}
 */
std::string topic_with_content_routing(
    const std::string& base_topic,
    const CdcEvent& event,
    const nlohmann::json& content_routing_rules);

/** T06: true if event.op is listed in message_filter_exclude_ops. */
bool op_filtered_out(const std::string& op, const nlohmann::json& exclude_ops);

/** D02: topic name for schema-change events. */
inline std::string schema_change_topic(const std::string& prefix) {
    return prefix + ".__schema";
}

}  // namespace cdc_routing
