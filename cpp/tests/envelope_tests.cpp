#include "cdc_envelope.hpp"
#include "test_assert.hpp"

#include <nlohmann/json.hpp>

int main() {
    expect_eq_int(static_cast<int>(op_char_from_mysql("INSERT").size()), 1, "insert op");
    expect_true(op_char_from_mysql("INSERT") == "c", "insert -> c");
    expect_true(op_char_from_mysql("UPDATE") == "u", "update -> u");
    expect_true(op_char_from_mysql("DELETE") == "d", "delete -> d");
    expect_true(op_char_from_mysql("UNKNOWN").empty(), "unknown op empty");

    CdcEvent event;
    event.op = "u";
    event.conn_id = "TEST";
    event.schema_name = "public";
    event.table_name = "users";
    event.after = nlohmann::json{{"id", 1}, {"email", "a@b.c"}};
    event.tx_id = 42LL;
    event.tx_event = "data";

    const nlohmann::json payload = nlohmann::json::parse(cdc_event_kafka_payload(event));
    expect_true(payload["op"] == "u", "payload op");
    expect_true(payload.contains("tx"), "payload has tx");
    expect_true(payload["tx"]["id"] == 42, "tx id");
    expect_true(payload["tx"]["event"] == "data", "tx event");

    CdcEvent tx_marker;
    tx_marker.op = "t";
    tx_marker.conn_id = "TEST";
    tx_marker.tx_id = 99LL;
    tx_marker.tx_event = "commit";
    const nlohmann::json tx_payload = nlohmann::json::parse(cdc_event_kafka_payload(tx_marker));
    expect_true(tx_payload["op"] == "t", "tx marker op");
    expect_true(tx_payload["tx"]["event"] == "commit", "tx marker event");
    return 0;
}
