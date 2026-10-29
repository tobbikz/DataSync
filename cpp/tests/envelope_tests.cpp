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

    event.query = "UPDATE users SET email='x' WHERE id=1";
    const nlohmann::json with_q = nlohmann::json::parse(cdc_event_kafka_payload(event));
    expect_true(with_q["source"]["query"] == "UPDATE users SET email='x' WHERE id=1", "source.query");

    event.ingestion_ts = "2026-10-29T00:00:00Z";
    const std::string plain = cdc_event_kafka_payload_maybe_cloudevents(event, false);
    expect_true(plain.find("specversion") == std::string::npos, "no ce when off");
    const nlohmann::json ce = nlohmann::json::parse(cdc_event_kafka_payload_maybe_cloudevents(event, true));
    expect_true(ce["specversion"] == "1.0", "ce specversion");
    expect_true(ce.contains("data"), "ce data");
    expect_true(ce["type"] == "com.datasync.cdc.u", "ce type");
    return 0;
}
