#include "cdc_routing.hpp"
#include "cdc_envelope.hpp"

#include <cstdlib>
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
    CdcEvent e;
    e.op = "d";
    e.schema_name = "s";
    e.table_name = "t";
    e.after = nullptr;
    e.before = {{"id", 1}};

    nlohmann::json rules = nlohmann::json::array({
        {{"when", {{"op", "d"}}}, {"topic_suffix", ".deletes"}},
        {{"when", {{"path", "after.status"}, {"equals", "archived"}}}, {"topic_suffix", ".arch"}},
    });

    EXPECT(cdc_routing::topic_with_content_routing("conn.bucket", e, rules) == "conn.bucket.deletes");

    e.op = "u";
    e.after = {{"status", "archived"}};
    EXPECT(cdc_routing::topic_with_content_routing("conn.bucket", e, rules) == "conn.bucket.arch");

    e.after = {{"status", "live"}};
    EXPECT(cdc_routing::topic_with_content_routing("conn.bucket", e, rules) == "conn.bucket");

    EXPECT(cdc_routing::op_filtered_out("d", nlohmann::json::array({"d"})));
    EXPECT(!cdc_routing::op_filtered_out("c", nlohmann::json::array({"d"})));
    EXPECT(cdc_routing::schema_change_topic("acme") == "acme.__schema");

    if (g_failures) {
        std::cerr << g_failures << " failure(s)\n";
        return 1;
    }
    std::cout << "cdc_routing_tests OK\n";
    return 0;
}
