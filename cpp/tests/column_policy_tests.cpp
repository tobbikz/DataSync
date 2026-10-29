#include "column_policy.hpp"

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
    using namespace column_policy;

    nlohmann::json extras = {
        {"table_include", nlohmann::json::array({"testdb\\..*", "orders"})},
        {"table_exclude", nlohmann::json::array({"testdb\\.tmp_.*"})},
    };
    auto tf = table_filter_from_extras(extras);
    EXPECT(table_allowed(tf, "testdb", "customers"));
    EXPECT(!table_allowed(tf, "testdb", "tmp_x"));
    EXPECT(table_allowed(tf, "other", "orders"));
    EXPECT(!table_allowed(tf, "other", "customers"));

    nlohmann::json meta = {
        {"column_include", nlohmann::json::array({"id", "email"})},
        {"column_exclude", nlohmann::json::array({"secret"})},
        {"column_mask", {{"email", "hash"}}},
    };
    auto cf = column_filter_from_meta(meta);
    EXPECT(column_allowed(cf, "id"));
    EXPECT(column_allowed(cf, "email"));
    EXPECT(!column_allowed(cf, "name"));
    EXPECT(!column_allowed(cf, "secret"));

    const std::string hashed = maybe_mask_value(cf, "email", "a@b.com");
    EXPECT(hashed.size() == 64);
    EXPECT(hashed != "a@b.com");
    EXPECT(maybe_mask_value(cf, "id", "1") == "1");
    EXPECT(sha256_hex("abc").size() == 64);

    EXPECT(is_online_ddl_helper_table("_t_gho"));
    EXPECT(is_online_ddl_helper_table("orders_ghc"));
    EXPECT(is_online_ddl_helper_table("orders_del"));
    EXPECT(is_online_ddl_helper_table("_orders_new"));
    EXPECT(is_online_ddl_helper_table("_orders_old"));
    EXPECT(!is_online_ddl_helper_table("orders"));
    EXPECT(!capture_online_ddl_helpers(nlohmann::json::object()));
    EXPECT(capture_online_ddl_helpers({{"capture_online_ddl_helpers", true}}));

    if (g_failures) {
        std::cerr << g_failures << " failure(s)\n";
        return 1;
    }
    std::cout << "column_policy_tests OK\n";
    return 0;
}
