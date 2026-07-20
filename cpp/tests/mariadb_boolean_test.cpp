#include "mariadb_boolean.hpp"

#include <iostream>
#include <string>
#include <string_view>

namespace {

int expect_bool(const char* label, std::string_view token, bool want) {
    const auto got = try_parse_mariadb_bool_token(token);
    if (!got.has_value() || *got != want) {
        std::cerr << "FAIL " << label << " token=[" << token << "] expected=" << (want ? "true" : "false")
                  << " got=" << (got ? (*got ? "true" : "false") : "nullopt") << "\n";
        return 1;
    }
    return 0;
}

int expect_null(const char* label, std::string_view token) {
    if (try_parse_mariadb_bool_token(token).has_value()) {
        std::cerr << "FAIL " << label << " token=[" << token << "] expected nullopt\n";
        return 1;
    }
    return 0;
}

}  // namespace

int main() {
    int failures = 0;

    failures += expect_bool("digit1", "1", true);
    failures += expect_bool("digit0", "0", false);
    failures += expect_bool("b1", "b'1'", true);
    failures += expect_bool("b0", "b'0'", false);
    failures += expect_bool("B1_upper", "B'1'", true);
    failures += expect_bool("hex_esc1", "\\x01", true);
    failures += expect_bool("hex_esc0", "\\x00", false);
    failures += expect_bool("ox01", "0x01", true);
    failures += expect_bool("ox00", "0x00", false);
    failures += expect_bool("Xq01", "X'01'", true);
    failures += expect_bool("true", "true", true);
    failures += expect_bool("false", "false", false);
    failures += expect_bool("yes", "yes", true);
    failures += expect_bool("no", "no", false);
    failures += expect_bool("raw_byte1", std::string(1, '\x01'), true);
    failures += expect_bool("raw_byte0", std::string(1, '\x00'), false);

    failures += expect_null("empty", "");
    failures += expect_null("garbage", "not-a-bool");
    failures += expect_null("multi_hex", "\\x0102");

    if (failures == 0) {
        std::cout << "mariadb_boolean_test: ok\n";
    }
    return failures;
}
