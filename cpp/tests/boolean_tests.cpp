#include "mariadb_boolean.hpp"
#include "test_assert.hpp"

int main() {
    expect_true(try_parse_mariadb_bool_token("1").value_or(false), "1 is true");
    expect_true(!try_parse_mariadb_bool_token("0").value_or(true), "0 is false");
    expect_true(try_parse_mariadb_bool_token("b'1'").value_or(false), "b'1' is true");
    expect_true(!try_parse_mariadb_bool_token("b'0'").value_or(true), "b'0' is false");
    expect_true(!try_parse_mariadb_bool_token("not-a-bool").has_value(), "garbage rejected");
    return 0;
}
