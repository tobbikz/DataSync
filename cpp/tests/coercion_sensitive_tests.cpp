#include "test_assert.hpp"
#include "type_coercion.hpp"

int main() {
    expect_true(mariadb_column_coercion_sensitive("bit(1)"), "bit(1) sensitive");
    expect_true(mariadb_column_coercion_sensitive("BIT(8)"), "BIT(8) sensitive");
    expect_true(mariadb_column_coercion_sensitive("tinyint(1)"), "tinyint(1) sensitive");
    expect_true(mariadb_column_coercion_sensitive("datetime"), "datetime sensitive");
    expect_true(mariadb_column_coercion_sensitive("timestamp(6)"), "timestamp sensitive");
    expect_true(!mariadb_column_coercion_sensitive("int(11)"), "int not sensitive");
    expect_true(!mariadb_column_coercion_sensitive("varchar(255)"), "varchar not sensitive");
    expect_true(mariadb_coercion_version_from_meta(R"({"coercion_mariadb_version":1})") == 1, "parse version");
    expect_true(mariadb_coercion_version_from_meta("{}") == -1, "missing version");
    return 0;
}
