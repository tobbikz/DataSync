#include "test_assert.hpp"
#include "type_coercion.hpp"

int main() {
    expect_true(mariadb_column_coercion_sensitive("bit(1)"), "mariadb bit(1)");
    expect_true(mariadb_column_coercion_sensitive("datetime"), "mariadb datetime");
    expect_true(!mariadb_column_coercion_sensitive("int(11)"), "mariadb int");

    expect_true(mssql_column_coercion_sensitive("bit"), "mssql bit");
    expect_true(mssql_column_coercion_sensitive("datetime2"), "mssql datetime2");
    expect_true(!mssql_column_coercion_sensitive("int"), "mssql int");

    expect_true(mongo_inferred_type_coercion_sensitive("BOOLEAN"), "mongo boolean");
    expect_true(!mongo_inferred_type_coercion_sensitive("TEXT"), "mongo text");

    expect_true(coercion_version_from_meta(R"({"coercion_mariadb_version":1})", kCoercionMariaDbMetaKey) == 1,
        "mariadb meta");
    expect_true(coercion_version_from_meta(R"({"coercion_mssql_version":2})", kCoercionMssqlMetaKey) == 2,
        "mssql meta");
    expect_true(coercion_version_from_meta(R"({"coercion_mongodb_version":1})", kCoercionMongodbMetaKey) == 1,
        "mongodb meta");
    expect_true(coercion_version_from_meta("{}", kCoercionMariaDbMetaKey) == -1, "missing meta");
    return 0;
}
