#include <catch2/catch_test_macros.hpp>

#include "mariadb_binlog.hpp"

namespace {

BinlogPosition pos(const char* file, long long position) {
    return BinlogPosition{file, position};
}

MasterBinlogStatus master(const char* file, long long position) {
    MasterBinlogStatus m;
    m.file = file;
    m.position = position;
    return m;
}

}  // namespace

TEST_CASE("binlog_file_sequence_number", "[mariadb_binlog]") {
    CHECK(binlog_file_sequence_number("mariadb2-bin.00013283") == 13283);
    CHECK(binlog_file_sequence_number("mysql-bin.000001") == 1);
    CHECK_FALSE(binlog_file_sequence_number("no-suffix").has_value());
    CHECK_FALSE(binlog_file_sequence_number("bad.bin").has_value());
}

TEST_CASE("binlog_position_caught_up", "[mariadb_binlog]") {
    SECTION("empty master file") {
        CHECK_FALSE(binlog_position_caught_up(pos("a.bin", 100), master("", 0)));
    }

    SECTION("same file") {
        CHECK(binlog_position_caught_up(pos("mariadb2-bin.000010", 500), master("mariadb2-bin.000010", 500)));
        CHECK(binlog_position_caught_up(pos("mariadb2-bin.000010", 600), master("mariadb2-bin.000010", 500)));
        CHECK_FALSE(binlog_position_caught_up(pos("mariadb2-bin.000010", 400), master("mariadb2-bin.000010", 500)));
    }

    SECTION("sequence-based ordering") {
        CHECK(binlog_position_caught_up(pos("mariadb2-bin.000011", 4), master("mariadb2-bin.000010", 999)));
        CHECK(binlog_position_caught_up(pos("mariadb2-bin.000010", 500), master("mariadb2-bin.000010", 400)));
        CHECK_FALSE(binlog_position_caught_up(pos("mariadb2-bin.000009", 900), master("mariadb2-bin.000010", 100)));
        CHECK_FALSE(
            binlog_position_caught_up(pos("mariadb2-bin.000010", 100), master("mariadb2-bin.000010", 200)));
    }

    SECTION("unparseable filenames") {
        CHECK_FALSE(binlog_position_caught_up(pos("alpha", 100), master("beta", 50)));
    }
}

TEST_CASE("binlog_cursor_is_behind", "[mariadb_binlog]") {
    SECTION("empty master file") {
        CHECK_FALSE(binlog_cursor_is_behind(pos("a.bin", 0), master("", 0)));
    }

    SECTION("same file") {
        CHECK(binlog_cursor_is_behind(pos("mariadb2-bin.000010", 100), master("mariadb2-bin.000010", 200)));
        CHECK_FALSE(binlog_cursor_is_behind(pos("mariadb2-bin.000010", 200), master("mariadb2-bin.000010", 200)));
        CHECK_FALSE(binlog_cursor_is_behind(pos("mariadb2-bin.000010", 300), master("mariadb2-bin.000010", 200)));
    }

    SECTION("sequence-based ordering") {
        CHECK(binlog_cursor_is_behind(pos("mariadb2-bin.000009", 900), master("mariadb2-bin.000010", 100)));
        CHECK_FALSE(binlog_cursor_is_behind(pos("mariadb2-bin.000011", 4), master("mariadb2-bin.000010", 999)));
        CHECK(binlog_cursor_is_behind(pos("mariadb2-bin.000010", 100), master("mariadb2-bin.000011", 4)));
    }

    SECTION("lexicographic fallback without numeric suffix") {
        CHECK(binlog_cursor_is_behind(pos("aaa", 0), master("bbb", 0)));
        CHECK_FALSE(binlog_cursor_is_behind(pos("ccc", 0), master("bbb", 0)));
    }
}

TEST_CASE("caught_up and is_behind are complementary on same file", "[mariadb_binlog]") {
    const auto cursor = pos("mariadb2-bin.000010", 150);
    const auto m = master("mariadb2-bin.000010", 200);
    CHECK(binlog_cursor_is_behind(cursor, m));
    CHECK_FALSE(binlog_position_caught_up(cursor, m));
}
