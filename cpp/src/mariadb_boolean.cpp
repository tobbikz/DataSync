#include "mariadb_boolean.hpp"

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>

namespace {

std::string_view trim_view(std::string_view s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
        s.remove_prefix(1);
    }
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
        s.remove_suffix(1);
    }
    return s;
}

std::string to_lower_ascii(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }
    return out;
}

bool is_hex_digit(char c) {
    return std::isxdigit(static_cast<unsigned char>(c)) != 0;
}

std::optional<std::uint8_t> parse_hex_byte_pair(char hi, char lo) {
    if (!is_hex_digit(hi) || !is_hex_digit(lo)) {
        return std::nullopt;
    }
    const char pair[3] = {hi, lo, '\0'};
    char* end = nullptr;
    const unsigned long v = std::strtoul(pair, &end, 16);
    if (end == nullptr || *end != '\0' || v > 0xffUL) {
        return std::nullopt;
    }
    return static_cast<std::uint8_t>(v);
}

/** Try \\xHH, 0xHH, X'HH' / x'HH' forms (1 byte). */
std::optional<bool> try_parse_one_byte_hex(std::string_view s) {
    if (s.size() >= 4 && s[0] == '\\' && (s[1] == 'x' || s[1] == 'X')) {
        // \\xHH or \\xHHHH… — only accept exactly one byte
        const std::string_view hex = s.substr(2);
        if (hex.size() == 2) {
            if (auto b = parse_hex_byte_pair(hex[0], hex[1])) {
                return *b != 0;
            }
        }
        return std::nullopt;
    }
    if (s.size() >= 4 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
        const std::string_view hex = s.substr(2);
        if (hex.size() == 2) {
            if (auto b = parse_hex_byte_pair(hex[0], hex[1])) {
                return *b != 0;
            }
        }
        return std::nullopt;
    }
    // X'01' / x'01'
    if (s.size() == 5 && (s[0] == 'X' || s[0] == 'x') && s[1] == '\'' && s[4] == '\'') {
        if (auto b = parse_hex_byte_pair(s[2], s[3])) {
            return *b != 0;
        }
    }
    return std::nullopt;
}

}  // namespace

std::optional<bool> try_parse_mariadb_bool_token(std::string_view s) {
    s = trim_view(s);
    if (s.empty()) {
        return std::nullopt;
    }

    // Raw single-byte BIT payload from libmysql / drivers.
    if (s.size() == 1) {
        const auto b = static_cast<unsigned char>(s[0]);
        if (b == 0x00) {
            return false;
        }
        if (b == 0x01) {
            return true;
        }
        // Other single bytes are not treated as bool.
        return std::nullopt;
    }

    if (auto hex_bool = try_parse_one_byte_hex(s)) {
        return hex_bool;
    }

    const std::string lower = to_lower_ascii(s);

    // MariaDB binlog verbose: b'1' / b'0'
    if (lower == "b'1'" || lower == "b\"1\"") {
        return true;
    }
    if (lower == "b'0'" || lower == "b\"0\"") {
        return false;
    }

    if (lower == "1" || lower == "t" || lower == "true" || lower == "yes") {
        return true;
    }
    if (lower == "0" || lower == "f" || lower == "false" || lower == "no") {
        return false;
    }

    return std::nullopt;
}
