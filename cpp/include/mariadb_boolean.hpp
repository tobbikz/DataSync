#pragma once

#include <optional>
#include <string_view>

/** Parse common MariaDB BIT(1)/bool wire tokens into a C++ bool.
 *
 * Recognizes: 0/1, t/f, true/false, yes/no, b'0'/b'1', \\x00/\\x01, 0x00/0x01, X'00'/X'01',
 * and a single raw byte \\x00 or \\x01.
 * Returns nullopt when the token is not a known boolean form.
 */
std::optional<bool> try_parse_mariadb_bool_token(std::string_view s);
