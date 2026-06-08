#pragma once

#include <string>

bool is_invalid_sql_date(const std::string& s);

std::string fix_date_separators(std::string s);

std::string normalize_text_for_pg(const std::string& s, const std::string& pg_type);

std::string pg_escape_literal(const std::string& value);

std::string normalize_pg_sql_literal(const std::string& sql_lit, const std::string& pg_type);
