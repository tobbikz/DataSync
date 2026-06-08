#pragma once

#include <string>

std::string sanitize_pg_identifier_part(const std::string& name, std::size_t max_len = 63);
std::string mssql_pg_schema_name(const std::string& database_name, const std::string& schema_name);
std::string mssql_pg_table_name(const std::string& table_name);
