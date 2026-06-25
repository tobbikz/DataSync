#pragma once

#include <string>

std::string epoch_to_timestamptz(long long secs);
std::string epoch_ms_to_timestamptz(long long ms);
std::string epoch_to_date(long long secs);

bool is_invalid_sql_date(const std::string& s);

std::string fix_date_separators(std::string s);

/** True for TIME / time / time without time zone (case-insensitive). */
bool is_time_pg_type(const std::string& pg_type);

/** SQL Server default datetime string → ISO-8601 for PostgreSQL TIMESTAMPTZ. */
std::string mssql_datetime_to_iso(const std::string& s);

/** SQL Server time / CONVERT(114) / FreeTDS dummy-date strings → PostgreSQL TIME. */
std::string mssql_time_to_pg(const std::string& s);

std::string normalize_text_for_pg(const std::string& s, const std::string& pg_type);

std::string pg_escape_literal(const std::string& value);

std::string normalize_pg_sql_literal(const std::string& sql_lit, const std::string& pg_type);
