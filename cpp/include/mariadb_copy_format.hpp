#pragma once

#include "mariadb_schema.hpp"

#include <string>

/** COPY CSV cell for one MariaDB column value (NULL → unquoted empty field, not \\N). Returns false if PK is null. */
bool mariadb_format_copy_cell(const char* data, unsigned long len, const MariaDbColumn& col, std::string& out);
