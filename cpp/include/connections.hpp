#pragma once

#include "config.hpp"

#include <libpq-fe.h>

/** Load active rows from cdc_catalog.connections into cfg source vectors.
 *  Returns count loaded from DB. 0 = table empty/missing; env fallback in cfg is kept. */
int reload_connections(PGconn* pg, AppConfig& cfg);
