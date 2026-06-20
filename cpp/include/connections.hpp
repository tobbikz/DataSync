#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <mutex>

/** Mutex guarding AppConfig source vectors during reload and daemon reads. */
std::mutex& app_config_mutex();

/** Load active rows from cdc_catalog.connections into cfg source vectors.
 *  Caller must hold app_config_mutex() if concurrent readers exist. */
int reload_connections_nolock(PGconn* pg, AppConfig& cfg);

/** Load active rows from cdc_catalog.connections into cfg source vectors.
 *  Returns sources loaded (mariadb+mssql+mongo), 0 if none, -1 on SQL error. */
int reload_connections(PGconn* pg, AppConfig& cfg);
