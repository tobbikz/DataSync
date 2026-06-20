#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <string>

int run_kafka_apply_native_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    int worker_id,
    int worker_count);
