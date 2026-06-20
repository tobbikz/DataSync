#pragma once

#include "config.hpp"
#include "mongo_conn.hpp"

#ifdef HAVE_MONGOC

#include <string>
#include <vector>

struct MongoPreflightResult {
    bool ok{true};
    std::vector<std::string> errors;
    std::vector<std::string> warnings;
};

MongoPreflightResult check_mongo_cdc_ready(MongoConn& mongo, const MongoSource& src);

#endif
