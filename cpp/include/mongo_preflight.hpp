#pragma once

#include "config.hpp"
#include "mongo_conn.hpp"

#ifdef HAVE_MONGOC

#include <map>
#include <string>
#include <vector>

struct MongoPreflightResult {
    bool ok{true};
    std::vector<std::string> errors;
    std::vector<std::string> warnings;
    /** Observed topology (replica set name, primary, wire version, ...) for the report. */
    std::map<std::string, std::string> facts;
};

MongoPreflightResult check_mongo_cdc_ready(MongoConn& mongo, const MongoSource& src);

#endif
