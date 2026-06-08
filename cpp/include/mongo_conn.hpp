#pragma once

#include "config.hpp"

#ifdef HAVE_MONGOC
#include <mongoc/mongoc.h>
#endif

#include <string>

#ifdef HAVE_MONGOC

struct MongoConn {
    mongoc_client_t* client{nullptr};

    explicit MongoConn(const MongoSource& src);
    ~MongoConn();

    MongoConn(const MongoConn&) = delete;
    MongoConn& operator=(const MongoConn&) = delete;

    mongoc_database_t* database(const std::string& name);
    mongoc_collection_t* collection(const std::string& database, const std::string& coll);
};

#endif

const MongoSource* find_mongo_source(const AppConfig& cfg, const std::string& conn_id);
