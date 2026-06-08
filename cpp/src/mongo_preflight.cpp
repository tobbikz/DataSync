#include "mongo_preflight.hpp"

#ifdef HAVE_MONGOC

#include <bson/bson.h>

namespace {

bool ping_admin(mongoc_client_t* client, std::string& error_out) {
    bson_t* cmd = BCON_NEW("ping", BCON_INT32(1));
    bson_t reply;
    bson_error_t error{};
    const bool ok = mongoc_client_command_simple(client, "admin", cmd, nullptr, &reply, &error);
    bson_destroy(cmd);
    bson_destroy(&reply);
    if (!ok) {
        error_out = error.message;
    }
    return ok;
}

}  // namespace

MongoPreflightResult check_mongo_cdc_ready(MongoConn& mongo, const MongoSource& src) {
    MongoPreflightResult result;
    if (!mongo.client) {
        result.ok = false;
        result.errors.push_back("mongo client is null");
        return result;
    }

    std::string ping_error;
    if (!ping_admin(mongo.client, ping_error)) {
        result.ok = false;
        result.errors.push_back("admin ping failed: " + ping_error);
        return result;
    }

    if (!src.replica_set_in_extras) {
        result.warnings.push_back("replica_set missing from connections extras (change streams require a replica set)");
    }

    return result;
}

#endif
