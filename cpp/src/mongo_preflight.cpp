#include "mongo_preflight.hpp"

#ifdef HAVE_MONGOC

#include <bson/bson.h>

#include <string>

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

std::string bson_utf8_field(const bson_t& doc, const char* key) {
    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, &doc, key) || !BSON_ITER_HOLDS_UTF8(&iter)) {
        return {};
    }
    uint32_t len = 0;
    const char* value = bson_iter_utf8(&iter, &len);
    return value ? std::string(value, len) : std::string();
}

bool bson_bool_field(const bson_t& doc, const char* key) {
    bson_iter_t iter;
    return bson_iter_init_find(&iter, &doc, key) && BSON_ITER_HOLDS_BOOL(&iter) &&
           bson_iter_bool(&iter);
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

    // extras only says what the operator declared; hello says what the server actually is.
    bson_t* cmd = BCON_NEW("hello", BCON_INT32(1));
    bson_t reply;
    bson_error_t error{};
    const bool ok = mongoc_client_command_simple(mongo.client, "admin", cmd, nullptr, &reply, &error);
    bson_destroy(cmd);
    if (!ok) {
        bson_destroy(&reply);
        result.warnings.push_back(std::string("hello command failed: ") + error.message);
        return result;
    }

    const std::string set_name = bson_utf8_field(reply, "setName");
    const std::string msg = bson_utf8_field(reply, "msg");
    const bool is_writable = bson_bool_field(reply, "isWritablePrimary");
    bson_destroy(&reply);

    if (!set_name.empty()) {
        result.facts["replica_set"] = set_name;
        result.facts["is_primary"] = is_writable ? "yes" : "no";
        // The driver already reached this node, so a stale label in extras is a documentation bug,
        // not a reason to block discover.
        if (src.replica_set_in_extras && !src.replica_set.empty() && src.replica_set != set_name) {
            result.warnings.push_back(
                "replica_set mismatch: extras says '" + src.replica_set + "' but the server reports '" +
                set_name + "'");
        }
    } else if (msg == "isdbgrid") {
        result.facts["topology"] = "sharded (mongos)";
    } else {
        result.ok = false;
        result.errors.push_back(
            "server is a standalone node (hello reports no setName): change streams require a replica "
            "set or a sharded cluster");
    }

    return result;
}

#endif
