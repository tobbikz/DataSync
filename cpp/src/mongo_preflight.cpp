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
        std::string detail = "admin ping failed: " + ping_error;
        // replica_set goes into the URI as ?replicaSet=, and the driver refuses to select a node
        // whose setName differs. A wrong label therefore surfaces here as an opaque "no suitable
        // servers", never as a reachable-but-mismatched node.
        if (src.replica_set_in_extras && !src.replica_set.empty()) {
            detail += " (the connection declares replica_set='" + src.replica_set +
                      "'; server selection fails outright when that name does not match the server)";
        }
        result.errors.push_back(detail);
        return result;
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
        if (!src.replica_set_in_extras) {
            result.warnings.push_back(
                "replica_set missing from connections extras (server reports '" + set_name + "')");
        }
    } else if (msg == "isdbgrid") {
        // A mongos is not a replica set: telling the operator to declare one here would put a
        // replicaSet= in the URI and break server selection.
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
