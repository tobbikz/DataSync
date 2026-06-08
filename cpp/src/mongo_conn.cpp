#include "mongo_conn.hpp"

#include <sstream>

const MongoSource* find_mongo_source(const AppConfig& cfg, const std::string& conn_id) {
    for (const auto& m : cfg.mongo_sources) {
        if (m.conn_id == conn_id) {
            return &m;
        }
    }
    return nullptr;
}

#ifdef HAVE_MONGOC

namespace {

std::string build_mongo_uri(const MongoSource& src) {
    std::ostringstream oss;
    oss << "mongodb://";
    if (!src.user.empty()) {
        oss << src.user << ':' << src.password << '@';
    }
    oss << src.host << ':' << src.port << '/';
    if (!src.replica_set.empty()) {
        oss << "?replicaSet=" << src.replica_set;
    }
    return oss.str();
}

}  // namespace

MongoConn::MongoConn(const MongoSource& src) {
    const std::string uri_str = build_mongo_uri(src);
    mongoc_uri_t* uri = mongoc_uri_new(uri_str.c_str());
    if (!uri) {
        throw std::runtime_error("MongoDB invalid URI conn_id=" + src.conn_id);
    }
    client = mongoc_client_new_from_uri(uri);
    mongoc_uri_destroy(uri);
    if (!client) {
        throw std::runtime_error("MongoDB connect failed conn_id=" + src.conn_id);
    }
    mongoc_client_set_appname(client, "datalake-catalog");
}

MongoConn::~MongoConn() {
    if (client) {
        mongoc_client_destroy(client);
    }
}

mongoc_database_t* MongoConn::database(const std::string& name) {
    return mongoc_client_get_database(client, name.c_str());
}

mongoc_collection_t* MongoConn::collection(const std::string& database, const std::string& coll) {
    return mongoc_client_get_collection(client, database.c_str(), coll.c_str());
}

#endif
