#include "mongo_conn.hpp"

#include <atomic>
#include <cctype>
#include <iomanip>
#include <mutex>
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

std::once_flag g_mongoc_init_once;
std::atomic<bool> g_mongoc_initialized{false};

std::string uri_percent_encode(const std::string& value) {
    std::ostringstream oss;
    oss << std::uppercase << std::hex << std::setfill('0');
    for (const unsigned char c : value) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            oss << static_cast<char>(c);
        } else {
            oss << '%' << std::setw(2) << static_cast<int>(c);
        }
    }
    return oss.str();
}

std::string build_mongo_uri(const MongoSource& src) {
    std::ostringstream oss;
    oss << "mongodb://";
    if (!src.user.empty()) {
        oss << uri_percent_encode(src.user) << ':' << uri_percent_encode(src.password) << '@';
    }
    oss << src.host << ':' << src.port << '/';
    if (!src.replica_set.empty()) {
        oss << "?replicaSet=" << uri_percent_encode(src.replica_set);
    }
    return oss.str();
}

}  // namespace

void mongo_library_init() {
    std::call_once(g_mongoc_init_once, [] {
        mongoc_init();
        g_mongoc_initialized.store(true);
    });
}

void mongo_library_cleanup() {
    if (g_mongoc_initialized.exchange(false)) {
        mongoc_cleanup();
    }
}

MongoConn::MongoConn(const MongoSource& src) {
    mongo_library_init();
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
