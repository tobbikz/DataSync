#include "mariadb_conn.hpp"

#include <stdexcept>

const MariaDbSource* find_mariadb_source(const AppConfig& cfg, const std::string& conn_id) {
    for (const auto& src : cfg.mariadb_sources) {
        if (src.conn_id == conn_id) {
            return &src;
        }
    }
    return nullptr;
}
