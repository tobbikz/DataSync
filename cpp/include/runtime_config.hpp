#pragma once

#include <libpq-fe.h>

#include <cstddef>
#include <shared_mutex>
#include <string>
#include <unordered_map>

// Hot-reload config from cdc_catalog.runtime_config.
// Lookup order: (key, component, conn_id) -> (key, component, '') -> (key, global, '').
class RuntimeConfig {
public:
    void reload(PGconn* pg);

    int get_int(
        const std::string& key,
        int default_value,
        const std::string& component = "global",
        const std::string& conn_id = "") const;

    std::size_t get_size_t(
        const std::string& key,
        std::size_t default_value,
        const std::string& component = "global",
        const std::string& conn_id = "") const;

    bool get_bool(
        const std::string& key,
        bool default_value,
        const std::string& component = "global",
        const std::string& conn_id = "") const;

    std::string get_string(
        const std::string& key,
        const std::string& default_value,
        const std::string& component = "global",
        const std::string& conn_id = "") const;

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::string> values_;

    static std::string make_lookup_key(
        const std::string& key,
        const std::string& component,
        const std::string& conn_id);

    const std::string* find_raw(
        const std::string& key,
        const std::string& component,
        const std::string& conn_id) const;
};
