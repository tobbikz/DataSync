#include "runtime_config.hpp"

#include <nlohmann/json.hpp>

namespace {

std::string json_scalar_to_string(const std::string& raw) {
    const auto j = nlohmann::json::parse(raw, nullptr, false);
    if (j.is_number_integer() || j.is_number_unsigned()) {
        return std::to_string(j.get<long long>());
    }
    if (j.is_number_float()) {
        return std::to_string(static_cast<long long>(j.get<double>()));
    }
    if (j.is_boolean()) {
        return j.get<bool>() ? "true" : "false";
    }
    if (j.is_string()) {
        return j.get<std::string>();
    }
    return raw;
}

}  // namespace

std::string RuntimeConfig::make_lookup_key(
    const std::string& key,
    const std::string& component,
    const std::string& conn_id) {
    return component + "\x1f" + conn_id + "\x1f" + key;
}

void RuntimeConfig::reload(PGconn* pg) {
    values_.clear();
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        return;
    }

    PGresult* res = PQexec(
        pg,
        "SELECT config_key, component, conn_id, config_value::text FROM cdc_catalog.runtime_config");
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return;
    }

    for (int i = 0; i < PQntuples(res); ++i) {
        values_[make_lookup_key(
            PQgetvalue(res, i, 0),
            PQgetvalue(res, i, 1),
            PQgetvalue(res, i, 2))] = json_scalar_to_string(PQgetvalue(res, i, 3));
    }
    PQclear(res);
}

const std::string* RuntimeConfig::find_raw(
    const std::string& key,
    const std::string& component,
    const std::string& conn_id) const {
    if (auto it = values_.find(make_lookup_key(key, component, conn_id)); it != values_.end()) {
        return &it->second;
    }
    if (!conn_id.empty()) {
        if (auto it = values_.find(make_lookup_key(key, component, "")); it != values_.end()) {
            return &it->second;
        }
    }
    if (component != "global") {
        if (auto it = values_.find(make_lookup_key(key, "global", "")); it != values_.end()) {
            return &it->second;
        }
    }
    return nullptr;
}

int RuntimeConfig::get_int(
    const std::string& key,
    int default_value,
    const std::string& component,
    const std::string& conn_id) const {
    const std::string* raw = find_raw(key, component, conn_id);
    if (!raw) {
        return default_value;
    }
    try {
        return std::stoi(*raw);
    } catch (...) {
        return default_value;
    }
}

std::size_t RuntimeConfig::get_size_t(
    const std::string& key,
    std::size_t default_value,
    const std::string& component,
    const std::string& conn_id) const {
    const int v = get_int(key, static_cast<int>(default_value), component, conn_id);
    return v < 0 ? default_value : static_cast<std::size_t>(v);
}

bool RuntimeConfig::get_bool(
    const std::string& key,
    bool default_value,
    const std::string& component,
    const std::string& conn_id) const {
    const std::string* raw = find_raw(key, component, conn_id);
    if (!raw) {
        return default_value;
    }
    if (*raw == "true" || *raw == "1") {
        return true;
    }
    if (*raw == "false" || *raw == "0") {
        return false;
    }
    return default_value;
}

std::string RuntimeConfig::get_string(
    const std::string& key,
    const std::string& default_value,
    const std::string& component,
    const std::string& conn_id) const {
    const std::string* raw = find_raw(key, component, conn_id);
    return raw ? *raw : default_value;
}
