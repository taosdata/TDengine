#pragma once
#include "ConfigParser.hpp"
#include "TDengineConfig.hpp"
#include <yaml-cpp/yaml.h>
#include <set>
#include <stdexcept>

namespace YAML {
    template<>
    struct convert<TDengineConfig> {
        static bool decode(const Node& node, TDengineConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "dsn", "drop_if_exists", "props", "pool"
            };
            check_unknown_keys(node, valid_keys, "tdengine config");

            if (node["dsn"]) {
                rhs.dsn = node["dsn"].as<std::string>();
                rhs.parse_dsn();
            }
            if (node["drop_if_exists"]) {
                rhs.drop_if_exists = node["drop_if_exists"].as<bool>();
            }
            if (node["props"]) {
                rhs.properties = node["props"].as<std::string>();
            }
            if (node["pool"]) {
                const auto& pool_node = node["pool"];

                // Detect unknown keys in pool
                static const std::set<std::string> pool_keys = {"enabled", "max_size", "min_size", "timeout"};
                check_unknown_keys(pool_node, pool_keys, "tdengine config::connection pool");

                if (pool_node["enabled"]) rhs.pool.enabled = pool_node["enabled"].as<bool>();
                if (pool_node["max_size"]) rhs.pool.max_size = pool_node["max_size"].as<size_t>();
                if (pool_node["min_size"]) rhs.pool.min_size = pool_node["min_size"].as<size_t>();
                if (pool_node["timeout"]) rhs.pool.timeout = pool_node["timeout"].as<size_t>();
            }

            rhs.enabled = true;
            return true;
        }
    };
} // namespace YAML