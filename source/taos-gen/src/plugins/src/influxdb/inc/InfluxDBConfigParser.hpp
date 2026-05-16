#pragma once
#include "ConfigParser.hpp"
#include "InfluxDBConfig.hpp"
#include <yaml-cpp/yaml.h>
#include <set>

namespace YAML {
    template<>
    struct convert<InfluxDBConfig> {
        static bool decode(const Node& node, InfluxDBConfig& rhs) {
            static const std::set<std::string> valid_keys = {
                "url", "token", "org", "bucket"
            };
            check_unknown_keys(node, valid_keys, "influxdb config");

            if (node["url"]) {
                rhs.url = node["url"].as<std::string>();
            }
            if (node["token"]) {
                rhs.token = node["token"].as<std::string>();
            }
            if (node["org"]) {
                rhs.org = node["org"].as<std::string>();
            }
            if (node["bucket"]) {
                rhs.bucket = node["bucket"].as<std::string>();
            }

            rhs.enabled = true;
            return true;
        }
    };
} // namespace YAML
