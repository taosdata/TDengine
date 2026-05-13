#pragma once
#include "ConfigParser.hpp"
#include "KafkaConfig.hpp"
#include <yaml-cpp/yaml.h>
#include <set>
#include <stdexcept>

namespace YAML {
    template<>
    struct convert<KafkaConfig> {
        static bool decode(const Node& node, KafkaConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "topic", "bootstrap_servers", "client_id", "rdkafka_options"
            };
            check_unknown_keys(node, valid_keys, "kafka config");

            if (node["topic"]) {
                rhs.topic = node["topic"].as<std::string>();
            }

            if (node["bootstrap_servers"]) {
                rhs.bootstrap_servers = node["bootstrap_servers"].as<std::string>();
            }

            if (node["client_id"]) {
                rhs.client_id = node["client_id"].as<std::string>();
            }

            if (node["rdkafka_options"]) {
                if (node["rdkafka_options"].IsMap()) {
                    for (const auto& it : node["rdkafka_options"]) {
                        rhs.rdkafka_options[it.first.as<std::string>()] = it.second.as<std::string>();
                    }
                } else {
                    throw std::runtime_error("rdkafka_options must be a map of string key-value pairs in kafka config.");
                }
            }

            rhs.enabled = true;
            return true;
        }
    };
} // namespace YAML