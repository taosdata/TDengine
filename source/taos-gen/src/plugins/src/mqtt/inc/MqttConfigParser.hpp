#pragma once
#include "ConfigParser.hpp"
#include "MqttConfig.hpp"
#include <yaml-cpp/yaml.h>
#include <set>
#include <stdexcept>

namespace YAML {
    template<>
    struct convert<MqttConfig> {
        static bool decode(const Node& node, MqttConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "uri", "user", "password", "client_id",
                "keep_alive", "clean_session", "max_buffered_messages"
            };
            check_unknown_keys(node, valid_keys, "mqtt config");

            if (node["uri"]) {
                rhs.uri = node["uri"].as<std::string>();
            }
            if (node["user"]) {
                rhs.user = node["user"].as<std::string>();
            }
            if (node["password"]) {
                rhs.password = node["password"].as<std::string>();
            }

            if (node["client_id"]) {
                rhs.client_id = node["client_id"].as<std::string>();
            }

            if (node["keep_alive"]) {
                rhs.keep_alive = node["keep_alive"].as<size_t>();
            }

            if (node["clean_session"]) {
                rhs.clean_session = node["clean_session"].as<bool>();
            }

            if (node["max_buffered_messages"]) {
                rhs.max_buffered_messages = node["max_buffered_messages"].as<size_t>();
                if (rhs.max_buffered_messages == 0) {
                    throw std::runtime_error("max_buffered_messages must be greater than 0 in mqtt config.");
                }
            }

            rhs.enabled = true;
            return true;
        }
    };
} // namespace YAML