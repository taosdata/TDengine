#include "ParameterContext.hpp"
#include "PluginConfigRegistry.hpp"
#include "StepParserRegistry.hpp"
#include "MqttRegistrar.hpp"
#include "MqttFormatOptions.hpp"
#include "InsertDataAction.hpp"

void register_mqtt_plugin_config_hooks() {
    // YAML -> MqttConfig
    PluginConfigRegistry::register_parser("mqtt",
        [](const YAML::Node& node) -> std::any {
            if (node.IsDefined()) {
                return node.as<MqttConfig>();
            } else {
                return MqttConfig{};
            }
        });

    // Formatting configuration decoding
    PluginConfigRegistry::register_format_decoder("mqtt",
        [](const YAML::Node& node, InsertDataConfig& cfg) {
            // Detect unknown configuration keys
            static const std::set<std::string> target_mqtt = {
                "mqtt", "format", "topic", "compression", "encoding", "tbname_key",
                "qos", "retain", "records_per_message"
            };
            std::set<std::string> valid_keys = YAML::merge_keys<std::string>({YAML::insert_common_keys, target_mqtt});
            YAML::check_unknown_keys(node, valid_keys, "mqtt/publish");

            auto* mc = get_plugin_config_mut<MqttConfig>(cfg.extensions, "mqtt");
            if (!mc) {
                set_plugin_config(cfg.extensions, "mqtt", MqttConfig{});
                mc = get_plugin_config_mut<MqttConfig>(cfg.extensions, "mqtt");
            }
            if (!mc) return;

            if (node["mqtt"]) {
                *mc = node["mqtt"].as<MqttConfig>();
            }

            auto* fmt = get_format_opt_mut<MqttFormatOptions>(cfg.data_format, "mqtt");
            if (!fmt) {
                set_format_opt(cfg.data_format, "mqtt", MqttFormatOptions{});
                fmt = get_format_opt_mut<MqttFormatOptions>(cfg.data_format, "mqtt");
                if (!fmt) return;
            }

            if (node["format"]) {
                fmt->content_type = node["format"].as<std::string>();
                if (fmt->content_type != "json") {
                    throw std::runtime_error("Invalid format type for mqtt target: " + fmt->content_type + ". It must be 'json'.");
                }
            } else {
                fmt->content_type = "json";
            }

            if (node["topic"]) fmt->topic = node["topic"].as<std::string>();

            if (node["compression"]) {
                fmt->compression = node["compression"].as<std::string>();
                const std::set<std::string> valid = {"none", "gzip", "lz4", "zstd", ""};
                if (!valid.count(fmt->compression)) {
                    throw std::runtime_error("Invalid compression value: " + fmt->compression + " in mqtt config.");
                }
            }

            if (node["encoding"]) {
                fmt->encoding = node["encoding"].as<std::string>();
                const std::set<std::string> valid = {"NONE", "GBK", "GB18030", "BIG5", "UTF-8"};
                if (!valid.count(fmt->encoding)) {
                    throw std::runtime_error("Invalid encoding value: " + fmt->encoding + " in mqtt config.");
                }
            }

            if (node["tbname_key"]) fmt->tbname_key = node["tbname_key"].as<std::string>();
            if (node["qos"]) {
                fmt->qos = node["qos"].as<size_t>();
                if (fmt->qos > 2) throw std::runtime_error("Invalid QoS value: " + std::to_string(fmt->qos));
            }
            if (node["retain"]) fmt->retain = node["retain"].as<bool>();
            if (node["records_per_message"]) {
                int64_t v = node["records_per_message"].as<int64_t>();
                if (v <= 0) throw std::runtime_error("records_per_message must be greater than 0 in mqtt config.");
                fmt->records_per_message = static_cast<size_t>(v);
            }

            cfg.data_format.format_type = "mqtt";
            cfg.data_format.support_tags = true;
        });

    // CLI merger
    PluginConfigRegistry::register_cli_merger("mqtt",
        [](const std::unordered_map<std::string, std::string>& cli, PluginExtensions& exts) {
            auto* mc = get_plugin_config_mut<MqttConfig>(exts, "mqtt");
            if (!mc) {
                set_plugin_config(exts, "mqtt", MqttConfig{});
                mc = get_plugin_config_mut<MqttConfig>(exts, "mqtt");
            }
            if (!mc) return;

            bool hostport = false;

            if (auto it = cli.find("--host"); it != cli.end() && !it->second.empty()) {
                mc->host = it->second; hostport = true;
            }
            if (auto it = cli.find("--port"); it != cli.end() && !it->second.empty()) {
                try {
                    mc->port = std::stoi(it->second);
                } catch (...) {
                    throw std::runtime_error("Invalid port number: " + it->second);
                }
                hostport = true;
            }
            if (hostport) {
                mc->update_uri_from_host_port();
            }

            if (auto it = cli.find("--user"); it != cli.end()) {
                mc->user = it->second;
            }
            if (auto it = cli.find("--password"); it != cli.end()) {
                mc->password = it->second;
            }
        });

    // ENV merger
    PluginConfigRegistry::register_env_merger("mqtt",
        [](PluginExtensions& exts) {
            auto* mc = get_plugin_config_mut<MqttConfig>(exts, "mqtt");
            if (!mc) {
                set_plugin_config(exts, "mqtt", MqttConfig{});
                mc = get_plugin_config_mut<MqttConfig>(exts, "mqtt");
            }
            if (!mc) return;

            const char* env_host = std::getenv("TAOS_HOST");
            const char* env_port = std::getenv("TAOS_PORT");
            const char* env_user = std::getenv("TAOS_USER");
            const char* env_pass = std::getenv("TAOS_PASSWORD");

            bool hostport = false;

            if (env_host && *env_host) { mc->host = env_host; hostport = true; }
            if (env_port && *env_port) {
                try {
                    mc->port = std::stoi(env_port);
                } catch (...) {
                    throw std::runtime_error(std::string("Invalid TAOS_PORT: ") + env_port);
                }
                hostport = true;
            }
            if (hostport) {
                mc->update_uri_from_host_port();
            }

            if (env_user && *env_user) {
                mc->user = env_user;
            }
            if (env_pass && *env_pass) {
                mc->password = env_pass;
            }
        });

    // Register action step parser
    StepParserRegistry::register_parser("mqtt/publish",
        [](ParameterContext& ctx, Job& job, Step& step) {
            ctx.parse_insert_action(job, step, "mqtt");
        });

    // Register action to ActionFactory
    ActionFactory::instance().register_action(
        "mqtt/publish",
        [](const GlobalConfig& global, const ActionConfigVariant& config) {
            return std::make_unique<InsertDataAction>(global, std::get<InsertDataConfig>(config));
        });
}