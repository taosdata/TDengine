#include "ParameterContext.hpp"
#include "PluginConfigRegistry.hpp"
#include "StepParserRegistry.hpp"
#include "InfluxDBRegistrar.hpp"
#include "InfluxDBFormatOptions.hpp"
#include "InsertDataAction.hpp"

void register_influxdb_plugin_config_hooks() {
    // YAML -> InfluxDBConfig
    PluginConfigRegistry::register_parser("influxdb",
        [](const YAML::Node& node) -> std::any {
            if (node.IsDefined()) {
                return node.as<InfluxDBConfig>();
            } else {
                return InfluxDBConfig{};
            }
        });

    // Formatting configuration decoding
    PluginConfigRegistry::register_format_decoder("influxdb",
        [](const YAML::Node& node, InsertDataConfig& cfg) {
            // Detect unknown configuration keys
            static const std::set<std::string> target_keys = {
                "influxdb", "precision", "batch_size", "gzip", "tbname_key"
            };
            std::set<std::string> valid_keys = YAML::merge_keys<std::string>(
                {YAML::insert_common_keys, target_keys});
            YAML::check_unknown_keys(node, valid_keys, "influxdb/write");

            auto* ic = get_plugin_config_mut<InfluxDBConfig>(cfg.extensions, "influxdb");
            if (!ic) {
                set_plugin_config(cfg.extensions, "influxdb", InfluxDBConfig{});
                ic = get_plugin_config_mut<InfluxDBConfig>(cfg.extensions, "influxdb");
            }
            if (!ic) return;

            if (node["influxdb"]) {
                *ic = node["influxdb"].as<InfluxDBConfig>();
            }

            auto* fmt = get_format_opt_mut<InfluxDBFormatOptions>(cfg.data_format, "influxdb");
            if (!fmt) {
                set_format_opt(cfg.data_format, "influxdb", InfluxDBFormatOptions{});
                fmt = get_format_opt_mut<InfluxDBFormatOptions>(cfg.data_format, "influxdb");
                if (!fmt) return;
            }

            if (node["precision"]) {
                fmt->precision = node["precision"].as<std::string>();
                const std::set<std::string> valid_precisions = {"ns", "us", "ms", "s"};
                if (valid_precisions.find(fmt->precision) == valid_precisions.end()) {
                    throw std::runtime_error(
                        "Invalid precision: " + fmt->precision +
                        ". Supported values are 'ns', 'us', 'ms', 's'.");
                }
            }

            if (node["batch_size"]) {
                int64_t val = node["batch_size"].as<int64_t>();
                if (val <= 0) {
                    throw std::runtime_error("batch_size must be greater than 0");
                }
                fmt->batch_size = static_cast<size_t>(val);
            }

            if (node["gzip"]) {
                fmt->gzip = node["gzip"].as<bool>();
            }

            if (node["tbname_key"]) {
                fmt->tbname_key = node["tbname_key"].as<std::string>();
            }

            cfg.data_format.format_type = "influxdb";
            cfg.data_format.support_tags = true;
        });

    // CLI merger
    PluginConfigRegistry::register_cli_merger("influxdb",
        [](const std::unordered_map<std::string, std::string>& cli, PluginExtensions& exts) {
            auto* ic = get_plugin_config_mut<InfluxDBConfig>(exts, "influxdb");
            if (!ic) {
                set_plugin_config(exts, "influxdb", InfluxDBConfig{});
                ic = get_plugin_config_mut<InfluxDBConfig>(exts, "influxdb");
            }
            if (!ic) return;

            if (auto it = cli.find("--host"); it != cli.end() && !it->second.empty()) {
                if (it->second.find("://") != std::string::npos) {
                    ic->url = it->second;
                } else {
                    ic->url = "http://" + it->second + ":8086";
                }
            }
            if (auto it = cli.find("--password"); it != cli.end() && !it->second.empty()) {
                ic->token = it->second;
            }
        });

    // ENV merger
    PluginConfigRegistry::register_env_merger("influxdb",
        [](PluginExtensions& exts) {
            auto* ic = get_plugin_config_mut<InfluxDBConfig>(exts, "influxdb");
            if (!ic) {
                set_plugin_config(exts, "influxdb", InfluxDBConfig{});
                ic = get_plugin_config_mut<InfluxDBConfig>(exts, "influxdb");
            }
            if (!ic) return;

            const char* env_token = std::getenv("INFLUXDB_TOKEN");
            if (env_token && *env_token) {
                ic->token = env_token;
            }
        });

    // Register action step parser
    StepParserRegistry::register_parser("influxdb/write",
        [](ParameterContext& ctx, Job& job, Step& step) {
            ctx.parse_insert_action(job, step, "influxdb");
        });

    // Register action to ActionFactory
    ActionFactory::instance().register_action(
        "influxdb/write",
        [](const GlobalConfig& global, const ActionConfigVariant& config) {
            return std::make_unique<InsertDataAction>(global, std::get<InsertDataConfig>(config));
        });
}
