#include "ParameterContext.hpp"
#include "PluginConfigRegistry.hpp"
#include "StepParserRegistry.hpp"
#include "TDengineRegistrar.hpp"
#include "SqlFormatOptions.hpp"
#include "StmtFormatOptions.hpp"
#include "InsertDataAction.hpp"

void register_tdengine_plugin_config_hooks() {
    // YAML -> TDengineConfig
    PluginConfigRegistry::register_parser("tdengine",
        [](const YAML::Node& node) -> std::any {
            if (node.IsDefined()) {
                return node.as<TDengineConfig>();
            } else {
                return TDengineConfig{};
            }
        });

    // Formatting configuration decoding
    PluginConfigRegistry::register_format_decoder("tdengine",
        [](const YAML::Node& node, InsertDataConfig& cfg) {
            // Detect unknown configuration keys
            static const std::set<std::string> target_tdengine = {
                "tdengine", "format", "auto_create_table"
            };

            std::set<std::string> valid_keys = YAML::merge_keys<std::string>({YAML::insert_common_keys, target_tdengine});
            YAML::check_unknown_keys(node, valid_keys, "tdengine/insert");

            auto* tc = get_plugin_config_mut<TDengineConfig>(cfg.extensions, "tdengine");
            if (!tc) {
                set_plugin_config(cfg.extensions, "tdengine", TDengineConfig{});
                tc = get_plugin_config_mut<TDengineConfig>(cfg.extensions, "tdengine");
            }
            if (!tc) return;

            if (node["tdengine"]) {
                *tc = node["tdengine"].as<TDengineConfig>();
            }

            if (node["format"]) {
                cfg.data_format.format_type = node["format"].as<std::string>();
                if (cfg.data_format.format_type != "sql" && cfg.data_format.format_type != "stmt") {
                    throw std::runtime_error("Invalid format type for tdengine target: " + cfg.data_format.format_type + ". It must be 'sql' or 'stmt'.");
                }
            } else {
                cfg.data_format.format_type = "stmt";
            }

            // sql
            {
                auto* fmt = get_format_opt_mut<SqlFormatOptions>(cfg.data_format, "sql");
                if (!fmt) {
                    set_format_opt(cfg.data_format, "sql", SqlFormatOptions{});
                    fmt = get_format_opt_mut<SqlFormatOptions>(cfg.data_format, "sql");
                    if (!fmt) return;
                }

                if (node["auto_create_table"]) {
                    fmt->auto_create_table = node["auto_create_table"].as<bool>();
                }

                cfg.data_format.support_tags = fmt->auto_create_table;
            }

            // stmt
            {
                auto* fmt = get_format_opt_mut<StmtFormatOptions>(cfg.data_format, "stmt");
                if (!fmt) {
                    set_format_opt(cfg.data_format, "stmt", StmtFormatOptions{});
                    fmt = get_format_opt_mut<StmtFormatOptions>(cfg.data_format, "stmt");
                    if (!fmt) return;
                }

                if (node["auto_create_table"]) {
                    fmt->auto_create_table = node["auto_create_table"].as<bool>();
                }

                cfg.data_format.support_tags = fmt->auto_create_table;
            }
        });

    // CLI merger
    PluginConfigRegistry::register_cli_merger("tdengine",
        [](const std::unordered_map<std::string, std::string>& cli, PluginExtensions& exts) {
            auto* cfg = get_plugin_config_mut<TDengineConfig>(exts, "tdengine");
            if (!cfg) {
                set_plugin_config(exts, "tdengine", TDengineConfig{});
                cfg = get_plugin_config_mut<TDengineConfig>(exts, "tdengine");
            }
            if (!cfg) return;

            if (auto it = cli.find("--host"); it != cli.end() && !it->second.empty()) {
                cfg->host = it->second;
            }
            if (auto it = cli.find("--port"); it != cli.end() && !it->second.empty()) {
                try { cfg->port = std::stoi(it->second); }
                catch (...) {
                    throw std::runtime_error("Invalid port number: " + it->second);
                }
            }
            if (auto it = cli.find("--user"); it != cli.end()) {
                cfg->user = it->second;
            }
            if (auto it = cli.find("--password"); it != cli.end()) {
                cfg->password = it->second;
            }
        });

    // ENV merger
    PluginConfigRegistry::register_env_merger("tdengine",
        [](PluginExtensions& exts) {
            auto* cfg = get_plugin_config_mut<TDengineConfig>(exts, "tdengine");
            if (!cfg) {
                set_plugin_config(exts, "tdengine", TDengineConfig{});
                cfg = get_plugin_config_mut<TDengineConfig>(exts, "tdengine");
            }
            if (!cfg) return;

            const char* env_host = std::getenv("TAOS_HOST");
            const char* env_port = std::getenv("TAOS_PORT");
            const char* env_user = std::getenv("TAOS_USER");
            const char* env_pass = std::getenv("TAOS_PASSWORD");

            if (env_host && *env_host) {
                cfg->host = env_host;
            }
            if (env_port && *env_port) {
                try {
                    cfg->port = std::stoi(env_port);
                }
                catch (...) {
                    throw std::runtime_error(std::string("Invalid TAOS_PORT: ") + env_port);
                }
            }
            if (env_user && *env_user) {
                cfg->user = env_user;
            }
            if (env_pass && *env_pass) {
                cfg->password = env_pass;
            }
        });

    // Register action step parser
    StepParserRegistry::register_parser("tdengine/insert",
        [](ParameterContext& ctx, Job& job, Step& step) {
            ctx.parse_insert_action(job, step, "tdengine");
            CheckpointAction::checkpoint_recover(ctx.get_global_config(),
                                                 std::get<InsertDataConfig>(step.action_config));
        });

    // Register action to ActionFactory
    ActionFactory::instance().register_action(
        "tdengine/insert",
        [](const GlobalConfig& global, const ActionConfigVariant& config) {
            return std::make_unique<InsertDataAction>(global, std::get<InsertDataConfig>(config));
        });
}