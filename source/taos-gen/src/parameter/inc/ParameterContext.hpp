#pragma once

#include "ConfigParser.hpp"
#include "ConfigData.hpp"
#include "PluginConfigRegistry.hpp"

#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <variant>


class ParameterContext {
public:
    ParameterContext();

    bool parse_args(int argc, char* argv[]);

    bool has_cli_param(const std::string& param) const;

    bool init_global(int argc, char* argv[]);
    void init_jobs();
    bool init(int argc, char* argv[]);
    void show_help();
    void show_version();

    // Merge parameter sources
    void parse_commandline(int argc, char* argv[]);
    void merge_commandline();
    void merge_commandline(int argc, char* argv[]);
    void merge_environment_vars();
    void merge_yaml_global(const YAML::Node& config);
    void merge_yaml_jobs(const YAML::Node& config);
    void merge_yaml(const YAML::Node& config);
    void merge_yaml(const std::string& file_path);
    void merge_all();
    void merge_all_global();
    void merge_all_jobs();

    // Get parameter
    // template <typename T>
    // T get(const std::string& path) const;

    // Get global config and connection info
    const ConfigData& get_config_data() const;
    const GlobalConfig& get_global_config() const;
    const TDengineConfig& get_tdengine() const;
    const DatabaseInfo& get_database_info() const;
    const SuperTableInfo& get_super_table_info() const;

    // Get log file path
    std::string get_log_file_path() const;
    std::string get_log_dir() const;

    void parse_insert_action(Job& job, Step& step, std::string target_type);

private:
    // int concurrency = 1;
    ConfigData config_data; // Top-level config data
    YAML::Node cached_config_;

    // Command line and environment variable storage
    std::unordered_map<std::string, std::string> cli_params;
    std::unordered_map<std::string, std::string> env_params;

    // Helper methods
    YAML::Node load_default_config();
    YAML::Node load_config(const std::string& file_path);
    void parse_schema(const YAML::Node& schema_node);
    void parse_jobs(const YAML::Node& jobs_node);
    void parse_steps(const YAML::Node& steps_node, Job& job);

    void register_core_step_parsers();
    void prepare_work();
    void parse_td_create_database_action(Job& job, Step& step);
    void parse_td_create_super_table_action(Job& job, Step& step);
    void parse_td_create_child_table_action(Job& job, Step& step);
    void parse_query_action(Job& job, Step& step);
    void parse_subscribe_action(Job& job, Step& step);

    // void validate();

private:
    // Command option structure definition
    struct CommandOption {
        std::string long_opt;    // Long option (e.g. "--host")
        char short_opt;          // Short option (e.g. 'h')
        std::string description; // Option description
        bool requires_value;     // Whether value is required
    };

    // List of valid command options
    static const std::vector<CommandOption> valid_options;
};