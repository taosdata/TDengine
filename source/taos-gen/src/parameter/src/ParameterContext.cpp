#include "ParameterContext.hpp"
#include "StepParserRegistry.hpp"
#include "LogUtils.hpp"
#include "version.hpp"
#include "CheckpointAction.hpp"
#include "InsertDataConfig.hpp"
#include "CreateSuperTableConfig.hpp"
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include "FilesystemCompat.hpp"
#include "CheckpointAction.hpp"
#include <filesystem>


ParameterContext::ParameterContext() {
    register_core_step_parsers();
}

// Define static member variable
const std::vector<ParameterContext::CommandOption> ParameterContext::valid_options = {
    {"--host", 'h', "Specify FQDN to connect server", true},
    {"--port", 'P', "The TCP/IP port number to use for the connection", true},
    {"--user", 'u', "The user name to use when connecting to the server", true},
    {"--password", 'p', "The password to use when connecting to the server", true},
    {"--config-file", 'c', "Specify config file path", true},
    {"--log-dir", 'd', "Specify log output directory (default: ./log)", true},
    {"--log-file", 'o', "Specify complete log file path (overrides --log-dir)", true},
    {"--verbose", 'v', "Increase output verbosity", false},
    {"--version", 'V', "Output version information", false},
    {"--help", '?', "Display this help message", false}
    // Add more command options here
};

void ParameterContext::show_help() {
    LogUtils::info("Usage: taosgen [OPTIONS]...");
    LogUtils::info("");
    LogUtils::info("Options:");

    // Calculate the longest option length for alignment
    size_t max_opt_len = 0;
    for (const auto& opt : valid_options) {
        size_t total_len = 4 + opt.long_opt.length(); // 4 = length of "-X, "
        max_opt_len = std::max(max_opt_len, total_len);
    }

    // Reserve fixed space for VALUE
    const size_t value_width = 8;
    const size_t desc_offset = max_opt_len + value_width;

    // Output help info for each option
    for (const auto& opt : valid_options) {
        std::ostringstream oss;

        // Output short and long option
        oss << "  -" << opt.short_opt << ", " << opt.long_opt;

        // Calculate current output length
        size_t current_len = 4 + opt.long_opt.length();

        // Output VALUE (if needed) and spaces
        if (opt.requires_value) {
            oss << "=VALUE";
            current_len += 6;
        }

        // Calculate padding for alignment
        size_t padding = desc_offset - current_len;
        oss << std::string(padding, ' ');

        // Output description
        oss << opt.description;
        LogUtils::info(oss.str());
    }

    LogUtils::info("");
    LogUtils::info("Examples:");
    LogUtils::info("  taosgen --config-file=example.yaml");
    LogUtils::info("  taosgen -h localhost -P 6041 -u root -p taosdata");
    LogUtils::info("");
    LogUtils::info("For more information, visit: https://docs.taosdata.com/");
    LogUtils::info("");
}

void ParameterContext::show_version() {
    LogUtils::info("taosgen version: {}", TAOSGEN_VERSION);
    LogUtils::info("git: {}", TSGEN_BUILD_GIT);
    LogUtils::info("build: {}-{} {}", TSGEN_BUILD_TARGET_OSTYPE, TSGEN_BUILD_TARGET_CPUTYPE, TSGEN_BUILD_DATE);
}

void ParameterContext::parse_schema(const YAML::Node& schema_node) {
    auto& global_config = config_data.global;
    global_config.schema = schema_node.as<SchemaConfig>();

    // if (!global_config.schema.tbname.enabled && !global_config.schema.from_csv.enabled) {
    //     throw std::runtime_error("Missing required field 'tbname' or 'from_csv' in schema.");
    // }

    if (global_config.schema.columns.size() == 0) {
        throw std::runtime_error("Schema must have at least one column defined.");
    }
}

void ParameterContext::parse_jobs(const YAML::Node& jobs_node) {
    for (const auto& job_node : jobs_node) {
        Job job;
        job.extensions = config_data.global.extensions;
        job.schema = config_data.global.schema;

        job.key = job_node.first.as<std::string>(); // Get job identifier
        const auto& job_content = job_node.second;

        // Detect unknown configuration keys
        static const std::set<std::string> valid_keys = {
            "name", "needs", "steps"
        };
        YAML::check_unknown_keys(job_content, valid_keys, "job");

        if (job_content["name"]) {
            job.name = job_content["name"].as<std::string>();
        } else {
            job.name = job.key;
        }

        if (job_content["needs"]) {
            job.needs = job_content["needs"].as<std::vector<std::string>>();
        }

        if (job_content["steps"]) {
            parse_steps(job_content["steps"], job);
        }
        config_data.jobs.emplace_back(std::move(job));
    }

    prepare_work();
}

void ParameterContext::prepare_work() {
    for (auto& job : config_data.jobs) {
        if (job.find_create) {
            continue;
        }

        if (job.need_create) {
            bool has_create_db_dependency = false;
            for (const auto& dep_key : job.needs) {
                auto it = std::find_if(config_data.jobs.begin(), config_data.jobs.end(),
                    [&dep_key](const Job& j) { return j.key == dep_key; });
                if (it != config_data.jobs.end() && it->find_create) {
                    has_create_db_dependency = true;
                    break;
                }
            }

            if (has_create_db_dependency) {
                continue;
            }

            const auto* tc = get_plugin_config<TDengineConfig>(job.extensions, "tdengine");
            if (tc == nullptr) {
                throw std::runtime_error("TDengine configuration not found for job: " + job.name);
            }

            CheckpointInfo ci;
            CreateDatabaseConfig cdc;
            cdc.tdengine = *tc;
            cdc.checkpoint_info = ci;

            Step step;
            step.name = "Create Database";
            step.uses = "tdengine/create-database";
            step.with = YAML::Node(YAML::NodeType::Map);
            step.action_config = cdc;

            job.steps.insert(
                job.steps.begin(),
                step
            );
        }
    }
}

void ParameterContext::parse_steps(const YAML::Node& steps_node, Job& job) {
    for (const auto& step_node : steps_node) {
        Step step;

        // Detect unknown configuration keys
        static const std::set<std::string> valid_keys = {
            "name", "uses", "with"
        };
        YAML::check_unknown_keys(step_node, valid_keys, "job::steps");

        if (step_node["uses"]) {
            step.uses = step_node["uses"].as<std::string>();
        } else {
            throw std::runtime_error("Missing required field 'uses' for step in job: " + job.name);
        }

        if (step_node["name"]) {
            step.name = step_node["name"].as<std::string>();
        } else {
            step.name = step.uses;
        }

        if (step_node["with"]) {
            step.with = step_node["with"];
        } else {
            step.with = YAML::Node(YAML::NodeType::Map);
        }

        // Parse action by uses field
        if (!StepParserRegistry::apply(step.uses, *this, job, step)) {
            throw std::runtime_error("Unknown action type: " + step.uses);
        }

        job.steps.emplace_back(std::move(step));
    }

    // Validate tag types for schemaless + create-super-table compatibility.
    // TDengine schemaless (InfluxDB line protocol) always parses tags as NCHAR.
    // If create-super-table creates tags with non-NCHAR types (e.g., VARCHAR,
    // BINARY, INT), schemaless inserts will fail with type mismatch errors.
    bool has_schemaless = false;
    bool has_create_stb = false;
    for (const auto& step : job.steps) {
        if (auto* cfg = std::get_if<InsertDataConfig>(&step.action_config)) {
            if (cfg->data_format.format_type == "schemaless") {
                has_schemaless = true;
            }
        }
        if (std::holds_alternative<CreateSuperTableConfig>(step.action_config)) {
            has_create_stb = true;
        }
    }
    if (has_schemaless && has_create_stb) {
        for (const auto& tag : job.schema.tags) {
            if (tag.type_tag != ColumnTypeTag::NCHAR) {
                throw std::runtime_error(
                    "Tag '" + tag.name + "' has type '" + tag.type +
                    "', but TDengine schemaless (InfluxDB line protocol) requires all tags "
                    "to be NCHAR type. Please change the tag type to 'nchar' in your YAML "
                    "configuration to be compatible with schemaless inserts.");
            }
        }

        // Note: TDengine schemaless uses '_ts' as the default timestamp column
        // name (configurable via smlTsDefaultName on the server side). We do not
        // enforce a specific name here because users may have customized it.
    }
}

void ParameterContext::parse_td_create_database_action(Job& job, Step& step) {
    CreateDatabaseConfig create_db_config = step.with.as<CreateDatabaseConfig>();

    const auto* tc = get_plugin_config<TDengineConfig>(job.extensions, "tdengine");
    if (tc == nullptr) {
        throw std::runtime_error("TDengine configuration not found for job: " + job.name);
    }

    create_db_config.tdengine = *tc;

    // Parse database_info (required)
    if (step.with["checkpoint"]) {
        create_db_config.checkpoint_info = step.with["checkpoint"].as<CheckpointInfo>();
    }
    // Print parse result
    LogUtils::info("Parsed create-database action: {}", create_db_config.tdengine.database);

    // Save result to Step's action_config field
    step.action_config = std::move(create_db_config);
    job.find_create = true;
}

void ParameterContext::parse_td_create_super_table_action(Job& job, Step& step) {
    CreateSuperTableConfig create_stb_config = step.with.as<CreateSuperTableConfig>();

    const auto* tc = get_plugin_config<TDengineConfig>(job.extensions, "tdengine");
    if (tc == nullptr) {
        throw std::runtime_error("TDengine configuration not found for job: " + job.name);
    }

    create_stb_config.tdengine = *tc;
    create_stb_config.schema = job.schema;

    if (step.with["schema"]) {
        const auto& schema = step.with["schema"];

        if (schema["name"]) {
            create_stb_config.schema.name = schema["name"].as<std::string>();
        }

        if (schema["from_csv"]) {
            create_stb_config.schema.from_csv = schema["from_csv"].as<FromCSVConfig>();
        }

        if (schema["tbname"]) {
            create_stb_config.schema.tbname = schema["tbname"].as<TableNameConfig>();
        }

        if (schema["columns"]) {
            create_stb_config.schema.columns = schema["columns"].as<ColumnConfigVector>();
        }

        if (schema["tags"]) {
            create_stb_config.schema.tags = schema["tags"].as<ColumnConfigVector>();
        }

        if (schema["generation"]) {
            create_stb_config.schema.generation = schema["generation"].as<GenerationConfig>();
        }
        create_stb_config.schema.apply();
    }

    // Validate columns and tags
    if (create_stb_config.schema.columns.empty()) {
        throw std::runtime_error("Missing required 'columns' in schema.");
    }

    // Print parse result
    LogUtils::info("Parsed create-super-table action: {}", create_stb_config.schema.name);

    // Save result to Step's action_config field
    job.schema = create_stb_config.schema;
    step.action_config = std::move(create_stb_config);
    job.need_create = true;
}

void ParameterContext::parse_td_create_child_table_action(Job& job, Step& step) {
    CreateChildTableConfig create_ctb_config = step.with.as<CreateChildTableConfig>();

    const auto* tc = get_plugin_config<TDengineConfig>(job.extensions, "tdengine");
    if (tc == nullptr) {
        throw std::runtime_error("TDengine configuration not found for job: " + job.name);
    }

    create_ctb_config.tdengine = *tc;
    create_ctb_config.schema = job.schema;

    if (step.with["schema"]) {
        const auto& schema = step.with["schema"];

        if (schema["name"]) {
            create_ctb_config.schema.name = schema["name"].as<std::string>();
        }

        if (schema["from_csv"]) {
            create_ctb_config.schema.from_csv = schema["from_csv"].as<FromCSVConfig>();
        }

        if (schema["tbname"]) {
            create_ctb_config.schema.tbname = schema["tbname"].as<TableNameConfig>();
        }

        if (schema["columns"]) {
            create_ctb_config.schema.columns = schema["columns"].as<ColumnConfigVector>();
        }

        if (schema["tags"]) {
            create_ctb_config.schema.tags = schema["tags"].as<ColumnConfigVector>();
        }

        if (schema["generation"]) {
            create_ctb_config.schema.generation = schema["generation"].as<GenerationConfig>();
        }
        create_ctb_config.schema.apply();
    }

    // Parse batch (optional)
    if (step.with["batch"]) {
        create_ctb_config.batch = step.with["batch"].as<CreateChildTableConfig::BatchConfig>();
    }

    // Print parse result
    LogUtils::info("Parsed create-child-table action for super table: {}", create_ctb_config.schema.name);

    // Save result to Step's action_config field
    job.schema = create_ctb_config.schema;
    step.action_config = std::move(create_ctb_config);
}

void ParameterContext::register_core_step_parsers() {
    StepParserRegistry::register_parser("tdengine/create-database",
        [](ParameterContext& ctx, Job& job, Step& step) {
            ctx.parse_td_create_database_action(job, step);
        });

    StepParserRegistry::register_parser("tdengine/create-super-table",
        [](ParameterContext& ctx, Job& job, Step& step) {
            ctx.parse_td_create_super_table_action(job, step);
        });

    StepParserRegistry::register_parser("tdengine/create-child-table",
        [](ParameterContext& ctx, Job& job, Step& step) {
            ctx.parse_td_create_child_table_action(job, step);
        });

    StepParserRegistry::register_parser("tdengine/query",
        [](ParameterContext& ctx, Job& job, Step& step) {
            ctx.parse_query_action(job, step);
        });

    StepParserRegistry::register_parser("tdengine/subscribe",
        [](ParameterContext& ctx, Job& job, Step& step) {
            ctx.parse_subscribe_action(job, step);
        });
}

void ParameterContext::parse_insert_action(Job& job, Step& step, std::string target_type) {
    step.with["target"] = target_type;
    InsertDataConfig insert_config = step.with.as<InsertDataConfig>();

    insert_config.target_type = target_type;
    insert_config.extensions = job.extensions;
    insert_config.schema = job.schema;

    PluginConfigRegistry::parse_into_extensions(step.with, insert_config.extensions, true);

    if (step.with["schema"]) {
        const auto& schema = step.with["schema"];

        if (schema["name"]) {
            insert_config.schema.name = schema["name"].as<std::string>();
        }

        if (schema["from_csv"]) {
            insert_config.schema.from_csv = schema["from_csv"].as<FromCSVConfig>();
        }

        if (schema["tbname"]) {
            insert_config.schema.tbname = schema["tbname"].as<TableNameConfig>();
        }

        if (schema["columns"]) {
            insert_config.schema.columns = schema["columns"].as<ColumnConfigVector>();
        }

        if (schema["tags"]) {
            insert_config.schema.tags = schema["tags"].as<ColumnConfigVector>();
        }

        if (schema["generation"]) {
            insert_config.schema.generation = schema["generation"].as<GenerationConfig>();
        }
        insert_config.schema.apply();
    }

    if (!step.with["timestamp_precision"]) {
        insert_config.timestamp_precision = insert_config.schema.columns[0].ts.get_precision();
    }

    if (!insert_config.schema.generation.generate_threads.has_value()) {
        if (job.schema.generation.generate_threads.has_value()) {
            insert_config.schema.generation.generate_threads = job.schema.generation.generate_threads;
        } else {
            insert_config.schema.generation.generate_threads = insert_config.insert_threads;
        }
    }

    if (insert_config.checkpoint_info.enabled) {
        if (insert_config.schema.generation.data_cache.enabled) {
            LogUtils::warn("Configuration 'data_cache.enabled' is set to 'false' because 'checkpoint' is enabled");
            insert_config.schema.generation.data_cache.enabled = false;
        }
    }

    if (insert_config.data_format.support_tags) {
        if (insert_config.schema.tags_cfg.source_type != "generator") {
            if (insert_config.schema.tags.size() > 0) {
                throw std::runtime_error("Configuration 'data_format." + insert_config.data_format.format_type +
                                         "' does not support tags from source type '" +
                                         insert_config.schema.tags_cfg.source_type + "'");
            }
        }
    } else {
        insert_config.schema.tags.clear();
        insert_config.schema.tags_cfg.clear_schema();
    }

    // Print parse result
    LogUtils::info("Parsed {} action", step.uses);

    // Save result to Step's action_config field
    job.extensions = insert_config.extensions;
    job.schema = insert_config.schema;
    step.action_config = std::move(insert_config);
}

void ParameterContext::parse_query_action(Job& /*job*/, Step& step) {
    QueryDataConfig query_config;

    // Parse source (required)
    if (step.with["source"]) {
        query_config.source = step.with["source"].as<QueryDataConfig::Source>();
    } else {
        throw std::runtime_error("Missing required 'source' for query-data action.");
    }

    // Parse control (required)
    if (step.with["control"]) {
        query_config.control = step.with["control"].as<QueryDataConfig::Control>();
    } else {
        throw std::runtime_error("Missing required 'control' for query-data action.");
    }

    // Print parse result
    LogUtils::info("Parsed query-data action");

    // Save result to Step's action_config field
    step.action_config = std::move(query_config);
}

void ParameterContext::parse_subscribe_action(Job& /*job*/, Step& step) {
    SubscribeDataConfig subscribe_config;

    // Parse source (required)
    if (step.with["source"]) {
        subscribe_config.source = step.with["source"].as<SubscribeDataConfig::Source>();
    } else {
        throw std::runtime_error("Missing required 'source' for subscribe-data action.");
    }

    // Parse control (required)
    if (step.with["control"]) {
        subscribe_config.control = step.with["control"].as<SubscribeDataConfig::Control>();
    } else {
        throw std::runtime_error("Missing required 'control' for subscribe-data action.");
    }

    // Print parse result
    LogUtils::info("Parsed subscribe-data action");

    // Save result to Step's action_config field
    step.action_config = std::move(subscribe_config);
}

void ParameterContext::merge_yaml_global(const YAML::Node& config) {
    PluginConfigRegistry::parse_into_extensions(config, config_data.global.extensions, false);

    if (config["schema"]) {
        parse_schema(config["schema"]);
    } else {
        YAML::Node schema = load_default_config()["schema"];
        parse_schema(schema);
    }

    // Parse job concurrency
    if (config["concurrency"]) {
        config_data.concurrency = config["concurrency"].as<int>();
    }

    // Parse log path from YAML
    if (config["log_dir"]) {
        config_data.global.log_dir = config["log_dir"].as<std::string>();
    }
    if (config["log_file"]) {
        config_data.global.log_file = config["log_file"].as<std::string>();
    }
}

void ParameterContext::merge_yaml_jobs(const YAML::Node& config) {
    // Parse job list
    if (config["jobs"]) {
        parse_jobs(config["jobs"]);
    }

    if (!config["concurrency"]) {
        config_data.concurrency = static_cast<int>(config_data.jobs.size());
    }
}

void ParameterContext::merge_yaml(const YAML::Node& config) {
    merge_yaml_global(config);
    merge_yaml_jobs(config);
}

void ParameterContext::merge_yaml(const std::string& file_path) {
    YAML::Node config = load_config(file_path);
    merge_yaml(config);
}

YAML::Node ParameterContext::load_default_config() {
    return YAML::Load(R"(
tdengine:
  dsn: taos+ws://root:taosdata@127.0.0.1:6041/tsbench
  drop_if_exists: false
  props: precision 'ms' vgroups 4

schema:
  name: meters
  tbname:
    prefix: d
    count: 10000
    from: 0
  columns:
    - name: ts
      type: timestamp
      start: 1735660800000
      precision : ms
      step: 1
    - name: current
      type: float
      min: 0
      max: 100
    - name: voltage
      type: int
      min: 200
      max: 240
    - name: phase
      type: float
      expr: _i * math.pi % 180
  tags:
    - name: groupid
      type: int
      min: 1
      max: 10
    - name: location
      type: binary(24)
      values:
        - New York
        - Los Angeles
        - Chicago
        - Houston
        - Phoenix
        - Philadelphia
        - San Antonio
        - San Diego
        - Dallas
        - Austin
  generation:
    concurrency: 8
    rows_per_table: 10000
    rows_per_batch: 10000

jobs:
  # TDengine insert job
  insert-data:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 1000
            concurrency: 10

      - uses: tdengine/insert
        with:
          concurrency: 8
)");
}

YAML::Node ParameterContext::load_config(const std::string& file_path) {
    try {
        // Load YAML file
        config_data.global.yaml_cfg_dir = file_path;
        return YAML::LoadFile(file_path);
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to load yaml file '" + file_path + "': " + e.what());
    }
}

void ParameterContext::parse_commandline(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        std::string key, value;

        // Handle long option format (--key=value)
        if (arg.substr(0, 2) == "--") {
            size_t pos = arg.find('=');
            if (pos != std::string::npos) {
                // Handle long option with value (--key=value)
                key = arg.substr(0, pos);
                value = arg.substr(pos + 1);
            } else {
                // Handle long option without value (--key)
                key = arg;
                value = "";
            }

            // Validate long option
            auto it = std::find_if(valid_options.begin(), valid_options.end(),
                [&key](const CommandOption& opt) { return opt.long_opt == key; });

            if (it == valid_options.end()) {
                throw std::runtime_error("Unknown option: " + key);
            }

            // Check if value is required
            if (it->requires_value) {
                if (pos == std::string::npos) {
                    // Try to get value from next argv
                    if (i + 1 >= argc) {
                        throw std::runtime_error("Option requires a value: " + key);
                    }
                    value = argv[++i];
                }
            }

            cli_params[key] = value;
        }
        // Handle short option format (-k value)
        else if (arg[0] == '-') {
            // Check short option format length
            if (arg.length() != 2) {
                throw std::runtime_error("Invalid short option format '" + arg + "'. Must be single character after '-'");
            }

            char short_opt = arg[1];

            // Deprecated: -f was renamed to -o; kept for backward compatibility
            if (short_opt == 'f') {
                LogUtils::warn("Option '-f' is deprecated and will be removed in a future version. Please use '-o' or '--log-file' instead.");
                if (i + 1 >= argc) {
                    throw std::runtime_error("Option requires a value: -f");
                }
                cli_params["--log-file"] = argv[++i];
                continue;
            }

            auto it = std::find_if(valid_options.begin(), valid_options.end(),
                [short_opt](const CommandOption& opt) { return opt.short_opt == short_opt; });

            if (it == valid_options.end()) {
                throw std::runtime_error("Unknown option: " + arg);
            }

            key = it->long_opt;
            if (it->requires_value) {
                if (i + 1 >= argc) {
                    throw std::runtime_error("Option requires a value: " + key);
                }
                value = argv[++i];
            } else {
                value = "";
            }

            cli_params[key] = value;
        }
        else {
            throw std::runtime_error("Unknown argument: " + arg);
        }
    }
}

void ParameterContext::merge_commandline(int argc, char* argv[]) {
    parse_commandline(argc, argv);
    merge_commandline();
}

void ParameterContext::merge_commandline() {
    PluginConfigRegistry::apply_cli_mergers(cli_params, config_data.global.extensions);

    if (cli_params.count("--verbose")) {
        config_data.global.verbose = true;
    }
    if (cli_params.count("--log-dir")) {
        config_data.global.log_dir = cli_params.at("--log-dir");
    }
    if (cli_params.count("--log-file")) {
        config_data.global.log_file = cli_params.at("--log-file");
    }
}

void ParameterContext::merge_environment_vars() {
    PluginConfigRegistry::apply_env_mergers(config_data.global.extensions);
}

void ParameterContext::merge_all_global() {
    cached_config_ = YAML::Node(YAML::NodeType::Map);

    if (cli_params.count("--config-file")) {
        const std::string& config_file = cli_params["--config-file"];
        cached_config_ = load_config(config_file);
    } else {
        cached_config_ = load_default_config();
    }

    if (cli_params.count("--verbose")) {
        YAML::Emitter emitter;
        emitter << cached_config_;
        LogUtils::info("Loaded YAML Config:\n{}", emitter.c_str());
    }

    merge_yaml_global(cached_config_);
    merge_environment_vars();
    merge_commandline();
}

void ParameterContext::merge_all_jobs() {
    merge_yaml_jobs(cached_config_);
}

void ParameterContext::merge_all() {
    merge_all_global();
    merge_all_jobs();
}

bool ParameterContext::parse_args(int argc, char* argv[]) {
    parse_commandline(argc, argv);

    if (cli_params.count("--help")) {
        show_help();
        return false;
    } else if (cli_params.count("--version")) {
        show_version();
        return false;
    }

    return true;
}

bool ParameterContext::has_cli_param(const std::string& param) const {
    return cli_params.count(param) > 0;
}

bool ParameterContext::init_global(int argc, char* argv[]) {
    if (!parse_args(argc, argv)) {
        return false;
    }
    merge_all_global();
    return true;
}

void ParameterContext::init_jobs() {
    merge_all_jobs();
}

bool ParameterContext::init(int argc, char* argv[]) {
    if (!init_global(argc, argv)) {
        return false;
    }
    init_jobs();
    return true;
}

const ConfigData& ParameterContext::get_config_data() const {
    return config_data;
}

const GlobalConfig& ParameterContext::get_global_config() const {
    return config_data.global;
}

const TDengineConfig& ParameterContext::get_tdengine() const {
    const auto* tc = get_plugin_config<TDengineConfig>(config_data.global.extensions, "tdengine");
    if (tc == nullptr) {
        throw std::runtime_error("TDengine configuration not found in global extensions.");
    }

    return *tc;
}

const DatabaseInfo& ParameterContext::get_database_info() const {
    return config_data.global.database_info;
}

const SuperTableInfo& ParameterContext::get_super_table_info() const {
    return config_data.global.super_table_info;
}

std::string ParameterContext::get_log_file_path() const {
    if (!config_data.global.log_file.empty()) {
        return config_data.global.log_file;
    }

    std::string log_dir = config_data.global.log_dir;
    if (!log_dir.empty() && log_dir.back() == '/') {
        log_dir.pop_back();
    }
    return log_dir + "/taosgen.log";
}

std::string ParameterContext::get_log_dir() const {
    if (!config_data.global.log_file.empty()) {
        fs::path p(config_data.global.log_file);
        fs::path parent = p.parent_path();
        return parent.empty() ? "." : parent.string();
    }
    return config_data.global.log_dir;
}

// template <typename T>
// T ParameterContext::get(const std::string& path) const {
//     // Get parameter value by priority
//     if (cli_params.count(path)) {
//         return cli_params.at(path);
//     }
//     if (env_params.count(path)) {
//         return env_params.at(path);
//     }
//     if (json_config.contains(path)) {
//         return json_config.at(path).get<T>();
//     }
//     throw std::runtime_error("Parameter not found: " + path);
// }


// void validate() {
//     // Scope validation
//     validate_scope_constraints();
//
//     // Type validation
//     validate_type_compatibility();
//
//     // Dependency validation
//     validate_dependencies();
//
//     // Conflict validation
//     validate_conflicts();
//
//     // Custom business rules
//     validate_business_rules();
//   }