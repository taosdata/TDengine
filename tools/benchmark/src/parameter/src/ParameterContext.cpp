#include "ParameterContext.h"
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <sstream>


ParameterContext::ParameterContext() {}

// 解析全局配置
void ParameterContext::parse_global(const YAML::Node& global_yaml) {
    auto& global_config = config_data.global;
    if (global_yaml["confirm_prompt"]) {
        global_config.confirm_prompt = global_yaml["confirm_prompt"].as<bool>();
    }
    if (global_yaml["log_dir"]) {
        global_config.log_dir = global_yaml["log_dir"].as<std::string>();
    }
    if (global_yaml["cfg_dir"]) {
        global_config.cfg_dir = global_yaml["cfg_dir"].as<std::string>();
    }
    if (global_yaml["connection_info"]) {
        global_config.connection_info = global_yaml["connection_info"].as<ConnectionInfo>();
    }
    if (global_yaml["data_format"]) {
        global_config.data_format = global_yaml["data_format"].as<DataFormat>();
    }
    if (global_yaml["data_channel"]) {
        global_config.data_channel = global_yaml["data_channel"].as<DataChannel>();
    }
    if (global_yaml["database_info"]) {
        global_config.database_info = global_yaml["database_info"].as<DatabaseInfo>();
    }
    if (global_yaml["super_table_info"]) {
        global_config.super_table_info = global_yaml["super_table_info"].as<SuperTableInfo>();
    }
}


void ParameterContext::parse_jobs(const YAML::Node& jobs_yaml) {
    for (const auto& job_node : jobs_yaml) {
        Job job;
        job.key = job_node.first.as<std::string>(); // 获取作业标识符
        const auto& job_content = job_node.second;

        if (job_content["name"]) {
            job.name = job_content["name"].as<std::string>();
        }
        if (job_content["needs"]) {
            job.needs = job_content["needs"].as<std::vector<std::string>>();
        }
        if (job_content["steps"]) {
            parse_steps(job_content["steps"], job.steps);
        }
        config_data.jobs.push_back(job);
    }
}



void ParameterContext::parse_steps(const YAML::Node& steps_yaml, std::vector<Step>& steps) {
    for (const auto& step_node : steps_yaml) {
        Step step;
        step.name = step_node["name"].as<std::string>();
        step.uses = step_node["uses"].as<std::string>();
        if (step_node["with"]) {
            step.with = step_node["with"]; // 保留原始 YAML 节点

            // 根据 uses 字段解析具体行动
            if (step.uses == "actions/create-database") {
                parse_create_database_action(step);
            } else if (step.uses == "actions/create-super-table") {
                parse_create_super_table_action(step);
            } else if (step.uses == "actions/create-child-table") {
                parse_create_child_table_action(step);
            } else if (step.uses == "actions/insert-data") {
                parse_insert_data_action(step);
            } else if (step.uses == "actions/query-data") {
                parse_query_data_action(step);
            } else if (step.uses == "actions/subscribe-data") {
                parse_subscribe_data_action(step);
            } else {
                throw std::runtime_error("Unknown action type: " + step.uses);
            }
            // 其他行动解析逻辑可以在此扩展
        }
        steps.push_back(step);
    }
}


void ParameterContext::parse_create_database_action(Step& step) {
    CreateDatabaseConfig create_db_config;

    // 解析 connection_info（可选）
    if (step.with["connection_info"]) {
        create_db_config.connection_info = step.with["connection_info"].as<ConnectionInfo>();
    } else {
        // 如果未指定 connection_info，则使用全局配置
        create_db_config.connection_info = config_data.global.connection_info;
    }

    // 解析 data_format（可选）
    if (step.with["data_format"]) {
        create_db_config.data_format = step.with["data_format"].as<DataFormat>();
    } else {
        // 如果未指定 data_format，则使用全局配置
        create_db_config.data_format = config_data.global.data_format;
    }

    // 解析 data_channel（可选）
    if (step.with["data_channel"]) {
        create_db_config.data_channel = step.with["data_channel"].as<DataChannel>();
    } else {
        // 如果未指定 data_channel，则使用全局配置
        create_db_config.data_channel = config_data.global.data_channel;
    }

    // 解析 database_info（必需）
    if (step.with["database_info"]) {
        create_db_config.database_info = step.with["database_info"].as<DatabaseInfo>();
    } else {
        throw std::runtime_error("Missing required 'database_info' for create-database action.");
    }

    // 将解析结果保存到 Step 的 action_config 字段
    step.action_config = std::move(create_db_config);

    // 打印解析结果
    std::cout << "Parsed create-database action: " << create_db_config.database_info.name << std::endl;
}


void ParameterContext::parse_create_super_table_action(Step& step) {
    CreateSuperTableConfig create_stb_config;

    // 解析 connection_info（可选）
    if (step.with["connection_info"]) {
        create_stb_config.connection_info = step.with["connection_info"].as<ConnectionInfo>();
    } else {
        // 如果未指定 connection_info，则使用全局配置
        create_stb_config.connection_info = config_data.global.connection_info;
    }

    // 解析 data_format（可选）
    if (step.with["data_format"]) {
        create_stb_config.data_format = step.with["data_format"].as<DataFormat>();
    } else {
        // 如果未指定 data_format，则使用全局配置
        create_stb_config.data_format = config_data.global.data_format;
    }

    // 解析 data_channel（可选）
    if (step.with["data_channel"]) {
        create_stb_config.data_channel = step.with["data_channel"].as<DataChannel>();
    } else {
        // 如果未指定 data_channel，则使用全局配置
        create_stb_config.data_channel = config_data.global.data_channel;
    }

    // 解析 database_info（必需）
    if (step.with["database_info"]) {
        create_stb_config.database_info = step.with["database_info"].as<DatabaseInfo>();
    } else {
        throw std::runtime_error("Missing required 'database_info' for create-super-table action.");
    }

    // 解析 super_table_info（必需）
    if (step.with["super_table_info"]) {
        create_stb_config.super_table_info = step.with["super_table_info"].as<SuperTableInfo>();
    } else {
        throw std::runtime_error("Missing required 'super_table_info' for create-super-table action.");
    }

    // 校验 super_table_info 中的 columns 和 tags
    if (create_stb_config.super_table_info.columns.empty()) {
        throw std::runtime_error("Missing required 'columns' in super_table_info.");    
    }

    // 将解析结果保存到 Step 的 action_config 字段
    step.action_config = std::move(create_stb_config);

    // 打印解析结果
    std::cout << "Parsed create-super-table action: " << create_stb_config.super_table_info.name << std::endl;
}


void ParameterContext::parse_create_child_table_action(Step& step) {
    CreateChildTableConfig create_child_config;

    // 解析 connection_info（可选）
    if (step.with["connection_info"]) {
        create_child_config.connection_info = step.with["connection_info"].as<ConnectionInfo>();
    } else {
        // 如果未指定 connection_info，则使用全局配置
        create_child_config.connection_info = config_data.global.connection_info;
    }

    // 解析 data_format（可选）
    if (step.with["data_format"]) {
        create_child_config.data_format = step.with["data_format"].as<DataFormat>();
    } else {
        // 如果未指定 data_format，则使用全局配置
        create_child_config.data_format = config_data.global.data_format;
    }

    // 解析 data_channel（可选）
    if (step.with["data_channel"]) {
        create_child_config.data_channel = step.with["data_channel"].as<DataChannel>();
    } else {
        // 如果未指定 data_channel，则使用全局配置
        create_child_config.data_channel = config_data.global.data_channel;
    }

    // 解析 database_info（必需）
    if (step.with["database_info"]) {
        create_child_config.database_info = step.with["database_info"].as<DatabaseInfo>();
    } else {
        throw std::runtime_error("Missing required 'database_info' for create-child-table action.");
    }

    // 解析 super_table_info（必需）
    if (step.with["super_table_info"]) {
        create_child_config.super_table_info = step.with["super_table_info"].as<SuperTableInfo>();
    } else {
        throw std::runtime_error("Missing required 'super_table_info' for create-child-table action.");
    }

    // 解析 child_table_info（必需）
    if (step.with["child_table_info"]) {
        create_child_config.child_table_info = step.with["child_table_info"].as<ChildTableInfo>();
    } else {
        throw std::runtime_error("Missing required 'child_table_info' for create-child-table action.");
    }

    // 解析 batch（可选）
    if (step.with["batch"]) {
        create_child_config.batch = step.with["batch"].as<CreateChildTableConfig::BatchConfig>();
    }

    // 将解析结果保存到 Step 的 action_config 字段
    step.action_config = std::move(create_child_config);

    // 打印解析结果
    std::cout << "Parsed create-child-table action for super table: " << create_child_config.super_table_info.name << std::endl;
}


void ParameterContext::parse_insert_data_action(Step& step) {
    InsertDataConfig insert_config;

    if (step.with["source"]) {
        insert_config.source = step.with["source"].as<InsertDataConfig::Source>();
    }
    if (step.with["target"]) {
        insert_config.target = step.with["target"].as<InsertDataConfig::Target>();
    }
    if (step.with["control"]) {
        insert_config.control = step.with["control"].as<InsertDataConfig::Control>();
    }

    // 将解析结果保存到 Step 的 action_config 字段
    step.action_config = std::move(insert_config);

    // 打印解析结果
    std::cout << "Parsed insert-data action." << std::endl;
}


void ParameterContext::parse_query_data_action(Step& step) {
    QueryDataConfig query_config;

    // 解析 source（必需）
    if (step.with["source"]) {
        query_config.source = step.with["source"].as<QueryDataConfig::Source>();
    } else {
        throw std::runtime_error("Missing required 'source' for query-data action.");
    }

    // 解析 control（必需）
    if (step.with["control"]) {
        query_config.control = step.with["control"].as<QueryDataConfig::Control>();
    } else {
        throw std::runtime_error("Missing required 'control' for query-data action.");
    }

    // 将解析结果保存到 Step 的 action_config 字段
    step.action_config = std::move(query_config);

    // 打印解析结果
    std::cout << "Parsed query-data action." << std::endl;
}


void ParameterContext::parse_subscribe_data_action(Step& step) {
    SubscribeDataConfig subscribe_config;

    // 解析 source（必需）
    if (step.with["source"]) {
        subscribe_config.source = step.with["source"].as<SubscribeDataConfig::Source>();
    } else {
        throw std::runtime_error("Missing required 'source' for subscribe-data action.");
    }

    // 解析 control（必需）
    if (step.with["control"]) {
        subscribe_config.control = step.with["control"].as<SubscribeDataConfig::Control>();
    } else {
        throw std::runtime_error("Missing required 'control' for subscribe-data action.");
    }

    // 将解析结果保存到 Step 的 action_config 字段
    step.action_config = std::move(subscribe_config);

    // 打印解析结果
    std::cout << "Parsed subscribe-data action." << std::endl;
}


void ParameterContext::merge_yaml(const YAML::Node& config) {
    // 解析全局配置
    if (config["global"]) {
        parse_global(config["global"]);
    }

    // 解析作业并发数
    if (config["concurrency"]) {
        config_data.concurrency = config["concurrency"].as<int>();
    }

    // 解析作业列表
    if (config["jobs"]) {
        parse_jobs(config["jobs"]);
    }
}









void ParameterContext::merge_commandline(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.find('=') != std::string::npos) {
            auto pos = arg.find('=');
            std::string key = arg.substr(0, pos);
            std::string value = arg.substr(pos + 1);
            cli_params[key] = value;
        }
    }

    // 映射命令行参数到全局配置
    auto& conn_info = config_data.global.connection_info;
    if (cli_params.count("--host")) conn_info.host = cli_params["--host"];
    if (cli_params.count("--port")) conn_info.port = std::stoi(cli_params["--port"]);
    if (cli_params.count("--user")) conn_info.user = cli_params["--user"];
    if (cli_params.count("--password")) conn_info.password = cli_params["--password"];
}


void ParameterContext::merge_environment_vars() {
    // 定义需要读取的环境变量
    std::vector<std::pair<std::string, std::string>> env_mappings = {
        {"TAOS_HOST", "host"},
        {"TAOS_PORT", "port"},
        {"TAOS_USER", "user"},
        {"TAOS_PASSWORD", "password"}
    };

    auto& conn_info = config_data.global.connection_info;
    // 遍历环境变量并更新连接信息
    for (const auto& [env_var, key] : env_mappings) {
        const char* env_value = std::getenv(env_var.c_str());
        if (env_value) {
            if (key == "host") conn_info.host = env_value;
            else if (key == "port") conn_info.port = std::stoi(env_value);
            else if (key == "user") conn_info.user = env_value;
            else if (key == "password") conn_info.password = env_value;
        }
    }
}


const ConfigData& ParameterContext::get_config_data() const {
    return config_data;
}

const GlobalConfig& ParameterContext::get_global_config() const {
    return config_data.global;
}

const ConnectionInfo& ParameterContext::get_connection_info() const {
    return config_data.global.connection_info;
}

const DatabaseInfo& ParameterContext::get_database_info() const {
    return config_data.global.database_info;
}

const SuperTableInfo& ParameterContext::get_super_table_info() const {
    return config_data.global.super_table_info;
}

// template <typename T>
// T ParameterContext::get(const std::string& path) const {
//     // 根据优先级获取参数值
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
//     // 层级维度校验
//     validate_scope_constraints();
    
//     // 类型维度校验
//     validate_type_compatibility();
    
//     // 依赖维度校验
//     validate_dependencies();
    
//     // 冲突维度校验
//     validate_conflicts();
    
//     // 自定义业务规则
//     validate_business_rules();
//   }