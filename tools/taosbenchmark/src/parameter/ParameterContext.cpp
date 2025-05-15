#include "ParameterContext.h"
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <sstream>


ParameterContext::ParameterContext() {}

// 解析全局配
void ParameterContext::parse_columns_or_tags(const YAML::Node& node, std::vector<SuperTableInfo::Column>& target) {
    for (const auto& item : node) {
        SuperTableInfo::Column column;
        column.name = item["name"].as<std::string>();
        column.type = item["type"].as<std::string>();
        if (item["len"]) column.len = item["len"].as<int>();
        if (item["count"]) column.count = item["count"].as<int>();
        if (item["precision"]) column.precision = item["precision"].as<int>();
        if (item["scale"]) column.scale = item["scale"].as<int>();
        if (item["properties"]) column.properties = item["properties"].as<std::string>();
        if (item["null_ratio"]) column.null_ratio = item["null_ratio"].as<float>();
        if (item["gen_type"]) {
            column.gen_type = item["gen_type"].as<std::string>();
            if (*column.gen_type == "random") {
                if (item["min"]) column.min = item["min"].as<double>();
                if (item["max"]) column.max = item["max"].as<double>();
                if (item["dec_min"]) column.dec_min = item["dec_min"].as<std::string>();
                if (item["dec_max"]) column.dec_max = item["dec_max"].as<std::string>();
                if (item["corpus"]) column.corpus = item["corpus"].as<std::string>();
                if (item["chinese"]) column.chinese = item["chinese"].as<bool>();
                if (item["values"]) column.values = item["values"].as<std::vector<std::string>>();
            } else if (*column.gen_type == "order") {
                if (item["min"]) column.order_min = item["min"].as<double>();
                if (item["max"]) column.order_max = item["max"].as<double>();
            } else if (*column.gen_type == "function") {
                SuperTableInfo::Column::FunctionConfig func_config;
                func_config.expression = item["function"].as<std::string>(); // 解析完整表达式
                // 解析函数表达式的各部分
                // 假设函数表达式格式为：<multiple> * <function>(<args>) + <addend> * random(<random>) + <base>
                std::istringstream expr_stream(func_config.expression);
                std::string token;
                while (std::getline(expr_stream, token, '*')) {
                    if (token.find("sinusoid") != std::string::npos ||
                        token.find("counter") != std::string::npos ||
                        token.find("sawtooth") != std::string::npos ||
                        token.find("square") != std::string::npos ||
                        token.find("triangle") != std::string::npos) {
                        func_config.function = token.substr(0, token.find('('));
                        // 解析函数参数
                        auto args_start = token.find('(') + 1;
                        auto args_end = token.find(')');
                        auto args = token.substr(args_start, args_end - args_start);
                        std::istringstream args_stream(args);
                        std::string arg;
                        while (std::getline(args_stream, arg, ',')) {
                            if (arg.find("min") != std::string::npos) func_config.min = std::stod(arg.substr(arg.find('=') + 1));
                            if (arg.find("max") != std::string::npos) func_config.max = std::stod(arg.substr(arg.find('=') + 1));
                            if (arg.find("period") != std::string::npos) func_config.period = std::stoi(arg.substr(arg.find('=') + 1));
                            if (arg.find("offset") != std::string::npos) func_config.offset = std::stoi(arg.substr(arg.find('=') + 1));
                        }
                    } else if (token.find("random") != std::string::npos) {
                        func_config.random = std::stoi(token.substr(token.find('(') + 1, token.find(')') - token.find('(') - 1));
                    } else if (token.find('+') != std::string::npos) {
                        func_config.addend = std::stod(token.substr(0, token.find('+')));
                        func_config.base = std::stod(token.substr(token.find('+') + 1));
                    } else {
                        func_config.multiple = std::stod(token);
                    }
                }
                column.function_config = func_config;
            }
        }
        target.push_back(column);
    }
}


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
        const auto& conn = global_yaml["connection_info"];
        auto& conn_info = global_config.connection_info;
        if (conn["host"]) conn_info.host = conn["host"].as<std::string>();
        if (conn["port"]) conn_info.port = conn["port"].as<int>();
        if (conn["user"]) conn_info.user = conn["user"].as<std::string>();
        if (conn["password"]) conn_info.password = conn["password"].as<std::string>();
        if (conn["dsn"]) conn_info.dsn = conn["dsn"].as<std::string>();
    }
    if (global_yaml["database_info"]) {
        const auto& db = global_yaml["database_info"];
        auto& db_info = global_config.database_info;
        if (db["name"]) db_info.name = db["name"].as<std::string>();
        if (db["drop_if_exists"]) db_info.drop_if_exists = db["drop_if_exists"].as<bool>();
        if (db["properties"]) db_info.properties = db["properties"].as<std::string>();
    }
    if (global_yaml["super_table_info"]) {
        const auto& stb = global_yaml["super_table_info"];
        auto& stb_info = global_config.super_table_info;
        if (stb["name"]) stb_info.name = stb["name"].as<std::string>();
        if (stb["columns"]) {
            parse_columns_or_tags(stb["columns"], stb_info.columns);
        }
        if (stb["tags"]) {
            parse_columns_or_tags(stb["tags"], stb_info.tags);
        }
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
            for (const auto& step_node : job_content["steps"]) {
                Step step;
                step.name = step_node["name"].as<std::string>();
                step.uses = step_node["uses"].as<std::string>();
                if (step_node["with"]) {
                    step.with = step_node["with"];
                }
                job.steps.push_back(step);
            }
        }
        config_data.jobs.push_back(job);
    }
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