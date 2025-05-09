#include "ParameterContext.h"
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <sstream>


ParameterContext::ParameterContext() {}

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
    if (cli_params.count("--host")) config_data.global.host = cli_params["--host"];
    if (cli_params.count("--port")) config_data.global.port = std::stoi(cli_params["--port"]);
    if (cli_params.count("--user")) config_data.global.user = cli_params["--user"];
    if (cli_params.count("--password")) config_data.global.password = cli_params["--password"];
}


void ParameterContext::merge_environment_vars() {
    // 定义需要读取的环境变量
    std::vector<std::pair<std::string, std::string>> env_mappings = {
        {"TAOS_HOST", "host"},
        {"TAOS_PORT", "port"},
        {"TAOS_USER", "user"},
        {"TAOS_PASSWORD", "password"}
    };

    for (const auto& [env_var, key] : env_mappings) {
        const char* env_value = std::getenv(env_var.c_str());
        if (env_value) {
            if (key == "host") config_data.global.host = env_value;
            else if (key == "port") config_data.global.port = std::stoi(env_value);
            else if (key == "user") config_data.global.user = env_value;
            else if (key == "password") config_data.global.password = env_value;
        }
    }
}

void ParameterContext::merge_yaml(const YAML::Node& config) {
    // 解析全局配置
    if (config["global"]) {
        const auto& global = config["global"];
        if (global["host"]) config_data.global.host = global["host"].as<std::string>();
        if (global["port"]) config_data.global.port = global["port"].as<int>();
        if (global["user"]) config_data.global.user = global["user"].as<std::string>();
        if (global["password"]) config_data.global.password = global["password"].as<std::string>();
        if (global["dsn"]) config_data.global.parse_dsn(global["dsn"].as<std::string>());
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

void ParameterContext::parse_jobs(const YAML::Node& jobs_yaml) {
    for (const auto& job : jobs_yaml) {
        std::string job_type = job["job_type"].as<std::string>();
        if (job_type == "insert") {
            parse_insert_job(job);
        } else if (job_type == "query") {
            parse_query_job(job);
        } else if (job_type == "subscribe") {
            parse_subscribe_job(job);
        } else {
            throw std::runtime_error("Unknown job type: " + job_type);
        }
    }
}

void ParameterContext::parse_insert_job(const YAML::Node& job) {
    InsertJobConfig insert_job;
    insert_job.job_name = job["job_name"].as<std::string>();
    insert_job.source.source_type = job["source"]["source_type"].as<std::string>();
    // 继续解析其他字段...
    config_data.jobs.push_back(insert_job);
}

void ParameterContext::parse_query_job(const YAML::Node& job) {
    QueryJobConfig query_job;
    query_job.job_name = job["job_name"].as<std::string>();
    query_job.source.connection.host = job["source"]["connection"]["host"].as<std::string>();
    // 继续解析其他字段...
    config_data.jobs.push_back(query_job);
}

void ParameterContext::parse_subscribe_job(const YAML::Node& job) {
    SubscribeJobConfig subscribe_job;
    subscribe_job.job_name = job["job_name"].as<std::string>();
    subscribe_job.source.connection.host = job["source"]["connection"]["host"].as<std::string>();
    // 继续解析其他字段...
    config_data.jobs.push_back(subscribe_job);
}

const ConfigData& ParameterContext::get_config_data() const {
    return config_data;
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