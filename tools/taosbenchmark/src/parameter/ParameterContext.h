#ifndef PARAMETER_CONTEXT_H
#define PARAMETER_CONTEXT_H

#include "ConfigData.h"
// #include "ParamDescriptor.h"

#include <nlohmann/json.hpp>
#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <variant>

// class ParameterContext {
// private:
//     std::unordered_map<std::string, ParamEntry> current_params;
//     std::unordered_map<std::string, ParamDescriptor> descriptors;

// public:
//     ParameterContext();
//     void register_param(const ParamDescriptor& desc);
//     void merge_json(const nlohmann::json& config);
//     void merge_commandline(int argc, char* argv[]);
//     void validate();
//     // Other methods...
// };

using json = nlohmann::json;

class ParameterContext {
public:
    ParameterContext();

    // 合并参数来源
    void merge_commandline(int argc, char* argv[]);
    void merge_environment_vars();
    void merge_json(const json& config);

    // 获取参数
    // template <typename T>
    // T get(const std::string& path) const;

    // 获取所有作业
    const ConfigData& get_config_data() const;

private:
    ConfigData config_data; // 顶层配置数据

    // 命令行和环境变量存储
    std::unordered_map<std::string, std::string> cli_params;
    std::unordered_map<std::string, std::string> env_params;

    // 辅助方法
    void parse_jobs(const json& jobs_json);
    void parse_insert_job(const json& job);
    void parse_query_job(const json& job);
    void parse_subscribe_job(const json& job);
};

#endif // PARAMETER_CONTEXT_H