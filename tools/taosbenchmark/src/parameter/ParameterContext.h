#ifndef PARAMETER_CONTEXT_H
#define PARAMETER_CONTEXT_H

#include "ConfigData.h"
// #include "ParamDescriptor.h"

#include <yaml-cpp/yaml.h>
#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <variant>


class ParameterContext {
public:
    ParameterContext();

    // 合并参数来源
    void merge_commandline(int argc, char* argv[]);
    void merge_environment_vars();
    void merge_yaml(const YAML::Node& config); 

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
    void parse_jobs(const YAML::Node& jobs_yaml);
    void parse_insert_job(const YAML::Node& job);
    void parse_query_job(const YAML::Node& job);
    void parse_subscribe_job(const YAML::Node& job);

    // void validate();
};

#endif // PARAMETER_CONTEXT_H