#ifndef CONFIG_DATA_H
#define CONFIG_DATA_H

#include "InsertJobConfig.h"
#include "QueryJobConfig.h"
#include "SubscribeJobConfig.h"
#include <string>
#include <vector>
#include <optional>
#include <variant>


// 全局配置
struct GlobalConfig {
    bool confirm_prompt = false;
    std::string log_dir = "log/";
    std::string cfg_dir = "/etc/taos/";
    std::string host = "localhost";
    int port = 6030;
    std::string user = "root";
    std::string password = "taosdata";
    std::string dsn;

    /**
     * 解析 DSN 字符串并填充 host/port/user/password 字段
     * @param input_dsn 输入 DSN 字符串
     * @throws std::runtime_error 如果解析失败
     */
    void parse_dsn(const std::string& input_dsn);
};


// 定义 JobVariant 别名
using JobVariant = std::variant<InsertJobConfig, QueryJobConfig, SubscribeJobConfig>;


// 顶层配置
struct ConfigData {
    GlobalConfig global;
    int job_concurrency = 1;
    std::vector<JobVariant> jobs;
};


#endif // CONFIG_DATA_H