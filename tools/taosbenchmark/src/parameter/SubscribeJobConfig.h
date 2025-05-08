#ifndef SUBSCRIBE_JOB_CONFIG_H
#define SUBSCRIBE_JOB_CONFIG_H

#include "CommonConfig.h"
#include <string>
#include <vector>
#include <optional>
#include <map>



// 数据源配置
struct SubscribeSourceConfig {
    ConnectionConfig connection;
};

// 订阅控制策略
struct SubscribeControlConfig {
    std::string log_path = "result.txt";
    bool enable_dryrun = false;

    struct ExecutionConfig {
        int consumer_concurrency = 1;
        std::string create_mode; // "sequential" or "parallel"
        int poll_timeout = 1000; // in milliseconds
    };
    ExecutionConfig execution;

    struct Topic {
        std::string name;
        std::string sql;
    };
    std::vector<Topic> topics;

    struct CommitConfig {
        std::string mode = "auto"; // "auto" or "manual"
    };
    CommitConfig commit;

    struct GroupIDConfig {
        std::string strategy; // "shared", "independent", or "custom"
        std::optional<std::string> custom_id; // Only used if strategy == "custom"
    };
    GroupIDConfig group_id;

    struct OutputConfig {
        std::string file_prefix = "./subscribe_data";
        std::optional<int> expected_rows; // Optional: expected number of rows to consume
    };
    OutputConfig output;

    struct AdvancedConfig {
        std::map<std::string, std::string> parameters; // Advanced parameters as key-value pairs
    };
    AdvancedConfig advanced;
};


// 作业控制配置
struct SubscribeControl {
    DataFormatConfig data_format;
    DataChannelConfig data_channel;
    SubscribeControlConfig subscribe_control;
};

// 订阅作业配置
struct SubscribeJobConfig {
    std::string job_type = "subscribe";
    std::string job_name;
    std::vector<std::string> dependencies;
    SubscribeSourceConfig source;
    SubscribeControl control;
};

#endif // SUBSCRIBE_JOB_CONFIG_H