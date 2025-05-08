#ifndef QUERY_JOB_CONFIG_H
#define QUERY_JOB_CONFIG_H

#include "CommonConfig.h"
#include <string>
#include <vector>
#include <optional>


// 数据源配置
struct QuerySourceConfig {
    ConnectionConfig connection;
};

// 查询控制策略
struct QueryControlConfig {
    std::string log_path = "result.txt";
    bool enable_dryrun = false;

    struct ExecutionConfig {
        std::string mode = "sequential_per_thread"; // sequential_per_thread, sequential_overall, parallel_per_group, parallel_overall
        int threads = 1;
        int times = 1;
        int interval = 0; // in milliseconds
    };
    ExecutionConfig execution;

    std::string query_type; // "fixed" or "super_table"

    struct FixedQuery {
        struct Query {
            std::string sql;
            std::optional<std::string> output_file;
        };
        std::vector<Query> queries;
    };

    struct SuperTableQuery {
        std::string database_name;
        std::string super_table_name;
        std::string placeholder = "${child_table}";
        struct Template {
            std::string sql_template;
            std::optional<std::string> output_file;
        };
        std::vector<Template> templates;
    };

    std::optional<FixedQuery> fixed;
    std::optional<SuperTableQuery> super_table;
};


// 作业控制配置
struct QueryControl {
    DataFormatConfig data_format;
    DataChannelConfig data_channel;
    QueryControlConfig query_control;
};

// 查询作业配置
struct QueryJobConfig {
    std::string job_type = "query";
    std::string job_name;
    std::vector<std::string> dependencies;
    QuerySourceConfig source;
    QueryControl control;
};

#endif // QUERY_JOB_CONFIG_H
