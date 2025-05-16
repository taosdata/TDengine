#ifndef CONFIG_DATA_H
#define CONFIG_DATA_H

#include "InsertJobConfig.h"
#include "QueryJobConfig.h"
#include "SubscribeJobConfig.h"
#include <string>
#include <vector>
#include <optional>
#include <variant>
#include <yaml-cpp/yaml.h>





struct ConnectionInfo {
    std::string host = "localhost";
    int port = 6030;
    std::string user = "root";
    std::string password = "taosdata";
    std::optional<std::string> dsn;

    /**
     * 解析 DSN 字符串并填充 host/port/user/password 字段
     * @param input_dsn 输入 DSN 字符串
     * @throws std::runtime_error 如果解析失败
     */
    void parse_dsn(const std::string& input_dsn);


};

struct DatabaseInfo {
    std::string name;
    std::string precision;
    bool drop_if_exists = true;
    std::optional<std::string> properties;
};


struct SuperTableInfo {
    std::string name;

    struct Column {
        std::string name;
        std::string type;
        std::optional<int> len;
        int count = 1;
        std::optional<int> precision;
        std::optional<int> scale;
        std::optional<std::string> properties;
        std::optional<std::string> gen_type;
        std::optional<float> null_ratio;

        // Attributes for gen_type=random
        std::optional<double> min;
        std::optional<double> max;
        std::optional<std::string> dec_min;
        std::optional<std::string> dec_max;
        std::optional<std::string> corpus;
        std::optional<bool> chinese;
        std::optional<std::vector<std::string>> values;

        // Attributes for gen_type=order
        std::optional<double> order_min;
        std::optional<double> order_max;

        // Attributes for gen_type=function
        struct FunctionConfig {
            std::string expression; // 完整的函数表达式
            std::string function;   // 函数名，例如 sinusoid、counter 等
            double multiple = 1.0;  // 倍率
            double addend = 0.0;    // 加数
            int random = 0;         // 随机部分的范围
            double base = 0.0;      // 基值
            std::optional<double> min; // 函数参数：最小值
            std::optional<double> max; // 函数参数：最大值
            std::optional<int> period; // 函数参数：周期
            std::optional<int> offset; // 函数参数：偏移量
        };
        std::optional<FunctionConfig> function_config;
    };

    std::vector<Column> columns;
    std::vector<Column> tags;
};




struct GlobalConfig {
    bool confirm_prompt = false;
    std::string log_dir = "log/";
    std::string cfg_dir = "/etc/taos/";
    ConnectionInfo connection_info;
    DatabaseInfo database_info;
    SuperTableInfo super_table_info;

};


struct CreateDatabaseConfig {
    ConnectionInfo connection_info;
    DatabaseInfo database_info;
};


struct CreateSuperTableConfig {
    ConnectionInfo connection_info;
    DatabaseInfo database_info;
    SuperTableInfo super_table_info;
};


struct CreateChildTableConfig {
    ConnectionInfo connection_info;
    DatabaseInfo database_info;
    SuperTableInfo super_table_info;
};


struct InsertDataConfig {
    struct Source {
        std::string table_name;
        std::string source_type;
        // 其他字段...
    };
    struct Target {
        std::string database_name;
        std::string super_table_name;
        // 其他字段...
    };
    struct Control {
        int concurrency;
        int batch_size;
        // 其他字段...
    };

    Source source;
    Target target;
    Control control;
};


struct Step {
    std::string name; // 步骤名称
    std::string uses; // 使用的操作类型
    YAML::Node with;  // 原始参数配置
    std::variant<std::monostate, CreateDatabaseConfig, CreateSuperTableConfig, InsertDataConfig> action_config; 
    // 泛化字段，用于存储不同类型的 Action 配置
};




struct Job {
    std::string key;               // 作业标识符
    std::string name;              // 作业显示名称
    std::vector<std::string> needs; // 依赖的作业列表
    std::vector<Step> steps;       // 作业的步骤列表
};


// 顶层配置
struct ConfigData {
    GlobalConfig global;
    int concurrency = 1;
    std::vector<Job> jobs; // 存储作业列表
};


#endif // CONFIG_DATA_H