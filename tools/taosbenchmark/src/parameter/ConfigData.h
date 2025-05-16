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
    std::string precision = "ms"; // 默认时间精度为毫秒
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



struct TableNameConfig {
    std::string source_type; // 数据来源类型：generator 或 csv
    struct Generator {
        std::string prefix;
        int count;
        int from = 0; // 默认起始下标为 0
    } generator;
    struct CSV {
        std::string file_path;
        bool has_header = true;
        std::string delimiter = ",";
        int column_index = 0;
    } csv;
};








struct TagsConfig {
    std::string source_type; // 数据来源类型：generator 或 csv
    struct Generator {
        std::vector<SuperTableInfo::Column> schema; // 标签列的 Schema 定义
    } generator;
    struct CSV {
        std::string file_path;
        bool has_header = true;
        std::string delimiter = ",";
        int exclude_index = -1; // 默认不剔除任何列
    } csv;
};





struct ChildTableInfo {
    TableNameConfig table_name;     // 子表名称配置
    TagsConfig tags;                // 标签配置
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
    ConnectionInfo connection_info;  // 数据库连接信息
    DatabaseInfo database_info;      // 数据库信息
    SuperTableInfo super_table_info; // 超级表信息
    ChildTableInfo child_table_info; // 子表信息

    struct BatchConfig {
        int size = 1000;       // 每批创建的子表数量
        int concurrency = 10;  // 并发执行的批次数量
    } batch;
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


using ActionConfigVariant = std::variant<
    std::monostate,
    CreateDatabaseConfig,
    CreateSuperTableConfig,
    CreateChildTableConfig,
    InsertDataConfig
>;

struct Step {
    std::string name; // 步骤名称
    std::string uses; // 使用的操作类型
    YAML::Node with;  // 原始参数配置
    ActionConfigVariant action_config; // 泛化字段，用于存储不同类型的 Action 配置
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




namespace YAML {


    template<>
    struct convert<ConnectionInfo> {
        static bool decode(const Node& node, ConnectionInfo& rhs) {
            if (node["host"]) {
                rhs.host = node["host"].as<std::string>();
            }
            if (node["port"]) {
                rhs.port = node["port"].as<int>();
            }
            if (node["user"]) {
                rhs.user = node["user"].as<std::string>();
            }
            if (node["password"]) {
                rhs.password = node["password"].as<std::string>();
            }
            if (node["dsn"]) {
                rhs.dsn = node["dsn"].as<std::string>();
                rhs.parse_dsn(*rhs.dsn); // 解析 DSN 字符串
            }
            return true;
        }
    };


    template<>
    struct convert<DatabaseInfo> {
        static bool decode(const Node& node, DatabaseInfo& rhs) {
            if (!node["name"]) {
                throw std::runtime_error("Missing required field 'name' in DatabaseInfo.");
            }
            rhs.name = node["name"].as<std::string>();
            if (node["drop_if_exists"]) {
                rhs.drop_if_exists = node["drop_if_exists"].as<bool>();
            }
            if (node["precision"]) {
                rhs.precision = node["precision"].as<std::string>();
                // 验证时间精度是否为合法值
                if (rhs.precision != "ms" && rhs.precision != "us" && rhs.precision != "ns") {
                    throw std::runtime_error("Invalid precision value: " + rhs.precision);
                }
            }
            if (node["properties"]) {
                rhs.properties = node["properties"].as<std::string>();
            }

            return true;
        }
    };



    template<>
    struct convert<SuperTableInfo::Column> {    
        static bool decode(const Node& node, SuperTableInfo::Column& rhs) {
            if (!node["name"]) {
                throw std::runtime_error("Missing required field 'name' for SuperTableInfo::Column.");
            }
            if (!node["type"]) {
                throw std::runtime_error("Missing required field 'type' for SuperTableInfo::Column.");
            }

            rhs.name = node["name"].as<std::string>();
            rhs.type = node["type"].as<std::string>();
            if (node["len"]) rhs.len = node["len"].as<int>();
            if (node["count"]) rhs.count = node["count"].as<int>();
            if (node["precision"]) rhs.precision = node["precision"].as<int>();
            if (node["scale"]) rhs.scale = node["scale"].as<int>();
            if (node["properties"]) rhs.properties = node["properties"].as<std::string>();
            if (node["null_ratio"]) rhs.null_ratio = node["null_ratio"].as<float>();
            if (node["gen_type"]) {
                rhs.gen_type = node["gen_type"].as<std::string>();
                if (*rhs.gen_type == "random") {
                    if (node["min"]) rhs.min = node["min"].as<double>();
                    if (node["max"]) rhs.max = node["max"].as<double>();
                    if (node["dec_min"]) rhs.dec_min = node["dec_min"].as<std::string>();
                    if (node["dec_max"]) rhs.dec_max = node["dec_max"].as<std::string>();
                    if (node["corpus"]) rhs.corpus = node["corpus"].as<std::string>();
                    if (node["chinese"]) rhs.chinese = node["chinese"].as<bool>();
                    if (node["values"]) rhs.values = node["values"].as<std::vector<std::string>>();
                } else if (*rhs.gen_type == "order") {
                    if (node["min"]) rhs.order_min = node["min"].as<double>();
                    if (node["max"]) rhs.order_max = node["max"].as<double>();
                } else if (*rhs.gen_type == "function") {
                    if (node["expression"]) {
                        if (!rhs.function_config) {
                            rhs.function_config = SuperTableInfo::Column::FunctionConfig();
                        }
                        rhs.function_config->expression = node["expression"].as<std::string>();
                    }
                    // SuperTableInfo::Column::FunctionConfig func_config;
                    // if (node["function_config"]) {
                    //     const auto& func_node = node["function_config"];
                    //     if (func_node["expression"]) func_config.expression = func_node["expression"].as<std::string>();
                    //     if (func_node["function"]) func_config.function = func_node["function"].as<std::string>();
                    //     if (func_node["multiple"]) func_config.multiple = func_node["multiple"].as<double>();
                    //     if (func_node["addend"]) func_config.addend = func_node["addend"].as<double>();
                    //     if (func_node["random"]) func_config.random = func_node["random"].as<int>();
                    //     if (func_node["base"]) func_config.base = func_node["base"].as<double>();
                    //     if (func_node["min"]) func_config.min = func_node["min"].as<double>();
                    //     if (func_node["max"]) func_config.max = func_node["max"].as<double>();
                    //     if (func_node["period"]) func_config.period = func_node["period"].as<int>();
                    //     if (func_node["offset"]) func_config.offset = func_node["offset"].as<int>();
                    // }
                    // rhs.function_config = func_config;

                    // SuperTableInfo::Column::FunctionConfig func_config;
                    // func_config.expression = item["function"].as<std::string>(); // 解析完整表达式
                    // // 解析函数表达式的各部分
                    // // 假设函数表达式格式为：<multiple> * <function>(<args>) + <addend> * random(<random>) + <base>
                    // std::istringstream expr_stream(func_config.expression);
                    // std::string token;
                    // while (std::getline(expr_stream, token, '*')) {
                    //     if (token.find("sinusoid") != std::string::npos ||
                    //         token.find("counter") != std::string::npos ||
                    //         token.find("sawtooth") != std::string::npos ||
                    //         token.find("square") != std::string::npos ||
                    //         token.find("triangle") != std::string::npos) {
                    //         func_config.function = token.substr(0, token.find('('));
                    //         // 解析函数参数
                    //         auto args_start = token.find('(') + 1;
                    //         auto args_end = token.find(')');
                    //         auto args = token.substr(args_start, args_end - args_start);
                    //         std::istringstream args_stream(args);
                    //         std::string arg;
                    //         while (std::getline(args_stream, arg, ',')) {
                    //             if (arg.find("min") != std::string::npos) func_config.min = std::stod(arg.substr(arg.find('=') + 1));
                    //             if (arg.find("max") != std::string::npos) func_config.max = std::stod(arg.substr(arg.find('=') + 1));
                    //             if (arg.find("period") != std::string::npos) func_config.period = std::stoi(arg.substr(arg.find('=') + 1));
                    //             if (arg.find("offset") != std::string::npos) func_config.offset = std::stoi(arg.substr(arg.find('=') + 1));
                    //         }
                    //     } else if (token.find("random") != std::string::npos) {
                    //         func_config.random = std::stoi(token.substr(token.find('(') + 1, token.find(')') - token.find('(') - 1));
                    //     } else if (token.find('+') != std::string::npos) {
                    //         func_config.addend = std::stod(token.substr(0, token.find('+')));
                    //         func_config.base = std::stod(token.substr(token.find('+') + 1));
                    //     } else {
                    //         func_config.multiple = std::stod(token);
                    //     }
                    // }
                    // column.function_config = func_config;
    
                }
            }
            return true;
        }
    };



    template<>
    struct convert<TableNameConfig> {
        static bool decode(const Node& node, TableNameConfig& rhs) {
            if (!node["source_type"]) {
                throw std::runtime_error("Missing required 'source_type' in TableNameConfig.");
            }
            rhs.source_type = node["source_type"].as<std::string>();
    
            if (rhs.source_type == "generator") {
                if (!node["generator"]) {
                    throw std::runtime_error("Missing required 'generator' configuration for source_type 'generator'.");
                }
                const auto& generator = node["generator"];
                if (generator["prefix"]) {
                    rhs.generator.prefix = generator["prefix"].as<std::string>();
                }
                if (generator["count"]) {
                    rhs.generator.count = generator["count"].as<int>();
                }
                if (generator["from"]) {
                    rhs.generator.from = generator["from"].as<int>();
                }
            } else if (rhs.source_type == "csv") {
                if (!node["csv"]) {
                    throw std::runtime_error("Missing required 'csv' configuration for source_type 'csv'.");
                }
                const auto& csv = node["csv"];
                if (csv["file_path"]) {
                    rhs.csv.file_path = csv["file_path"].as<std::string>();
                }
                if (csv["has_header"]) {
                    rhs.csv.has_header = csv["has_header"].as<bool>();
                }
                if (csv["delimiter"]) {
                    rhs.csv.delimiter = csv["delimiter"].as<std::string>();
                }
                if (csv["column_index"]) {
                    rhs.csv.column_index = csv["column_index"].as<int>();
                }
            } else {
                throw std::runtime_error("Invalid 'source_type' in TableNameConfig: " + rhs.source_type);
            }
    
            return true;
        }
    };


    template<>
    struct convert<TagsConfig> {
        static bool decode(const Node& node, TagsConfig& rhs) {
            if (!node["source_type"]) {
                throw std::runtime_error("Missing required field 'source_type' in TagsConfig.");
            }
            rhs.source_type = node["source_type"].as<std::string>();
            if (rhs.source_type == "generator") {
                if (!node["generator"]) {
                    throw std::runtime_error("Missing required 'generator' configuration for source_type 'generator'.");
                }
                if (node["generator"]["schema"]) {
                    for (const auto& item : node["generator"]["schema"]) {
                        rhs.generator.schema.push_back(item.as<SuperTableInfo::Column>());
                    }
                }
            } else if (rhs.source_type == "csv") {
                const auto& csv = node["csv"];
                if (csv["file_path"]) rhs.csv.file_path = csv["file_path"].as<std::string>();
                if (csv["has_header"]) rhs.csv.has_header = csv["has_header"].as<bool>();
                if (csv["delimiter"]) rhs.csv.delimiter = csv["delimiter"].as<std::string>();
                if (csv["exclude_index"]) rhs.csv.exclude_index = csv["exclude_index"].as<int>();
            } else {
                throw std::runtime_error("Invalid source_type for tags in child_table_info.");
            }
            return true;
        }
    };


    template<>
    struct convert<SuperTableInfo> {
        static bool decode(const Node& node, SuperTableInfo& rhs) {
            if (!node["name"]) {
                throw std::runtime_error("Missing required field 'name' in SuperTableInfo.");
            }
            rhs.name = node["name"].as<std::string>();
            if (node["columns"]) {
                for (const auto& item : node["columns"]) {
                    rhs.columns.push_back(item.as<SuperTableInfo::Column>());
                }
            }
            if (node["tags"]) {
                for (const auto& item : node["tags"]) {
                    rhs.tags.push_back(item.as<SuperTableInfo::Column>());
                }
            }
            return true;
        }
    };

    template<>
    struct convert<ChildTableInfo> {
        static bool decode(const Node& node, ChildTableInfo& rhs) {
            if (!node["table_name"]) {
                throw std::runtime_error("Missing required field 'table_name' in ChildTableInfo.");
            }
            if (!node["tags"]) {
                throw std::runtime_error("Missing required field 'tags' in ChildTableInfo.");
            }
    
            rhs.table_name = node["table_name"].as<TableNameConfig>();
            rhs.tags = node["tags"].as<TagsConfig>();
    
            return true;
        }
    };

    template<>
    struct convert<CreateChildTableConfig::BatchConfig> {
        static bool decode(const Node& node, CreateChildTableConfig::BatchConfig& rhs) {
            if (node["size"]) {
                rhs.size = node["size"].as<int>();
            }
            if (node["concurrency"]) {
                rhs.concurrency = node["concurrency"].as<int>();
            }
            return true;
        }
    };




}

    


#endif // CONFIG_DATA_H