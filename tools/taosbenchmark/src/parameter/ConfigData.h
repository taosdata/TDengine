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


struct TimestampGeneratorConfig {
    std::string start_timestamp = "now";
    std::string timestamp_precision = "ms";
    int timestamp_step = 1;
};


struct ColumnsConfig {
    std::string source_type; // 数据来源类型：generator 或 csv
    struct Generator {
        std::vector<SuperTableInfo::Column> schema; // 普通列的 Schema 定义
        struct TimestampStrategy {
            TimestampGeneratorConfig generator_config;
        } timestamp_strategy;
    } generator;
    struct CSV {
        std::string file_path;
        bool has_header = true;
        std::string delimiter = ",";
        struct TimestampStrategy {
            std::string strategy_type = "original";
            struct OriginalConfig {
                int column_index = 0;
                std::string precision = "ms";
                std::string offset_config;
            } original_config;
            TimestampGeneratorConfig generator_config;
        } timestamp_strategy;
    } csv;
};


struct DataFormat {
    std::string format_type = "sql";
    struct StmtConfig {
        std::string version = "v2"; // "v1" or "v2"
    } stmt_config;
    struct SchemalessConfig {
        std::string protocol  = "line"; // "line" "telnet" "json" or "taos-json"
    } schemaless_config;
    struct CSVConfig {
        std::string delimiter = ","; // 默认分隔符为逗号
        std::string quote_character = "\""; // 默认引号字符
        std::string escape_character = "\\"; // 默认转义字符
    } csv_config;
};


struct DataChannel {
    std::string channel_type = "native";    // "native" "websocket" "restful" or "file_stream"
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
        TableNameConfig table_name; // 子表名称配置
        TagsConfig tags;            // 标签列配置
        ColumnsConfig columns;      // 普通列配置
    } source;

    struct Target {
        std::string timestamp_precision ;   // 时间戳精度：ms、us、ns
        std::string target_type; // 数据目标类型：tdengine 或 file_system
        struct TDengine {
            ConnectionInfo connection_info;
            DatabaseInfo database_info;
            SuperTableInfo super_table_info;
        } tdengine;
        struct FileSystem {
            std::string output_dir;
            std::string file_prefix = "data";
            std::string timestamp_format;
            std::string timestamp_interval = "1d";
            bool include_header = true;
            std::string tbname_col_alias = "device_id";
            std::string compression_level = "none";
        } file_system;
    } target;

    struct Control {
        DataFormat data_format;
        DataChannel data_channel;
        struct DataQuality {
            struct DataDisorder {
                bool enabled = false;
                struct Interval {
                    std::string time_start;
                    std::string time_end;
                    double ratio = 0.0; // 比例
                    int latency_range = 0; // 延迟范围
                };
                std::vector<Interval> intervals; // 乱序时间区间
            } data_disorder; // 数据乱序配置
        } data_quality;
        struct DataGeneration {
            struct InterlaceMode {
                bool enabled = false;
                int rows = 1; // 行数
            } interlace_mode; // 交错模式配置

            int generate_threads = 1;
            int per_table_rows = 10000;
        } data_generation;
        struct DataCache {
            bool enabled = false;
            int cache_size = 1000000; // 缓存大小
        } data_cache;
        struct InsertControl {
            int per_request_rows = 30000;
            bool auto_create_table = false;
            int insert_threads = 8;
            std::string thread_allocation = "index_range"; // index_range or vgroup_binding
            std::string log_path = "result.txt";
            bool enable_dryrun = false;
            bool preload_table_meta = false;
        
            struct FailureHandling {
                int max_retries = 0;
                int retry_interval_ms = 1000;
                std::string on_failure = "exit"; //  exit or warn_and_continue
            } failure_handling;
        } insert_control;
        struct TimeInterval {
            bool enabled = false;                      // 是否启用间隔控制，默认值为 false
            std::string interval_strategy = "fixed";   // 时间间隔策略类型，默认值为 fixed，可选值为 first_to_first、last_to_first、fixed
        
            struct FixedInterval {
                int base_interval = 1000;              // 固定间隔数值，单位毫秒，必需
                int random_deviation = 0;              // 随机偏移量，默认值为 0
            } fixed_interval;
        
            struct DynamicInterval {
                int min_interval = -1;                 // 最小时间间隔阈值，默认值为 -1
                int max_interval = -1;                 // 最大时间间隔阈值，默认值为 -1
            } dynamic_interval;
        } time_interval;
    } control;
};



struct QueryDataConfig {
    struct Source {
        ConnectionInfo connection_info;
    } source;

    struct Control {
        DataFormat data_format;
        DataChannel data_channel;

        struct QueryControl {
            std::string log_path = "result.txt";
            bool enable_dryrun = false;

            struct Execution {
                std::string mode = "sequential_per_thread";
                int threads = 1;
                int times = 1;
                int interval = 0;
            } execution;

            std::string query_type;

            struct FixedQuery {
                std::string sql;
                std::string output_file;
            };
            struct SuperTableQueryTemplate {
                std::string sql_template;
                std::string output_file;
            };

            struct Fixed {
                std::vector<FixedQuery> queries;
            } fixed;

            struct SuperTable {
                std::string database_name;
                std::string super_table_name;
                std::string placeholder;
                std::vector<SuperTableQueryTemplate> templates;
            } super_table;
        } query_control;
    } control;
};


struct SubscribeDataConfig {
    struct Source {
        ConnectionInfo connection_info; // 数据库连接信息
    } source;

    struct Control {
        DataFormat data_format;   // 数据格式化配置
        DataChannel data_channel; // 数据通道配置

        struct SubscribeControl {
            std::string log_path = "result.txt"; // 日志文件路径
            bool enable_dryrun = false;         // 是否启用模拟执行

            struct Execution {
                int consumer_concurrency = 1; // 并发消费者数量
                int poll_timeout = 1000;      // 轮询超时时间（毫秒）
            } execution;

            struct Topic {
                std::string name; // 主题名称
                std::string sql;  // 创建主题的 SQL 语句
            };
            std::vector<Topic> topics; // 订阅主题列表

            struct Commit {
                std::string mode = "auto"; // 提交模式（auto 或 manual）
            } commit;

            struct GroupID {
                std::string strategy;       // Group ID 生成策略（shared、independent、custom）
                std::optional<std::string> custom_id; // 自定义 Group ID（当 strategy 为 custom 时必需）
            } group_id;

            struct Output {
                std::string path;         // 数据文件保存路径
                std::string file_prefix;  // 数据文件前缀
                std::optional<int> expected_rows; // 每个消费者期望消费的行数
            } output;

            std::map<std::string, std::string> advanced; // 高级参数配置，键值对映射
        } subscribe_control;
    } control;
};






using ActionConfigVariant = std::variant<
    std::monostate,
    CreateDatabaseConfig,
    CreateSuperTableConfig,
    CreateChildTableConfig,
    InsertDataConfig,
    QueryDataConfig,
    SubscribeDataConfig
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



    template<>
    struct convert<TimestampGeneratorConfig> {
        static bool decode(const Node& node, TimestampGeneratorConfig& rhs) {
            if (node["start_timestamp"]) {
                rhs.start_timestamp = node["start_timestamp"].as<std::string>("now");
            }
            if (node["timestamp_precision"]) {
                rhs.timestamp_precision = node["timestamp_precision"].as<std::string>("ms");
            }
            if (node["timestamp_step"]) {
                rhs.timestamp_step = node["timestamp_step"].as<int>(1);
            }
            return true;
        }
    };



    template<>
    struct convert<ColumnsConfig> {
        static bool decode(const Node& node, ColumnsConfig& rhs) {
            if (!node["source_type"]) {
                throw std::runtime_error("Missing required field 'source_type' in ColumnsConfig.");
            }
            rhs.source_type = node["source_type"].as<std::string>();
    
            if (rhs.source_type == "generator") {
                if (!node["generator"]) {
                    throw std::runtime_error("Missing required 'generator' configuration for source_type 'generator'.");
                }
                const auto& generator = node["generator"];
                if (generator["schema"]) {
                    rhs.generator.schema = generator["schema"].as<std::vector<SuperTableInfo::Column>>();
                }
                if (generator["timestamp_strategy"]) {
                    rhs.generator.timestamp_strategy.generator_config = generator["timestamp_strategy"]["generator_config"].as<TimestampGeneratorConfig>();
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
                    rhs.csv.has_header = csv["has_header"].as<bool>(true);
                }
                if (csv["delimiter"]) {
                    rhs.csv.delimiter = csv["delimiter"].as<std::string>(",");
                }
                if (csv["timestamp_strategy"]) {
                    const auto& ts = csv["timestamp_strategy"];
                    rhs.csv.timestamp_strategy.strategy_type = ts["strategy_type"].as<std::string>("original");
                    if (rhs.csv.timestamp_strategy.strategy_type == "original") {
                        const auto& original = ts["original_config"];
                        rhs.csv.timestamp_strategy.original_config.column_index = original["column_index"].as<int>(0);
                        rhs.csv.timestamp_strategy.original_config.precision = original["precision"].as<std::string>("ms");
                        if (original["offset_config"]) {
                            rhs.csv.timestamp_strategy.original_config.offset_config = original["offset_config"].as<std::string>();
                        }
                    }
                    if (ts["generator_config"]) {
                        rhs.csv.timestamp_strategy.generator_config = ts["generator_config"].as<TimestampGeneratorConfig>();
                    }
                }
            } else {
                throw std::runtime_error("Invalid 'source_type' in ColumnsConfig: " + rhs.source_type);
            }
    
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Source> {
        static bool decode(const Node& node, InsertDataConfig::Source& rhs) {
            if (node["table_name"]) {
                rhs.table_name = node["table_name"].as<TableNameConfig>();
            }
            if (node["tags"]) {
                rhs.tags = node["tags"].as<TagsConfig>();
            }
            if (node["columns"]) {
                rhs.columns = node["columns"].as<ColumnsConfig>();
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Target::TDengine> {
        static bool decode(const Node& node, InsertDataConfig::Target::TDengine& rhs) {
            if (node["connection_info"]) {
                rhs.connection_info = node["connection_info"].as<ConnectionInfo>();
            }
            if (node["database_info"]) {
                rhs.database_info = node["database_info"].as<DatabaseInfo>();
            }
            if (node["super_table_info"]) {
                rhs.super_table_info = node["super_table_info"].as<SuperTableInfo>();
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Target::FileSystem> {
        static bool decode(const Node& node, InsertDataConfig::Target::FileSystem& rhs) {
            if (node["output_dir"]) {
                rhs.output_dir = node["output_dir"].as<std::string>();
            }
            if (node["file_prefix"]) {
                rhs.file_prefix = node["file_prefix"].as<std::string>("data");
            }
            if (node["timestamp_format"]) {
                rhs.timestamp_format = node["timestamp_format"].as<std::string>();
            }
            if (node["timestamp_interval"]) {
                rhs.timestamp_interval = node["timestamp_interval"].as<std::string>("1d");
            }
            if (node["include_header"]) {
                rhs.include_header = node["include_header"].as<bool>(true);
            }
            if (node["tbname_col_alias"]) {
                rhs.tbname_col_alias = node["tbname_col_alias"].as<std::string>("device_id");
            }
            if (node["compression_level"]) {
                rhs.compression_level = node["compression_level"].as<std::string>("none");
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Target> {
        static bool decode(const Node& node, InsertDataConfig::Target& rhs) {
            if (node["timestamp_precision"]) {
                rhs.timestamp_precision = node["timestamp_precision"].as<std::string>();
            }
            if (node["target_type"]) {
                rhs.target_type = node["target_type"].as<std::string>();
            }
            if (rhs.target_type == "tdengine" && node["tdengine"]) {
                rhs.tdengine = node["tdengine"].as<InsertDataConfig::Target::TDengine>();
            } else if (rhs.target_type == "file_system" && node["file_system"]) {
                rhs.file_system = node["file_system"].as<InsertDataConfig::Target::FileSystem>();
            }
            return true;
        }
    };


    template<>
    struct convert<DataFormat> {
        static bool decode(const Node& node, DataFormat& rhs) {
            if (node["format_type"]) {
                rhs.format_type = node["format_type"].as<std::string>("sql");
            }
            if (rhs.format_type == "stmt" && node["stmt_config"]) {
                rhs.stmt_config.version = node["stmt_config"]["version"].as<std::string>("v2");
            }
            if (rhs.format_type == "schemaless" && node["schemaless_config"]) {
                rhs.schemaless_config.protocol = node["schemaless_config"]["protocol"].as<std::string>("line");
            }
            if (rhs.format_type == "csv" && node["csv_config"]) {
                rhs.csv_config.delimiter = node["csv_config"]["delimiter"].as<std::string>(",");
                rhs.csv_config.quote_character = node["csv_config"]["quote_character"].as<std::string>("\"");
                rhs.csv_config.escape_character = node["csv_config"]["escape_character"].as<std::string>("\\");
            }
            return true;
        }
    };


    template<>
    struct convert<DataChannel> {
        static bool decode(const Node& node, DataChannel& rhs) {
            if (node["channel_type"]) {
                rhs.channel_type = node["channel_type"].as<std::string>("native");
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Control::DataQuality> {
        static bool decode(const Node& node, InsertDataConfig::Control::DataQuality& rhs) {
            if (node["data_disorder"]) {
                const auto& disorder = node["data_disorder"];
                rhs.data_disorder.enabled = disorder["enabled"].as<bool>(false);
                if (disorder["intervals"]) {
                    for (const auto& interval : disorder["intervals"]) {
                        InsertDataConfig::Control::DataQuality::DataDisorder::Interval i;
                        i.time_start = interval["time_start"].as<std::string>();
                        i.time_end = interval["time_end"].as<std::string>();
                        i.ratio = interval["ratio"].as<double>(0.0);
                        i.latency_range = interval["latency_range"].as<int>(0);
                        rhs.data_disorder.intervals.push_back(i);
                    }
                }
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Control::DataGeneration> {
        static bool decode(const Node& node, InsertDataConfig::Control::DataGeneration& rhs) {
            if (node["interlace_mode"]) {
                const auto& interlace = node["interlace_mode"];
                rhs.interlace_mode.enabled = interlace["enabled"].as<bool>(false);
                rhs.interlace_mode.rows  = interlace["rows"].as<int>(1);
            }
            if (node["generate_threads"]) {
                rhs.generate_threads = node["generate_threads"].as<int>(1);
            }
            if (node["per_table_rows"]) {
                rhs.per_table_rows = node["per_table_rows"].as<int>(10000);
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Control::DataCache> {
        static bool decode(const Node& node, InsertDataConfig::Control::DataCache& rhs) {
            if (node["enabled"]) {
                rhs.enabled = node["enabled"].as<bool>(false);
            }
            if (node["cache_size"]) {
                rhs.cache_size = node["cache_size"].as<int>(1000000);
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Control::InsertControl> {
        static bool decode(const Node& node, InsertDataConfig::Control::InsertControl& rhs) {
            if (node["per_request_rows"]) {
                rhs.per_request_rows = node["per_request_rows"].as<int>(30000);
            }
            if (node["auto_create_table"]) {
                rhs.auto_create_table = node["auto_create_table"].as<bool>(false);
            }
            if (node["insert_threads"]) {
                rhs.insert_threads = node["insert_threads"].as<int>(8);
            }
            if (node["thread_allocation"]) {
                rhs.thread_allocation = node["thread_allocation"].as<std::string>("index_range");
            }
            if (node["log_path"]) {
                rhs.log_path = node["log_path"].as<std::string>("result.txt");
            }
            if (node["enable_dryrun"]) {
                rhs.enable_dryrun = node["enable_dryrun"].as<bool>(false);
            }
            if (node["preload_table_meta"]) {
                rhs.preload_table_meta = node["preload_table_meta"].as<bool>(false);
            }
            if (node["failure_handling"]) {
                const auto& failure = node["failure_handling"];
                rhs.failure_handling.max_retries = failure["max_retries"].as<int>(0);
                rhs.failure_handling.retry_interval_ms = failure["retry_interval_ms"].as<int>(1000);
                rhs.failure_handling.on_failure = failure["on_failure"].as<std::string>("exit");
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Control::TimeInterval> {
        static bool decode(const Node& node, InsertDataConfig::Control::TimeInterval& rhs) {
            if (node["enabled"]) {
                rhs.enabled = node["enabled"].as<bool>(false);
            }
            if (node["interval_strategy"]) {
                rhs.interval_strategy = node["interval_strategy"].as<std::string>("fixed");
            }
            if (rhs.interval_strategy == "fixed" && node["fixed_interval"]) {
                const auto& fixed = node["fixed_interval"];
                rhs.fixed_interval.base_interval = fixed["base_interval"].as<int>(1000);
                rhs.fixed_interval.random_deviation = fixed["random_deviation"].as<int>(0);
            } else if ((rhs.interval_strategy == "first_to_first" || rhs.interval_strategy == "last_to_first") && node["dynamic_interval"]) {
                const auto& dynamic = node["dynamic_interval"];
                rhs.dynamic_interval.min_interval = dynamic["min_interval"].as<int>(-1);
                rhs.dynamic_interval.max_interval = dynamic["max_interval"].as<int>(-1);
            }
            return true;
        }
    };


    template<>
    struct convert<InsertDataConfig::Control> {
        static bool decode(const Node& node, InsertDataConfig::Control& rhs) {
            if (node["data_format"]) {
                rhs.data_format = node["data_format"].as<DataFormat>();
            }
            if (node["data_channel"]) {
                rhs.data_channel = node["data_channel"].as<DataChannel>();
            }
            if (node["data_quality"]) {
                rhs.data_quality = node["data_quality"].as<InsertDataConfig::Control::DataQuality>();
            }
            if (node["data_generation"]) {
                rhs.data_generation = node["data_generation"].as<InsertDataConfig::Control::DataGeneration>();
            }
            if (node["data_cache"]) {
                rhs.data_cache = node["data_cache"].as<InsertDataConfig::Control::DataCache>();
            }
            if (node["insert_control"]) {
                rhs.insert_control = node["insert_control"].as<InsertDataConfig::Control::InsertControl>();
            }
            if (node["time_interval"]) {
                rhs.time_interval = node["time_interval"].as<InsertDataConfig::Control::TimeInterval>();
            }
            return true;
        }
    };

    // template<>
    // struct convert<InsertDataConfig> {
    //     static bool decode(const Node& node, InsertDataConfig& rhs) {
    //         if (node["source"]) {
    //             rhs.source = node["source"].as<InsertDataConfig::Source>();
    //         }
    //         if (node["target"]) {
    //             rhs.target = node["target"].as<InsertDataConfig::Target>();
    //         }
    //         if (node["control"]) {
    //             rhs.control = node["control"].as<InsertDataConfig::Control>();
    //         }
    //         return true;
    //     }
    // };


    template<>
    struct convert<QueryDataConfig::Source> {
        static bool decode(const Node& node, QueryDataConfig::Source& rhs) {
            if (node["connection_info"]) {
                rhs.connection_info = node["connection_info"].as<ConnectionInfo>();
            }
            return true;
        }
    };


    template<>
    struct convert<QueryDataConfig::Control::QueryControl::Execution> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl::Execution& rhs) {
            if (node["mode"]) {
                rhs.mode = node["mode"].as<std::string>("sequential_per_thread");
            }
            if (node["threads"]) {
                rhs.threads = node["threads"].as<int>(1);
            }
            if (node["times"]) {
                rhs.times = node["times"].as<int>(1);
            }
            if (node["interval"]) {
                rhs.interval = node["interval"].as<int>(0);
            }
            return true;
        }
    };



    template<>
    struct convert<QueryDataConfig::Control::QueryControl::Fixed> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl::Fixed& rhs) {
            if (node["queries"]) {
                for (const auto& query_node : node["queries"]) {
                    QueryDataConfig::Control::QueryControl::FixedQuery query;
                    query.sql = query_node["sql"].as<std::string>();
                    query.output_file = query_node["output_file"].as<std::string>();
                    rhs.queries.push_back(query);
                }
            }
            return true;
        }
    };



    template<>
    struct convert<QueryDataConfig::Control::QueryControl::SuperTable> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl::SuperTable& rhs) {
            if (node["database_name"]) {
                rhs.database_name = node["database_name"].as<std::string>();
            }
            if (node["super_table_name"]) {
                rhs.super_table_name = node["super_table_name"].as<std::string>();
            }
            if (node["placeholder"]) {
                rhs.placeholder = node["placeholder"].as<std::string>();
            }
            if (node["templates"]) {
                for (const auto& template_node : node["templates"]) {
                    QueryDataConfig::Control::QueryControl::SuperTableQueryTemplate query_template;
                    query_template.sql_template = template_node["sql_template"].as<std::string>();
                    query_template.output_file = template_node["output_file"].as<std::string>();
                    rhs.templates.push_back(query_template);
                }
            }
            return true;
        }
    };


    template<>
    struct convert<QueryDataConfig::Control::QueryControl> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl& rhs) {
            if (node["log_path"]) {
                rhs.log_path = node["log_path"].as<std::string>("result.txt");
            }
            if (node["enable_dryrun"]) {
                rhs.enable_dryrun = node["enable_dryrun"].as<bool>(false);
            }
            if (node["execution"]) {
                rhs.execution = node["execution"].as<QueryDataConfig::Control::QueryControl::Execution>();
            }
            if (node["query_type"]) {
                rhs.query_type = node["query_type"].as<std::string>();
                if (rhs.query_type == "fixed" && node["fixed"]) {
                    rhs.fixed = node["fixed"].as<QueryDataConfig::Control::QueryControl::Fixed>();
                } else if (rhs.query_type == "super_table" && node["super_table"]) {
                    rhs.super_table = node["super_table"].as<QueryDataConfig::Control::QueryControl::SuperTable>();
                } else {
                    throw std::runtime_error("Invalid or missing 'query_type' in query_control.");
                }
            }
            return true;
        }
    };


    template<>
    struct convert<QueryDataConfig::Control> {
        static bool decode(const Node& node, QueryDataConfig::Control& rhs) {
            if (node["data_format"]) {
                rhs.data_format = node["data_format"].as<DataFormat>();
            }
            if (node["data_channel"]) {
                rhs.data_channel = node["data_channel"].as<DataChannel>();
            }
            if (node["query_control"]) {
                rhs.query_control = node["query_control"].as<QueryDataConfig::Control::QueryControl>();
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Source> {
        static bool decode(const Node& node, SubscribeDataConfig::Source& rhs) {
            if (node["connection_info"]) {
                rhs.connection_info = node["connection_info"].as<ConnectionInfo>();
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Execution> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Execution& rhs) {
            if (node["consumer_concurrency"]) {
                rhs.consumer_concurrency = node["consumer_concurrency"].as<int>(1);
            }
            if (node["poll_timeout"]) {
                rhs.poll_timeout = node["poll_timeout"].as<int>(1000);
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Topic> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Topic& rhs) {
            if (node["name"]) {
                rhs.name = node["name"].as<std::string>();
            }
            if (node["sql"]) {
                rhs.sql = node["sql"].as<std::string>();
            }
            return true;
        }
    };

    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Commit> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Commit& rhs) {
            if (node["mode"]) {
                rhs.mode = node["mode"].as<std::string>("auto");
            }
            return true;
        }
    };

    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::GroupID> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::GroupID& rhs) {
            if (node["strategy"]) {
                rhs.strategy = node["strategy"].as<std::string>();
            }
            if (rhs.strategy == "custom" && node["custom_id"]) {
                rhs.custom_id = node["custom_id"].as<std::string>();
            }
            return true;
        }
    };

    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Output> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Output& rhs) {
            if (node["path"]) {
                rhs.path = node["path"].as<std::string>();
            }
            if (node["file_prefix"]) {
                rhs.file_prefix = node["file_prefix"].as<std::string>();
            }
            if (node["expected_rows"]) {
                rhs.expected_rows = node["expected_rows"].as<int>();
            }
            return true;
        }
    };

    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl& rhs) {
            if (node["log_path"]) {
                rhs.log_path = node["log_path"].as<std::string>("result.txt");
            }
            if (node["enable_dryrun"]) {
                rhs.enable_dryrun = node["enable_dryrun"].as<bool>(false);
            }
            if (node["execution"]) {
                rhs.execution = node["execution"].as<SubscribeDataConfig::Control::SubscribeControl::Execution>();
            }
            if (node["topics"]) {
                rhs.topics = node["topics"].as<std::vector<SubscribeDataConfig::Control::SubscribeControl::Topic>>();
            }
            if (node["commit"]) {
                rhs.commit = node["commit"].as<SubscribeDataConfig::Control::SubscribeControl::Commit>();
            }
            if (node["group_id"]) {
                rhs.group_id = node["group_id"].as<SubscribeDataConfig::Control::SubscribeControl::GroupID>();
            }
            if (node["output"]) {
                rhs.output = node["output"].as<SubscribeDataConfig::Control::SubscribeControl::Output>();
            }
            if (node["advanced"]) {
                rhs.advanced = node["advanced"].as<std::map<std::string, std::string>>();
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control> {
        static bool decode(const Node& node, SubscribeDataConfig::Control& rhs) {
            if (node["data_format"]) {
                rhs.data_format = node["data_format"].as<DataFormat>();
            }
            if (node["data_channel"]) {
                rhs.data_channel = node["data_channel"].as<DataChannel>();
            }
            if (node["subscribe_control"]) {
                rhs.subscribe_control = node["subscribe_control"].as<SubscribeDataConfig::Control::SubscribeControl>();
            }
            return true;
        }
    };




}


#endif // CONFIG_DATA_H