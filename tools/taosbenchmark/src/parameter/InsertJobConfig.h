#ifndef INSERT_JOB_CONFIG_H
#define INSERT_JOB_CONFIG_H

#include "CommonConfig.h"
#include <string>
#include <vector>
#include <optional>
#include <map>


// 时间戳策略
struct TimestampStrategy {
    std::string strategy_type; // "original" or "generated"
    struct OriginalConfig {
        int column_index = 0;
        std::string precision = "ms";
        std::optional<std::string> offset_config; // e.g., "2y6m"
    };
    struct GeneratedConfig {
        std::string start_timestamp = "now"; // "now" or integer
        int timestamp_step = 1;
    };
    std::optional<OriginalConfig> original_config;
    std::optional<GeneratedConfig> generated_config;
};

// 列描述
struct ColumnDefinition {
    std::string name;
    std::string type; // e.g., "float", "varchar"
    std::optional<int> len; // Length for varchar, nchar, etc.
    std::optional<int> count; // Number of columns with the same prefix
    std::optional<std::string> gen_type; // "random", "order", "function"
    std::optional<std::string> precision; // For decimal type
    std::optional<int> scale; // For float, double, decimal
    std::optional<double> min; // Minimum value for random/order
    std::optional<double> max; // Maximum value for random/order
    std::optional<std::string> dec_min; // For high-precision decimal
    std::optional<std::string> dec_max; // For high-precision decimal
    std::optional<std::string> corpus; // Corpus for random strings
    std::optional<bool> has_chinese; // Whether to include Chinese characters
    std::optional<std::vector<std::string>> values; // Value range for random
    std::optional<std::string> expression; // For function-based generation
};

// 标签描述
struct TagDefinition {
    std::string name;
    std::string type; // e.g., "varchar", "int"
    std::optional<int> len; // Length for varchar, nchar, etc.
    std::optional<std::vector<std::string>> values; // Value range for random
};

// 数据源配置
struct SourceConfig {
    std::string source_type; // "csv" or "generator"
    TimestampStrategy timestamp_strategy;
    std::vector<ColumnDefinition> columns;
    std::vector<TagDefinition> tags;
    struct CsvConfig {
        std::string file_path;
        bool has_header = true;
        std::optional<std::string> delimiter = ","; // Default is ","
    };
    std::optional<CsvConfig> csv_config; // Only for "csv" source_type
};

// 数据目标配置
struct TargetConfig {
    struct GeneralConfig {
        std::string timestamp_precision = "ms"; // "ms", "us", "ns"
        std::string child_table_prefix = "d";
        int child_table_count = 10;
        std::optional<int> child_table_from;
        std::optional<int> child_table_to;
        int insert_rows_per_table = 10000;
        bool use_source_definition = true;
        std::vector<ColumnDefinition> columns;
        std::vector<TagDefinition> tags;
    };
    GeneralConfig general;
    std::string target_type; // "tdengine" or "file_system"
    struct TDengineConfig {
        struct Connection {
            std::string host = "localhost";
            int port = 6030;
            std::string user = "root";
            std::string password = "taosdata";
        };
        struct DatabaseInfo {
            std::string name;
            bool drop_if_exists = true;
            int vgroups = 4;
            int replica = 1;
            int keep = 3650;
        };
        struct TableInfo {
            std::string name;
            bool super_table_exists = false;
            bool child_table_exists = false;
            bool auto_create_table = false;
            int create_table_threads = 8;
            int create_tables_per_batch = 100;
            bool preload_table_meta = false;
        };
        std::optional<Connection> connection;
        DatabaseInfo database_info;
        TableInfo table_info;
    };
    struct FileSystemConfig {
        std::string output_dir;
        std::string file_prefix = "data";
        std::optional<std::string> timestamp_format;
        std::optional<std::string> timestamp_interval = "1d";
        bool include_header = true;
        std::optional<std::string> tbname_col_alias = "device_id";
        std::optional<std::string> compression_level = "none"; // "none", "fast", "balance", "best"
    };
    std::optional<TDengineConfig> tdengine;
    std::optional<FileSystemConfig> file_system;
};

// 作业控制配置
struct ControlConfig {
    struct DataFormat {
        std::string format_type = "sql"; // "sql", "stmt", "schemaless", "csv"
        struct StmtConfig {
            std::string version = "v2"; // "v1", "v2"
        };
        struct SchemalessConfig {
            std::string protocol = "line"; // "line", "telnet", "json", "taos-json"
        };
        struct CsvConfig {
            std::string delimiter = ",";
            std::optional<std::string> quote_character = "\"";
            std::optional<std::string> escape_character = "\\";
        };
        std::optional<StmtConfig> stmt;
        std::optional<SchemalessConfig> schemaless;
        std::optional<CsvConfig> csv;
    };
    struct DataChannel {
        std::string channel_type = "native"; // "native", "websocket", "restful", "file_stream"
    };
    struct DataQuality {
        struct NullValue {
            bool enabled = false;
            double ratio = 0.0; // Ratio of NULL values
        };
        struct DataDisorder {
            bool enabled = false;
            struct Interval {
                std::string time_start;
                std::string time_end;
                double ratio = 0.0;
                int latency_range = 0;
            };
            std::vector<Interval> intervals;
        };
        NullValue null_value;
        DataDisorder data_disorder;
    };
    struct DataGeneration {
        struct InterlaceMode {
            bool enabled = false;
            int interlace_rows = 10;
        };
        InterlaceMode interlace_mode;
        int generate_threads = 8;
    };
    struct InsertControl {
        std::string log_path = "result.txt";
        bool enable_dryrun = false;
        int rows_per_request = 30000;
        int insert_threads = 8;
        std::string thread_allocation = "index_range"; // "vgroup_binding", "index_range"
        struct FailureHandling {
            int max_retries = 0;
            int retry_interval_ms = 1000;
            std::string on_failure = "exit"; // "exit", "warn_and_continue"
        };
        FailureHandling failure_handling;
    };
    struct TimeInterval {
        bool enabled = false;
        std::string interval_strategy = "fixed"; // "first_to_first", "last_to_first", "fixed"
        struct FixedInterval {
            int base_interval = 1000;
            int random_deviation = 0;
        };
        struct DynamicInterval {
            int min_interval = -1;
            int max_interval = -1;
        };
        std::optional<FixedInterval> fixed_interval;
        std::optional<DynamicInterval> dynamic_interval;
    };
    DataFormat data_format;
    DataChannel data_channel;
    DataQuality data_quality;
    DataGeneration data_generation;
    InsertControl insert_control;
    TimeInterval time_interval;
};

// 作业配置
struct InsertJobConfig {
    std::string job_type = "insert";
    std::string job_name;
    std::vector<std::string> dependencies;
    SourceConfig source;
    TargetConfig target;
    ControlConfig control;
};

#endif // INSERT_JOB_CONFIG_H