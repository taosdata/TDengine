#ifndef INSERT_DATA_CONFIG_H
#define INSERT_DATA_CONFIG_H

#include <string>
#include <vector>
#include "TableNameConfig.h"
#include "TagsConfig.h"
#include "ColumnsConfig.h"
#include "ConnectionInfo.h"
#include "DatabaseInfo.h"
#include "SuperTableInfo.h"
#include "DataFormat.h"
#include "DataChannel.h"

struct InsertDataConfig {
    struct Source {
        TableNameConfig table_name; // 子表名称配置
        TagsConfig tags;            // 标签列配置
        ColumnsConfig columns;      // 普通列配置
    } source;

    struct Target {
        std::string timestamp_precision; // 时间戳精度：ms、us、ns
        std::string target_type;         // 数据目标类型：tdengine 或 file_system

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
                    std::variant<int64_t, std::string> time_start;
                    std::variant<int64_t, std::string> time_end;
                    double ratio = 0.0;       // 比例
                    int latency_range = 0;    // 延迟范围
                };
                std::vector<Interval> intervals; // 乱序时间区间
            } data_disorder;
        } data_quality;

        struct DataGeneration {
            struct InterlaceMode {
                bool enabled = false;
                int rows = 1; // 行数
            } interlace_mode;

            struct DataCache {
                bool enabled = false;
                int64_t cache_size = 1000000; // 缓存大小
            } data_cache;

            struct FlowControl {
                bool enabled = false;
                int64_t rate_limit = 0;             // 每秒生成的行数
            } flow_control;

            int generate_threads = 1;
            int64_t per_table_rows = 10000;
            int queue_capacity = 1000;
        } data_generation;

        struct InsertControl {
            int64_t per_request_rows = 30000;
            bool auto_create_table = false;
            int insert_threads = 8;
            std::string thread_allocation = "index_range"; // index_range or vgroup_binding
            std::string log_path = "result.txt";
            bool enable_dryrun = false;
            bool preload_table_meta = false;

            struct FailureHandling {
                int max_retries = 0;
                int retry_interval_ms = 1000;
                std::string on_failure = "exit"; // exit or warn_and_continue
            } failure_handling;
        } insert_control;

        struct TimeInterval {
            bool enabled = false;                    // 是否启用间隔控制
            std::string interval_strategy = "fixed"; // 时间间隔策略类型
            std::string wait_strategy = "sleep";     // 时间间隔等待策略类型，sleep or busy_wait

            struct FixedInterval {
                int base_interval = 1000; // 固定间隔数值，单位毫秒
                int random_deviation = 0; // 随机偏移量
            } fixed_interval;

            struct DynamicInterval {
                int min_interval = -1; // 最小时间间隔阈值
                int max_interval = -1; // 最大时间间隔阈值
            } dynamic_interval;
        } time_interval;
    } control;
};

#endif // INSERT_DATA_CONFIG_H