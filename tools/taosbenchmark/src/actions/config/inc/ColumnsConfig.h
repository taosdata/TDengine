#pragma once

#include <string>
#include <vector>
#include "ColumnConfig.h"
#include "TimestampGeneratorConfig.h"

struct ColumnsConfig {
    std::string source_type; // 数据来源类型：generator 或 csv

    struct Generator {
        std::vector<ColumnConfig> schema; // 普通列的 Schema 定义

        struct TimestampStrategy {
            TimestampGeneratorConfig generator_config;
        } timestamp_strategy;
    } generator;

    struct CSV {
        std::string file_path;
        bool has_header = true;
        std::string delimiter = ",";

        struct TimestampStrategy {
            std::string strategy_type = "original"; // 时间戳策略类型：original 或 generator

            struct TimestampOriginalConfig {
                int timestamp_index = 0; // 指定原始时间列的索引（从 0 开始）
                std::string precision = "ms"; // 时间精度，可选值："s"、"ms"、"us"、"ns"

                struct OffsetConfig {
                    std::string offset_type; // 时间戳偏移类型："relative" 或 "absolute"
                    std::variant<std::string, int64_t> value; // 偏移量或起始时间戳
                };

                std::optional<OffsetConfig> offset_config; // 偏移配置
            };

            std::variant<TimestampOriginalConfig, TimestampGeneratorConfig> timestamp_config;
        } timestamp_strategy;
    } csv;
};