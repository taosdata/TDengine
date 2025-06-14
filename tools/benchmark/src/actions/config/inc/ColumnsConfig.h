#pragma once

#include <string>
#include <vector>
#include "ColumnConfig.h"
#include "TimestampOriginalConfig.h"
#include "TimestampGeneratorConfig.h"

struct ColumnsConfig {
    std::string source_type; // 数据来源类型：generator 或 csv

    struct Generator {
        ColumnConfigVector schema; // 普通列的 Schema 定义

        struct TimestampStrategy {
            TimestampGeneratorConfig timestamp_config;
        } timestamp_strategy;
    } generator;

    struct CSV {
        ColumnConfigVector schema; // 普通列的 Schema 定义

        std::string file_path;
        bool has_header = true;
        std::string delimiter = ",";
        int tbname_index = -1;

        struct TimestampStrategy {
            std::string strategy_type = "original"; // 时间戳策略类型：original 或 generator
            std::variant<TimestampOriginalConfig, TimestampGeneratorConfig> timestamp_config;

            std::string get_precision() const {
                if (strategy_type == "original") {
                    return std::get<TimestampOriginalConfig>(timestamp_config).timestamp_precision;
                } else if (strategy_type == "generator") {
                    return std::get<TimestampGeneratorConfig>(timestamp_config).timestamp_precision;
                }
                throw std::runtime_error("Invalid timestamp strategy type: " + strategy_type);
            }
        } timestamp_strategy;
    } csv;
};