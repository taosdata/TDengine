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
            TimestampGeneratorConfig generator_config;
        } timestamp_strategy;
    } generator;

    struct CSV {
        std::string file_path;
        bool has_header = true;
        std::string delimiter = ",";
        int tbname_index = -1;

        struct TimestampStrategy {
            std::string strategy_type = "original"; // 时间戳策略类型：original 或 generator

            std::variant<TimestampOriginalConfig, TimestampGeneratorConfig> timestamp_config;
        } timestamp_strategy;
    } csv;
};