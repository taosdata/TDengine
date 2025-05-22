#ifndef COLUMNS_CONFIG_H
#define COLUMNS_CONFIG_H

#include <string>
#include <vector>
#include "SuperTableInfo.h"
#include "TimestampGeneratorConfig.h"

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

#endif // COLUMNS_CONFIG_H