#pragma once

#include "ColumnConfig.hpp"
#include "TimestampStrategy.hpp"
#include <string>

struct ColumnsCSV {
    bool enabled = false;
    ColumnConfigVector schema;

    std::string loading_mode = "preload";
    std::string file_path;
    bool has_header = true;
    bool repeat_read = false;
    std::string delimiter = ",";
    int tbname_index = -1;
    int timestamp_index = -1;

    TimestampStrategy timestamp_strategy;
};