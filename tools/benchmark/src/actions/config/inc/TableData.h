#pragma once

#include "ColumnType.h"


struct RowData {
    // std::string table_name;
    // std::optional<int64_t> timestamp;
    int64_t timestamp;
    RowType columns;
};

struct TableData {
    std::string table_name;
    std::vector<int64_t> timestamps;
    std::vector<RowType> rows;
};

struct MultiBatch {
    std::vector<std::pair<std::string, std::vector<RowData>>> table_batches;
    int64_t start_time{0};
    int64_t end_time{0};
    size_t total_rows{0};
};