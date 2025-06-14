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
    size_t total_rows{0};
};