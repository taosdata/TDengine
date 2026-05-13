#pragma once

#include "ColumnType.hpp"
#include <limits>

struct RowData {
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

    MultiBatch() = default;
    MultiBatch(MultiBatch&& other) noexcept
        : table_batches(std::move(other.table_batches))
        , start_time(other.start_time)
        , end_time(other.end_time)
        , total_rows(other.total_rows)
    {
        other.start_time = 0;
        other.end_time = 0;
        other.total_rows = 0;
    }

    MultiBatch& operator=(MultiBatch&& other) noexcept {
        if (this != &other) {
            table_batches = std::move(other.table_batches);
            start_time = other.start_time;
            end_time = other.end_time;
            total_rows = other.total_rows;

            other.start_time = 0;
            other.end_time = 0;
            other.total_rows = 0;
        }
        return *this;
    }

    void reserve(size_t table_count) {
        table_batches.reserve(table_count);
    }

    void update_metadata() {
        start_time = std::numeric_limits<int64_t>::max();
        end_time = std::numeric_limits<int64_t>::min();
        total_rows = 0;

        bool has_data = false;

        for (const auto& [table_name, rows] : table_batches) {
            (void)table_name;
            for (const auto& row : rows) {
                start_time = std::min(start_time, row.timestamp);
                end_time = std::max(end_time, row.timestamp);
                ++total_rows;
                has_data = true;
            }
        }

        if (!has_data) {
            start_time = 0;
            end_time = 0;
            total_rows = 0;
        }
    }
};