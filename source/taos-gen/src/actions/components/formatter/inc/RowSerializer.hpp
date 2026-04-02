#pragma once

#include "ColumnConfigInstance.hpp"
#include "MemoryPool.hpp"
#include <nlohmann/json.hpp>

class RowSerializer {
public:
    static nlohmann::ordered_json to_json(
        const ColumnConfigInstanceVector& col_instances,
        const ColumnConfigInstanceVector& tag_instances,
        const MemoryPool::TableBlock& table,
        size_t row_index,
        const std::string& tbname_key
    );

    static void to_json_inplace(
        const ColumnConfigInstanceVector& col_instances,
        const ColumnConfigInstanceVector& tag_instances,
        const MemoryPool::TableBlock& table,
        size_t row_index,
        const std::string& tbname_key,
        nlohmann::ordered_json& out
    );

    static void to_influx_inplace(
        const ColumnConfigInstanceVector& col_instances,
        const ColumnConfigInstanceVector& tag_instances,
        const MemoryPool::TableBlock& table,
        size_t row_index,
        fmt::memory_buffer& out);
};
