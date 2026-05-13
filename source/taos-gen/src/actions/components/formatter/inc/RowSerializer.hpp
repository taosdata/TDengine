#pragma once

#include "ColumnConfigInstance.hpp"
#include "MemoryPool.hpp"
#include <nlohmann/json.hpp>

enum class IntSuffixMode {
    STANDARD,   // InfluxDB/Kafka: i, u
    TDENGINE    // TDengine schemaless: i8, u8, i16, u16, i32, u32, i64, u64
};

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
        const std::string& measurement,
        const std::string& id_tag_key,
        fmt::memory_buffer& out,
        IntSuffixMode suffix_mode = IntSuffixMode::STANDARD);
};
