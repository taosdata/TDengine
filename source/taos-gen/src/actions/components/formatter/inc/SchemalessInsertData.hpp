#pragma once
#include "taos.h"
#include <string>
#include <cstdint>
#include <typeindex>

struct SchemalessInsertData {
    std::string lines;
    int32_t total_rows = 0;
    int protocol = TSDB_SML_LINE_PROTOCOL;
    int precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;

    SchemalessInsertData() = default;

    SchemalessInsertData(std::string&& lines, int32_t total_rows, int protocol, int precision)
        : lines(std::move(lines)), total_rows(total_rows), protocol(protocol), precision(precision) {}

    SchemalessInsertData(SchemalessInsertData&&) = default;
    SchemalessInsertData& operator=(SchemalessInsertData&&) = default;

    SchemalessInsertData(const SchemalessInsertData&) = delete;
    SchemalessInsertData& operator=(const SchemalessInsertData&) = delete;
};

inline std::type_index SCHEMALESS_TYPE_ID = std::type_index(typeid(SchemalessInsertData));
