#pragma once

#include <variant>
#include <string>
#include <vector>
#include <cstdint>


struct Timestamp {
    int64_t value;
};


struct Decimal {
    std::string value;
};


// JSON 类型（可替换为 nlohmann::json）
struct JsonValue {
    std::string rawJson;
};


// 几何类型（示例：WKT格式）
struct Geometry {
    std::string wkt; // 如 "POINT(10 20)"
};


using ColumnType = std::variant<
    Timestamp,            // timestamp
    bool,                 // bool
    int8_t,               // tinyint
    uint8_t,              // tinyint unsigned
    int16_t,              // smallint
    uint16_t,             // smallint unsigned
    int32_t,              // int
    uint32_t,             // int unsigned
    int64_t,              // bigint
    uint64_t,             // bigint unsigned
    float,                // float
    double,               // double
    Decimal,              // decimal
    std::u16string,       // nchar (UTF-16)
    std::string,          // varchar / binary / json
    JsonValue,            // json（结构体）
    std::vector<uint8_t>, // varbinary
    Geometry              // geometry
>;

using ColumnTypeVector = std::vector<ColumnType>;
using RowType = ColumnTypeVector;