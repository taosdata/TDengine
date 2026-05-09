#pragma once
#include <cstdint>
#include <string>
#include <typeindex>

struct InfluxDBInsertData {
    std::string lines;
    int32_t total_rows = 0;

    InfluxDBInsertData() = default;
    InfluxDBInsertData(std::string lines_, int32_t rows)
        : lines(std::move(lines_)), total_rows(rows) {}
};

inline std::type_index INFLUXDB_TYPE_ID = std::type_index(typeid(InfluxDBInsertData));
inline uint64_t INFLUXDB_TYPE_HASH = INFLUXDB_TYPE_ID.hash_code();
