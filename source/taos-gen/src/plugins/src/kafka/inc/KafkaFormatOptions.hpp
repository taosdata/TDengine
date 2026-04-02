#pragma once
#include <string>
#include <cstddef>

struct KafkaFormatOptions {
    std::string key_pattern = "{table}";
    std::string key_serializer = "string-utf8";
    std::string value_serializer = "json"; // "json" | "influx"
    std::string tbname_key = "table";
    std::string acks = "0";
    std::string compression = "none";
    size_t records_per_message = 1;
};
