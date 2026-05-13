#pragma once
#include <string>
#include <cstddef>

struct MqttFormatOptions {
    std::string topic = "tsbench/{table}";
    std::string compression = "";
    std::string encoding = "UTF-8";
    std::string content_type = "json";
    std::string tbname_key = "table";

    size_t qos = 0;
    bool retain = false;
    size_t records_per_message = 1;
};