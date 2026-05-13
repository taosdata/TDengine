#pragma once

#include <string>
#include "ColumnType.hpp"

struct TimestampGeneratorConfig {
    std::variant<Timestamp, std::string> start_timestamp = "now";
    std::string timestamp_precision = "ms";
    std::variant<Timestamp, std::string> timestamp_step = 1;
};
