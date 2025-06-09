#pragma once

#include <string>
#include "ColumnType.h"

struct TimestampGeneratorConfig {
    std::variant<Timestamp, std::string> start_timestamp = "now";
    std::string timestamp_precision = "ms";
    int timestamp_step = 1;
};
