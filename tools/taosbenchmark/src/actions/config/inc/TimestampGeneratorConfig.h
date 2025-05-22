#ifndef TIMESTAMP_GENERATOR_CONFIG_H
#define TIMESTAMP_GENERATOR_CONFIG_H

#include <string>

struct TimestampGeneratorConfig {
    std::string start_timestamp = "now";       // 起始时间戳，默认值为 "now"
    std::string timestamp_precision = "ms";   // 时间戳精度，默认值为毫秒
    int timestamp_step = 1;                   // 时间戳步长，默认值为 1
};

#endif // TIMESTAMP_GENERATOR_CONFIG_H