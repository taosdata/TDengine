#pragma once
#include <string>
#include <cstddef>

struct InfluxDBFormatOptions {
    std::string precision = "ns";  // ns, us, ms, s
    size_t batch_size = 5000;      // lines per HTTP request
    bool gzip = false;             // enable gzip compression
};
