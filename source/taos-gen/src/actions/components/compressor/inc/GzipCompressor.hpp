#pragma once
#include <zlib.h>
#include <string>
#include <stdexcept>

class GzipCompressor {
public:
    static std::string compress(const std::string& data);
    static std::string decompress(const std::string& data);
};