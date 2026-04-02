#pragma once
#include <zstd.h>
#include <string>
#include <stdexcept>

class ZstdCompressor {
public:
    static std::string compress(const std::string& data);
    static std::string decompress(const std::string& data);
};