#pragma once
#include <lz4.h>
#include <string>
#include <stdexcept>

class Lz4Compressor {
public:
    static std::string compress(const std::string& data);
    static std::string decompress(const std::string& data);
};