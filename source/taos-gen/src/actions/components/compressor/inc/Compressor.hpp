#pragma once
#include "CompressionType.hpp"
#include <string>
#include <memory>

class BaseCompressor {
public:
    virtual ~BaseCompressor() = default;
    virtual std::string compress(const std::string& data) = 0;
    virtual std::string decompress(const std::string& data) = 0;
};

class Compressor {
public:
    static std::string compress(const std::string& data, CompressionType type);
    static std::string decompress(const std::string& data, CompressionType type);
    static std::unique_ptr<BaseCompressor> get_compressor(CompressionType type);
};