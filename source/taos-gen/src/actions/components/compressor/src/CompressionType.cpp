#include "CompressionType.hpp"
#include "StringUtils.hpp"
#include <stdexcept>

const char* compression_to_string(CompressionType type) {
    switch (type) {
        case CompressionType::NONE: return "NONE";
        case CompressionType::GZIP: return "GZIP";
        case CompressionType::LZ4:  return "LZ4";
        case CompressionType::ZSTD: return "ZSTD";
        default: return "UNKNOWN";
    }
}

CompressionType string_to_compression(const std::string& str) {
    std::string s = StringUtils::to_upper(str);
    if (s == "" || s == "NONE")  return CompressionType::NONE;
    if (s == "GZIP")  return CompressionType::GZIP;
    if (s == "LZ4")   return CompressionType::LZ4;
    if (s == "ZSTD")  return CompressionType::ZSTD;
    throw std::invalid_argument("Unknown compression type: " + str);
}