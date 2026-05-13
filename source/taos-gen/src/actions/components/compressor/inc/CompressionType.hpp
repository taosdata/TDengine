#pragma once

#include <string>

enum class CompressionType {
    NONE,
    GZIP,
    LZ4,
    ZSTD
};

const char* compression_to_string(CompressionType type);

CompressionType string_to_compression(const std::string& str);