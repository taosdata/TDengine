#include "ZstdCompressor.hpp"
#include <vector>

std::string ZstdCompressor::compress(const std::string& data) {
    size_t max_size = ZSTD_compressBound(data.size());
    if (ZSTD_isError(max_size)) {
        throw std::runtime_error("ZSTD_compressBound failed");
    }

    std::vector<char> compressed(max_size);
    size_t compressed_size = ZSTD_compress(
        compressed.data(), max_size,
        data.data(), data.size(),
        1  // Default compression level
    );

    if (ZSTD_isError(compressed_size)) {
        throw std::runtime_error("ZSTD compression failed: " +
            std::string(ZSTD_getErrorName(compressed_size)));
    }

    return {compressed.data(), compressed_size};
}

std::string ZstdCompressor::decompress(const std::string& data) {
    unsigned long long uncompressed_size = ZSTD_getFrameContentSize(
        data.data(), data.size()
    );

    if (uncompressed_size == ZSTD_CONTENTSIZE_ERROR) {
        throw std::runtime_error("Invalid ZSTD frame");
    }
    if (uncompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        throw std::runtime_error("Unknown content size");
    }

    std::string result(uncompressed_size, '\0');
    size_t actual_size = ZSTD_decompress(
        result.data(), uncompressed_size,
        data.data(), data.size()
    );

    if (ZSTD_isError(actual_size)) {
        throw std::runtime_error("ZSTD decompression failed: " +
            std::string(ZSTD_getErrorName(actual_size)));
    }

    return result;
}