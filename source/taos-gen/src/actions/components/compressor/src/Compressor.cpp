#include "Compressor.hpp"
#include "GzipCompressor.hpp"
#include "Lz4Compressor.hpp"
#include "ZstdCompressor.hpp"

class NoneCompressor : public BaseCompressor {
public:
    std::string compress(const std::string& data) override { return data; }
    std::string decompress(const std::string& data) override { return data; }
};

class GzipCompressorAdapter : public BaseCompressor {
public:
    std::string compress(const std::string& data) override {
        return GzipCompressor::compress(data);
    }
    std::string decompress(const std::string& data) override {
        return GzipCompressor::decompress(data);
    }
};

class Lz4CompressorAdapter : public BaseCompressor {
public:
    std::string compress(const std::string& data) override {
        return Lz4Compressor::compress(data);
    }
    std::string decompress(const std::string& data) override {
        return Lz4Compressor::decompress(data);
    }
};

class ZstdCompressorAdapter : public BaseCompressor {
public:
    std::string compress(const std::string& data) override {
        return ZstdCompressor::compress(data);
    }
    std::string decompress(const std::string& data) override {
        return ZstdCompressor::decompress(data);
    }
};

std::unique_ptr<BaseCompressor> Compressor::get_compressor(CompressionType type) {
    switch (type) {
        case CompressionType::NONE: return std::make_unique<NoneCompressor>();
        case CompressionType::GZIP: return std::make_unique<GzipCompressorAdapter>();
        case CompressionType::LZ4:  return std::make_unique<Lz4CompressorAdapter>();
        case CompressionType::ZSTD: return std::make_unique<ZstdCompressorAdapter>();
        default: throw std::runtime_error("Unsupported compression type");
    }
}

std::string Compressor::compress(const std::string& data, CompressionType type) {
    return get_compressor(type)->compress(data);
}

std::string Compressor::decompress(const std::string& data, CompressionType type) {
    return get_compressor(type)->decompress(data);
}