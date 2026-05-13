#include "Compressor.hpp"
#include <cassert>
#include <iostream>
#include <string>

void test_none_compressor() {
    std::string data = "hello world";
    std::string compressed = Compressor::compress(data, CompressionType::NONE);
    std::string decompressed = Compressor::decompress(compressed, CompressionType::NONE);
    assert(compressed == data);
    assert(decompressed == data);
    std::cout << "test_none_compressor passed.\n";
}

void test_gzip_compressor() {
    std::string data = "gzip test data 1234567890";
    std::string compressed = Compressor::compress(data, CompressionType::GZIP);
    assert(compressed != data);
    std::string decompressed = Compressor::decompress(compressed, CompressionType::GZIP);
    assert(decompressed == data);
    std::cout << "test_gzip_compressor passed.\n";
}

void test_lz4_compressor() {
    std::string data = "lz4 test data abcdefghijklmnopqrstuvwxyz";
    std::string compressed = Compressor::compress(data, CompressionType::LZ4);
    assert(compressed != data);
    std::string decompressed = Compressor::decompress(compressed, CompressionType::LZ4);
    assert(decompressed == data);
    std::cout << "test_lz4_compressor passed.\n";
}

void test_zstd_compressor() {
    std::string data = "zstd test data 0987654321";
    std::string compressed = Compressor::compress(data, CompressionType::ZSTD);
    assert(compressed != data);
    std::string decompressed = Compressor::decompress(compressed, CompressionType::ZSTD);
    assert(decompressed == data);
    std::cout << "test_zstd_compressor passed.\n";
}

void test_invalid_type() {
    std::string data = "invalid type";
    bool caught = false;
    try {
        Compressor::compress(data, static_cast<CompressionType>(999));
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Unsupported compression type") != std::string::npos);
        caught = true;
    }
    (void)caught;
    assert(caught);
    std::cout << "test_invalid_type passed.\n";
}

int main() {
    test_none_compressor();
    test_gzip_compressor();
    test_lz4_compressor();
    test_zstd_compressor();
    test_invalid_type();
    std::cout << "All Compressor tests passed.\n";
    return 0;
}