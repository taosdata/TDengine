#include "GzipCompressor.hpp"
#include <sstream>
#include <cstring>

std::string GzipCompressor::compress(const std::string& data) {
    z_stream zs{};
    if (deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                     15 | 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
        throw std::runtime_error("deflateInit2 failed");
    }

    zs.next_in = (Bytef*)data.data();
    zs.avail_in = data.size();

    char buffer[32768];
    std::string result;
    int ret;

    do {
        zs.next_out = (Bytef*)buffer;
        zs.avail_out = sizeof(buffer);
        ret = deflate(&zs, Z_FINISH);

        if (ret != Z_OK && ret != Z_STREAM_END) {
            deflateEnd(&zs);
            throw std::runtime_error("deflate failed");
        }
        result.append(buffer, sizeof(buffer) - zs.avail_out);
    } while (ret != Z_STREAM_END);

    deflateEnd(&zs);
    return result;
}

std::string GzipCompressor::decompress(const std::string& data) {
    z_stream zs{};
    if (inflateInit2(&zs, 15 | 16) != Z_OK) {
        throw std::runtime_error("inflateInit2 failed");
    }

    zs.next_in = (Bytef*)data.data();
    zs.avail_in = data.size();

    char buffer[32768];
    std::string result;
    int ret;

    do {
        zs.next_out = (Bytef*)buffer;
        zs.avail_out = sizeof(buffer);
        ret = inflate(&zs, 0);

        if (ret != Z_OK && ret != Z_STREAM_END) {
            inflateEnd(&zs);
            throw std::runtime_error("inflate failed: " + std::to_string(ret));
        }
        result.append(buffer, sizeof(buffer) - zs.avail_out);
    } while (ret != Z_STREAM_END);

    inflateEnd(&zs);
    return result;
}