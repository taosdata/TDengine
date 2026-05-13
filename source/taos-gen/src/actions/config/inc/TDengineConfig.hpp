#pragma once
#include "ISinkConfig.hpp"
#include <string>
#include <optional>

struct TDengineConfig : ISinkConfig {
    bool enabled = false;
    std::string dsn = "taos+ws://root:taosdata@localhost:6041/tsbench";
    std::string host = "localhost";
    int port = 6041;
    std::string user = "root";
    std::string password = "taosdata";
    std::string database = "tsbench";
    bool drop_if_exists = false;
    std::optional<std::string> properties;

    struct ConnectionPool {
        bool enabled = true;
        size_t max_size = 100;
        size_t min_size = 2;
        size_t timeout = 1000;       // unit: ms
    } pool;

    enum class ProtocolType {
        Native,
        WebSocket,
        RESTful
    } protocol_type = ProtocolType::WebSocket;

    TDengineConfig();
    TDengineConfig(std::string input_dsn);

    void init();
    void parse_dsn();

    std::string get_sink_info() const override {
        return "TDengine(" + host + ":" + std::to_string(port) + ")";
    }

    std::string get_sink_type() const override { return "TDengine"; }

    bool is_enabled() const override { return enabled; }
};
