#pragma once
#include "ISinkConfig.hpp"
#include <string>

struct MqttConfig : ISinkConfig {
    bool enabled = false;
    std::string uri = "tcp://localhost:1883";
    std::string host = "localhost";
    int port = 1883;
    std::string user = "root";
    std::string password = "taosdata";
    std::string client_id = "taosgen";

    bool clean_session = true;
    size_t keep_alive = 120;
    size_t max_buffered_messages = 10000;

    void update_uri_from_host_port() {
        uri = "tcp://" + host + ":" + std::to_string(port);
    }

    std::string generate_client_id(size_t no = 0) const {
        return client_id + "-" + std::to_string(no);
    }

    MqttConfig() = default;

    MqttConfig(
        std::string uri_,
        std::string user_,
        std::string password_
    )
        : uri(std::move(uri_))
        , user(std::move(user_))
        , password(std::move(password_))
    {}

    std::string get_sink_info() const override {
        return "MQTT(" + uri + ")";
    }

    std::string get_sink_type() const override { return "MQTT"; }

    bool is_enabled() const override { return enabled; }

private:
    std::string topic_;
};
