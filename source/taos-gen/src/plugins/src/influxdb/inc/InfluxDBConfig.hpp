#pragma once
#include "ISinkConfig.hpp"
#include <string>

struct InfluxDBConfig : ISinkConfig {
    bool enabled = false;
    std::string url = "http://localhost:8086";
    std::string token;
    std::string org = "default";
    std::string bucket = "default";

    std::string get_sink_info() const override {
        return "InfluxDB(" + url + "/" + bucket + ")";
    }

    std::string get_sink_type() const override { return "InfluxDB"; }

    bool is_enabled() const override { return enabled; }
};
