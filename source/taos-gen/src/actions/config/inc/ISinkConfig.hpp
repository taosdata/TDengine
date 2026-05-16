#pragma once
#include <string>

struct ISinkConfig {
    virtual ~ISinkConfig() = default;

    // Obtain human-readable sink info
    virtual std::string get_sink_info() const = 0;

    // Obtain the name of the sink type
    virtual std::string get_sink_type() const = 0;

    // Whether to enable
    virtual bool is_enabled() const = 0;
};