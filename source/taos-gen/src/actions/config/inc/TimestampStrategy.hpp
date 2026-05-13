#pragma once

#include "TimestampCSVConfig.hpp"
#include "TimestampGeneratorConfig.hpp"
#include <string>

struct TimestampStrategy {
    std::string strategy_type = "generator";
    TimestampCSVConfig csv;
    TimestampGeneratorConfig generator;

    std::string get_precision() const {
        if (strategy_type == "csv") {
            return csv.timestamp_precision.value();
        } else if (strategy_type == "generator") {
            return generator.timestamp_precision;
        }
        throw std::runtime_error("Invalid timestamp strategy type: " + strategy_type);
    }
};