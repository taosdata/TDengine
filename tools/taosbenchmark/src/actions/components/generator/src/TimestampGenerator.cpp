#include "TimestampGenerator.h"
#include <chrono>
#include <stdexcept>
#include <variant>
#include <unordered_map>


TimestampGenerator::TimestampGenerator(const TimestampGeneratorConfig& config) 
    : config_(config) 
{
    // Initialize current timestamp
    if (std::holds_alternative<std::string>(config_.start_timestamp)) {
        const std::string& start = std::get<std::string>(config_.start_timestamp);
        if (start == "now" || start == "now()") {
            auto now = std::chrono::system_clock::now();

            if (config_.timestamp_precision == "ms") {
                current_ = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            } else if (config_.timestamp_precision == "us") {
                current_ = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
            } else if (config_.timestamp_precision == "ns") {
                current_ = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
            } else {
                throw std::runtime_error("Invalid timestamp_precision value: " + config_.timestamp_precision);
            }
        } else {
            throw std::runtime_error("Invalid start_timestamp value: " + start);
        }
    } else {
        current_ = std::get<Timestamp>(config_.start_timestamp);
    }
}

Timestamp TimestampGenerator::generate() const {
    Timestamp ts{current_};
    current_ += config_.timestamp_step;
    return ts;
}

std::vector<Timestamp> TimestampGenerator::generate(size_t count) const {
    std::vector<Timestamp> timestamps;
    timestamps.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        timestamps.push_back(generate());
    }

    return timestamps;
}
