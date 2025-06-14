#include "TimestampGenerator.h"
#include <chrono>
#include <stdexcept>
#include <variant>
#include <unordered_map>
#include "TimestampUtils.h"


TimestampGenerator::TimestampGenerator(const TimestampGeneratorConfig& config) 
    : config_(config) 
{
    // Initialize current timestamp
    reset();
}

void TimestampGenerator::reset() {
    // Reset current timestamp to initial value based on config
    current_ = TimestampUtils::parse_timestamp(
        config_.start_timestamp, 
        config_.timestamp_precision
    );
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
