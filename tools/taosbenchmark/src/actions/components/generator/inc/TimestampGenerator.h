#pragma once

#include <cstdint>
#include <memory>
#include <vector>
#include "TimestampGeneratorConfig.h"


class TimestampGenerator {
public:
    explicit TimestampGenerator(const TimestampGeneratorConfig& config);
    
    Timestamp generate() const;
    
    std::vector<Timestamp> generate(size_t count) const;

    static std::unique_ptr<TimestampGenerator> create(const TimestampGeneratorConfig& config) {
        return std::make_unique<TimestampGenerator>(config);
    }

private:
    TimestampGeneratorConfig config_;
    mutable int64_t current_ = 0;
};