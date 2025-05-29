#pragma once
#include <vector>
#include <cstdint>
#include "TimestampGeneratorConfig.h"


class TimestampGenerator {
public:
    explicit TimestampGenerator(const TimestampGeneratorConfig& config);
    
    Timestamp generate() const;
    
    std::vector<Timestamp> generate(size_t count) const;

private:
    TimestampGeneratorConfig config_;
    mutable int64_t current_ = 0;
};