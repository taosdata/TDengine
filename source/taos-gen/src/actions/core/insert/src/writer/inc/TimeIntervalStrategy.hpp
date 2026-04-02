#pragma once
#include <chrono>
#include <atomic>
#include "InsertDataConfig.hpp"


enum class IntervalStrategyType {
    Fixed, FirstToFirst, LastToFirst, Literal, Unknown
};

class TimeIntervalStrategy {
public:
    TimeIntervalStrategy(
        const InsertDataConfig::TimeInterval& config,
        std::string timestamp_precision);

    // Apply wait strategy
    void apply_wait_strategy(int64_t current_start_time,
                             int64_t current_end_time,
                             int64_t last_start_time,
                             int64_t last_end_time,
                             bool is_first_write);

    // Timestamp conversion
    int64_t to_milliseconds(int64_t ts) const;
    int64_t to_microseconds(int64_t ts) const;

    // Fixed interval strategy
    int64_t fixed_interval_strategy() const;

    // Dynamic interval clamping
    int64_t clamp_interval(int64_t interval) const;

    std::chrono::steady_clock::time_point last_write_time() const noexcept {
        return last_write_time_;
    }

    IntervalStrategyType strategy_type() const noexcept {
        return strategy_type_;
    }

    bool is_enabled() const noexcept {
        return config_.enabled;
    }

    bool is_literal_strategy() const noexcept {
        return config_.enabled && strategy_type_ == IntervalStrategyType::Literal;
    }

private:
    // First row to first row strategy
    int64_t first_to_first_strategy(int64_t current_start, int64_t last_start) const;

    // Last row to first row strategy
    int64_t last_to_first_strategy(int64_t current_start, int64_t last_end) const;

    // Real-time strategy
    int64_t literal_strategy(int64_t current_start) const;

    // Configuration
    const InsertDataConfig::TimeInterval& config_;

    // Timestamp precision
    std::string timestamp_precision_;

    // Last write completion timestamp
    std::chrono::steady_clock::time_point last_write_time_;

    // Interval strategy type
    IntervalStrategyType strategy_type_;
};