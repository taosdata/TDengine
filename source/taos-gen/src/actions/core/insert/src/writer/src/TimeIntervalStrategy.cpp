#include "TimeIntervalStrategy.hpp"
#include <chrono>
#include <thread>
#include <cstdint>
#include <cstring>
#if defined(__linux__)
#include <unistd.h>
#include <sys/timerfd.h>
#endif

constexpr int64_t k_ms_to_us = 1000;

static void precise_sleep_us(uint64_t usec) {
#if defined(__linux__)
    int tfd = timerfd_create(CLOCK_MONOTONIC, 0);
    if (tfd == -1) {
        std::this_thread::sleep_for(std::chrono::microseconds(usec));
        return;
    }

    struct itimerspec ts;
    memset(&ts, 0, sizeof(ts));
    ts.it_value.tv_sec = usec / 1000000;
    ts.it_value.tv_nsec = (usec % 1000000) * 1000;

    if (timerfd_settime(tfd, 0, &ts, nullptr) == -1) {
        close(tfd);
        std::this_thread::sleep_for(std::chrono::microseconds(usec));
        return;
    }

    uint64_t exp;
    ssize_t s;
    do {
        s = read(tfd, &exp, sizeof(exp));
    } while (s == -1 && errno == EINTR);

    close(tfd);

    if (s != sizeof(exp)) {
        std::this_thread::sleep_for(std::chrono::microseconds(usec));
    }
#else
    std::this_thread::sleep_for(std::chrono::microseconds(usec));
#endif
}

IntervalStrategyType parse_strategy(const std::string& s) {
    if (s == "fixed") return IntervalStrategyType::Fixed;
    if (s == "first_to_first") return IntervalStrategyType::FirstToFirst;
    if (s == "last_to_first") return IntervalStrategyType::LastToFirst;
    if (s == "literal") return IntervalStrategyType::Literal;
    return IntervalStrategyType::Unknown;
}

TimeIntervalStrategy::TimeIntervalStrategy(
    const InsertDataConfig::TimeInterval& config,
    std::string timestamp_precision)
    : config_(config)
    , timestamp_precision_(timestamp_precision)
    , last_write_time_(std::chrono::steady_clock::now())
    , strategy_type_(parse_strategy(config.interval_strategy))
    {}

int64_t TimeIntervalStrategy::to_milliseconds(int64_t ts) const {
    if (timestamp_precision_ == "ms") return ts;
    if (timestamp_precision_ == "us") return ts / 1000;
    if (timestamp_precision_ == "ns") return ts / 1000000;
    throw std::runtime_error("Unknown timestamp precision");
}

int64_t TimeIntervalStrategy::to_microseconds(int64_t ts) const {
    if (timestamp_precision_ == "us") return ts;
    if (timestamp_precision_ == "ms") return ts * 1000;
    if (timestamp_precision_ == "ns") return ts / 1000;
    throw std::runtime_error("Unknown timestamp precision");
}

int64_t TimeIntervalStrategy::clamp_interval(int64_t interval) const {
    const auto& dyn_cfg = config_.dynamic_interval;
    if (dyn_cfg.min_interval >= 0 && interval < dyn_cfg.min_interval * k_ms_to_us) {
        return dyn_cfg.min_interval * k_ms_to_us;
    }
    if (dyn_cfg.max_interval >= 0 && interval > dyn_cfg.max_interval * k_ms_to_us) {
        return dyn_cfg.max_interval * k_ms_to_us;
    }
    return interval;
}

int64_t TimeIntervalStrategy::fixed_interval_strategy() const {
    return config_.fixed_interval.base_interval * k_ms_to_us;
}

int64_t TimeIntervalStrategy::first_to_first_strategy(
    int64_t current_start, int64_t last_start) const
{
    return to_microseconds(current_start) - to_microseconds(last_start);
}

int64_t TimeIntervalStrategy::last_to_first_strategy(
    int64_t current_start, int64_t last_end) const
{
    return to_microseconds(current_start) - to_microseconds(last_end);
}

int64_t TimeIntervalStrategy::literal_strategy(int64_t current_start) const {
    int64_t now_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    return to_microseconds(current_start) - now_timestamp;
}

void TimeIntervalStrategy::apply_wait_strategy(
    int64_t current_start_time,
    int64_t current_end_time,
    int64_t last_start_time,
    int64_t last_end_time,
    bool is_first_write)
{
    (void)current_end_time;

    if (!config_.enabled || (is_first_write && strategy_type_ != IntervalStrategyType::Literal)) {
        last_write_time_ = std::chrono::steady_clock::now();
        return;
    }

    int64_t wait_time = 0;

    // Calculate wait time
    switch (strategy_type_) {
        case IntervalStrategyType::Fixed:
            wait_time = fixed_interval_strategy();
            break;
        case IntervalStrategyType::FirstToFirst:
            wait_time = first_to_first_strategy(current_start_time, last_start_time);
            wait_time = clamp_interval(wait_time);
            break;
        case IntervalStrategyType::LastToFirst:
            wait_time = last_to_first_strategy(current_start_time, last_end_time);
            wait_time = clamp_interval(wait_time);
            break;
        case IntervalStrategyType::Literal:
            wait_time = literal_strategy(current_start_time);
            break;
        default:
            wait_time = 0;
            break;
    }

    // Execute wait strategy
    if (wait_time > 0) {
        if (config_.wait_strategy == "sleep") {
            precise_sleep_us(wait_time);
        } else { // busy_wait
            auto start = std::chrono::steady_clock::now();
            while (std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count() < wait_time) {
            }
        }
    }

    last_write_time_ = std::chrono::steady_clock::now();
}