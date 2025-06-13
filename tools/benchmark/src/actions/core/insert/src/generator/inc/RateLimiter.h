#pragma once
#include <chrono>
#include <thread>

class RateLimiter {
public:
    explicit RateLimiter(int64_t rate_limit);
    
    // Acquire specified number of tokens
    void acquire(int64_t tokens = 1);

private:
    int64_t rate_limit_;  // Tokens per second
    double tokens_;       // Current token count
    std::chrono::steady_clock::time_point last_time_;
};