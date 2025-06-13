#include "RateLimiter.h"
#include <algorithm>

RateLimiter::RateLimiter(int64_t rate_limit)
    : rate_limit_(rate_limit),
      tokens_(0),
      last_time_(std::chrono::steady_clock::now()) {}

void RateLimiter::acquire(int64_t tokens) {
    using namespace std::chrono;
    auto now = steady_clock::now();
    auto elapsed = duration_cast<milliseconds>(now - last_time_).count();
    
    // Calculate new tokens
    tokens_ += elapsed * rate_limit_ / 1000.0;
    tokens_ = std::min(tokens_, static_cast<double>(rate_limit_));
    
    // Check for sufficient tokens
    while (tokens_ < tokens) {
        // Calculate wait time
        double deficit = tokens - tokens_;
        int64_t wait_ms = std::max(static_cast<int64_t>(1), static_cast<int64_t>(deficit * 1000 / rate_limit_));
        std::this_thread::sleep_for(milliseconds(wait_ms));
        
        // Update tokens
        now = steady_clock::now();
        elapsed = duration_cast<milliseconds>(now - last_time_).count();
        tokens_ += elapsed * rate_limit_ / 1000;
        tokens_ = std::min(tokens_, static_cast<double>(rate_limit_));
    }
    
    // Consume tokens
    tokens_ -= tokens;
    last_time_ = now;
}