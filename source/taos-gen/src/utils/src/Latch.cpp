#include "Latch.hpp"
#include <chrono>

Latch::Latch(std::size_t count) : count_(count) {}

void Latch::count_down() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (count_ > 0) {
        if (--count_ == 0) {
            cond_.notify_all();
        }
    }
}

void Latch::wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [this] { return count_ == 0; });
}

void Latch::wait(const StopPredicate& stop_condition) {
    std::unique_lock<std::mutex> lock(mutex_);
    while (count_ > 0 && !stop_condition()) {
        cond_.wait_for(lock, std::chrono::milliseconds(100));
    }
}

void Latch::interrupt() {
    cond_.notify_all();
}