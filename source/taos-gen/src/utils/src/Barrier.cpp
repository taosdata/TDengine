#include "Barrier.hpp"

Barrier::Barrier(std::size_t count)
    : threshold_(count), count_(count), generation_(0) {}

void Barrier::arrive_and_wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    auto gen = generation_;
    if (--count_ == 0) {
        generation_++;
        count_ = threshold_;
        cond_.notify_all();
    } else {
        cond_.wait(lock, [this, gen] { return gen != generation_; });
    }
}