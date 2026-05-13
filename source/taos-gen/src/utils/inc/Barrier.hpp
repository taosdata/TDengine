#pragma once
#include <mutex>
#include <condition_variable>
#include <cstddef>

class Barrier {
public:
    explicit Barrier(std::size_t count);
    void arrive_and_wait();
private:
    std::mutex mutex_;
    std::condition_variable cond_;
    std::size_t threshold_;
    std::size_t count_;
    std::size_t generation_;
};