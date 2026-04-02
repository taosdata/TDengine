#pragma once
#include <mutex>
#include <condition_variable>
#include <cstddef>
#include <functional>

class Latch {
public:
    using StopPredicate = std::function<bool()>;

    explicit Latch(std::size_t count);

    void count_down();

    void wait();

    void wait(const StopPredicate& stop_condition);

    void interrupt();

private:
    std::mutex mutex_;
    std::condition_variable cond_;
    std::size_t count_;
};