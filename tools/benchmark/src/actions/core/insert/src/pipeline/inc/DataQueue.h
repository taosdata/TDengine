#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <chrono>


enum class PopStatus {
    Success,
    Timeout,
    Terminated
};

template <typename T>
class DataQueue {
public:
    struct PopResult {
        std::optional<T> data;
        PopStatus status;
    };

    explicit DataQueue(size_t capacity = 1000) : capacity_(capacity) {}
    DataQueue(const DataQueue&) = delete;
    DataQueue& operator=(const DataQueue&) = delete;

    void push(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (terminated_) {
            throw std::runtime_error("Queue has been terminated");
        }

        not_full_.wait(lock, [this] { 
            return (queue_.size() < capacity_) && !terminated_; 
        });

        if (terminated_) {
            throw std::runtime_error("Queue has been terminated");
        }

        queue_.push(std::move(item));
        not_empty_.notify_one();
    }

    bool try_push(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (terminated_ || queue_.size() >= capacity_) {
            return false;
        }

        queue_.push(std::move(item));
        not_empty_.notify_one();
        return true;
    }

    PopResult pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        bool ready = not_empty_.wait_for(lock, std::chrono::milliseconds(100), 
            [this] { return !queue_.empty() || terminated_; });
            
        if (!ready) {
            return PopResult{std::nullopt, PopStatus::Timeout};
        }
        
        if (queue_.empty() && terminated_) {
            return PopResult{std::nullopt, PopStatus::Terminated};
        }
        
        T item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return PopResult{std::move(item), PopStatus::Success};
    }

    PopResult try_pop() {
        std::unique_lock<std::mutex> lock(mutex_);
    
        if (queue_.empty()) {
            if (terminated_)
                return PopResult{std::nullopt, PopStatus::Terminated};
            else
                return PopResult{std::nullopt, PopStatus::Timeout};
        }
        
        T item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return PopResult{std::move(item), PopStatus::Success};
    }

    void terminate() {
        std::lock_guard<std::mutex> lock(mutex_);
        terminated_ = true;
        not_empty_.notify_all();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    size_t capacity_;
    bool terminated_ = false;
};