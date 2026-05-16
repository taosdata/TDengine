#pragma once

#include <thread>
#include <vector>
#include <atomic>
#include <functional>
#include <memory>
#include <stdexcept>
#include "blockingconcurrentqueue.hpp"
#include "MemoryPool.hpp"

template<typename>
struct is_variant : std::false_type {};

template<typename... Ts>
struct is_variant<std::variant<Ts...>> : std::true_type {};

template <typename T>
class GarbageCollector {
public:
    struct Task {
        std::function<void()> destructor;

        Task() = default;
        Task(Task&&) noexcept = default;
        Task& operator=(Task&&) noexcept = default;
    };

    // Constructor: specify the number of consumer threads
    explicit GarbageCollector(size_t num_consumer_threads = 4)
        : terminated_(false) {

        if (num_consumer_threads == 0)
            throw std::invalid_argument("Number of consumer threads must be at least 1");

        // Start consumer threads
        threads_.reserve(num_consumer_threads);
        for (size_t i = 0; i < num_consumer_threads; ++i) {
            threads_.emplace_back([this] { worker_thread(); });
        }
    }

    ~GarbageCollector() {
        terminate();
        for (auto& t : threads_) {
            if (t.joinable()) t.join();
        }
    }

    // Submit an object to be collected
    void dispose(T&& data) {
        if (terminated_.load(std::memory_order_relaxed)) return;

        using DataType = std::decay_t<T>;
        auto ptr = std::make_shared<DataType>(std::move(data));

        Task task;
        task.destructor = [ptr, this]() {
            (void)ptr;
            (void)this;
            // if constexpr (is_variant<DataType>::value) {
            //     // DataType is a std::variant
            //     std::visit([this](auto& obj) {
            //         using U = std::decay_t<decltype(obj)>;
            //         if constexpr (std::is_same_v<U, StmtV2InsertData>) {
            //             obj.block->release();
            //         }
            //     }, *ptr);
            // } else {
            //     // Other types do not require special handling
            // }
        };


        queue_.enqueue(std::move(task));
    }

    // Terminate the collector
    void terminate() {
        terminated_.store(true, std::memory_order_release);
    }

private:
    void worker_thread() {
        Task task;
        while (!terminated_.load(std::memory_order_acquire)) {
            if (queue_.wait_dequeue_timed(task, std::chrono::milliseconds(100))) {
                if (task.destructor) {
                    task.destructor();
                }
            }
        }

        // Clear remaining tasks
        while (queue_.try_dequeue(task)) {
            if (task.destructor) {
                task.destructor();
            }
        }
    }

private:
    moodycamel::BlockingConcurrentQueue<Task> queue_;
    std::vector<std::thread> threads_;
    std::atomic<bool> terminated_;
};