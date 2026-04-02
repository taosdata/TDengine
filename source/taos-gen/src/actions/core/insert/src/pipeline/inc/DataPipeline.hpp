#pragma once

#include "blockingconcurrentqueue.hpp"
#include <memory>
#include <vector>
#include <atomic>
#include <optional>
#include <stdexcept>
#include <numeric>
#include <algorithm>
#include <mutex>
#include <condition_variable>
#include <queue>

template <typename T>
class DataPipeline {
public:
    enum class Status {
        Success,
        Timeout,
        Terminated
    };

    struct Result {
        std::optional<T> data;
        Status status;
    };

    DataPipeline(bool use_shared_queue, size_t producer_count, size_t consumer_count, size_t queue_capacity = 100) {
        if (use_shared_queue) {
            impl_ = std::make_unique<SharedQueueImpl>(queue_capacity * consumer_count);
        } else {
            impl_ = std::make_unique<IndependentQueuesImpl>(producer_count, consumer_count, queue_capacity);
        }
    }

    ~DataPipeline() {
        terminate();
    }

    DataPipeline(const DataPipeline&) = delete;
    DataPipeline& operator=(const DataPipeline&) = delete;

    void push_data(size_t producer_id, T formatted_data) {
        impl_->push_data(producer_id, std::move(formatted_data));
    }

    Result fetch_data(size_t consumer_id) {
        return impl_->fetch_data(consumer_id);
    }

    void terminate() {
        impl_->terminate();
    }

    size_t total_queued() const {
        return impl_->total_queued();
    }

private:
    struct Implementation {
        virtual ~Implementation() = default;
        virtual void push_data(size_t producer_id, T formatted_data) = 0;
        virtual Result fetch_data(size_t consumer_id) = 0;
        virtual void terminate() = 0;
        virtual size_t total_queued() const = 0;
    };

    class IndependentQueuesImpl : public Implementation {
    public:
        IndependentQueuesImpl(size_t producer_count, size_t consumer_count, size_t queue_capacity)
            : producer_count_(producer_count), consumer_count_(consumer_count) {
            queues_.reserve(consumer_count);
            for (size_t i = 0; i < consumer_count; ++i) {
                queues_.emplace_back(std::make_unique<DataQueue>(queue_capacity));
            }
        }

        ~IndependentQueuesImpl() {
            terminate();
        }

        void push_data(size_t producer_id, T formatted_data) override {
            if (terminated_.load()) {
                throw std::runtime_error("Pipeline has been terminated");
            }

            if (producer_id >= producer_count_) {
                throw std::out_of_range("Invalid producer ID");
            }

            size_t queue_index = producer_id % consumer_count_;
            auto& queue = queues_[queue_index];
            queue->push(std::move(formatted_data));
        }

        Result fetch_data(size_t consumer_id) override {
            if (consumer_id >= consumer_count_) {
                throw std::out_of_range("Invalid consumer ID");
            }

            auto& queue = queues_[consumer_id];
            auto result = queue->pop();

            if (result.status == DataQueue::PopStatus::Success) {
                return {std::move(result.data), Status::Success};
            } else if (result.status == DataQueue::PopStatus::Terminated) {
                return {std::nullopt, Status::Terminated};
            } else {
                return {std::nullopt, Status::Timeout};
            }
        }

        void terminate() override {
            terminated_.store(true);
            for (auto& queue : queues_) {
                queue->terminate();
            }
        }

        size_t total_queued() const override {
            size_t total = 0;
            for (const auto& queue : queues_) {
                total += queue->size();
            }
            return total;
        }

    private:
        class DataQueue {
        public:
            enum class PopStatus {
                Success,
                Timeout,
                Terminated
            };

            struct PopResult {
                std::optional<T> data;
                PopStatus status;
            };

            explicit DataQueue(size_t capacity = 100) : capacity_(capacity) {}
            DataQueue(const DataQueue&) = delete;
            DataQueue& operator=(const DataQueue&) = delete;

            void push(T item) {
                std::unique_lock<std::mutex> lock(mutex_);
                if (terminated_) {
                    throw std::runtime_error("Queue has been terminated");
                }

                not_full_.wait(lock, [this] {
                    return (queue_.size() < capacity_) || terminated_;
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
                not_full_.notify_all();
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

        size_t producer_count_;
        size_t consumer_count_;
        std::atomic<bool> terminated_{false};
        std::vector<std::unique_ptr<DataQueue>> queues_;
    };

    class SharedQueueImpl : public Implementation {
    public:
        SharedQueueImpl(size_t queue_capacity) : queue_(queue_capacity) {}

        ~SharedQueueImpl() {
            terminate();
        }

        void push_data(size_t /* producer_id */, T formatted_data) override {
            if (terminated_.load(std::memory_order_acquire)) {
                throw std::runtime_error("Pipeline has been terminated");
            }
            queue_.enqueue(std::make_unique<T>(std::move(formatted_data)));
        }

        Result fetch_data(size_t /* consumer_id */) override {
            std::unique_ptr<T> item;

            while (true) {
                if (queue_.wait_dequeue_timed(item, std::chrono::milliseconds(100))) {
                    if (item) {
                        return Result{std::move(*item), Status::Success};
                    } else {
                        if (queue_.size_approx() == 0) {
                            queue_.enqueue(nullptr);
                            return Result{std::nullopt, Status::Terminated};
                        } else {
                            continue;
                        }
                    }
                } else {
                    if (terminated_.load(std::memory_order_acquire) && queue_.size_approx() == 0) {
                        return Result{std::nullopt, Status::Terminated};
                    }
                }
            }
        }

        void terminate() override {
            terminated_.store(true, std::memory_order_release);
            queue_.enqueue(nullptr);
        }

        size_t total_queued() const override {
            return queue_.size_approx();
        }

    private:
        std::atomic<bool> terminated_{false};
        moodycamel::BlockingConcurrentQueue<std::unique_ptr<T>> queue_;
    };

    std::unique_ptr<Implementation> impl_;
};