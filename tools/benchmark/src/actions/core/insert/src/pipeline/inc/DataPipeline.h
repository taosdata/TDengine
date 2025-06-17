#pragma once

#include "DataQueue.h"
#include <vector>
#include <memory>
#include <atomic>
#include <numeric>
#include <algorithm>


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

    DataPipeline(size_t producer_count, 
                    size_t consumer_count,
                    size_t queue_capacity = 1024)
        : producer_count_(producer_count),
          consumer_count_(consumer_count),
          consumer_states_(consumer_count)
    {
        queues_.reserve(producer_count);
        for (size_t i = 0; i < producer_count; ++i) {
            queues_.emplace_back(
                std::make_unique<DataQueue<T>>(queue_capacity)
            );
        }

        initialize_queue_mappings();
    }

    ~DataPipeline() {
        terminate();
    }

    DataPipeline(const DataPipeline&) = delete;
    DataPipeline& operator=(const DataPipeline&) = delete;

    DataPipeline(DataPipeline&&) = default;
    DataPipeline& operator=(DataPipeline&&) = default;

    // 生产者接口
    void push_data(size_t producer_id, T formatted_data) {
        if (terminated_.load())
            throw std::runtime_error("Pipeline has been terminated");

        if (producer_id >= queues_.size()) 
            throw std::out_of_range("Invalid producer ID");
        
        queues_[producer_id]->push(std::move(formatted_data));
    }

    // 消费者接口
    Result fetch_data(size_t consumer_id) {
        auto& state = consumer_states_.at(consumer_id);
        const size_t start = state.current_index;
        size_t checked = 0;
        
        while (checked++ < state.active_queues.size()) {
            const size_t qidx = state.active_queues[state.current_index];
            auto result = queues_[qidx]->try_pop();
            
            switch (result.status) {
            case PopStatus::Success:
                advance_pointer(state);
                return {std::move(result.data), Status::Success};
            case PopStatus::Terminated:
                deactivate_queue(state, qidx);
                break;
            default: 
                advance_pointer(state);
                break;
            }
        }
        
        return {std::nullopt, state.active_queues.empty() ? 
            Status::Terminated : Status::Timeout};
    }

    // 终止信号
    void terminate() {
        terminated_.store(true);
        for (auto& queue : queues_) {
            queue->terminate();
        }
    }
    
    // 状态监控
    size_t total_queued() const {
        size_t total = 0;
        for (const auto& queue : queues_) {
            total += queue->size();
        }
        return total;
    }

private:
    struct ConsumerState {
        std::vector<size_t> active_queues;
        size_t current_index = 0;
    };

    void initialize_queue_mappings() {
        consumer_states_.resize(consumer_count_);
    
        if (consumer_count_ > producer_count_) {
            // 消费者数量大于队列数量
            size_t consumers_per_queue = consumer_count_ / producer_count_;
            size_t remainder = consumer_count_ % producer_count_;
            
            size_t current_consumer = 0;
            for (size_t i = 0; i < producer_count_; ++i) {
                size_t count = consumers_per_queue + (i < remainder ? 1 : 0);
                for (size_t j = 0; j < count; ++j) {
                    consumer_states_[current_consumer++].active_queues.push_back(i);
                }
            }
        } else {
            // 消费者数量小于等于队列数量
            size_t queues_per_consumer = producer_count_ / consumer_count_;
            size_t remainder = producer_count_ % consumer_count_;

            size_t current_queue = 0;
            for (size_t i = 0; i < consumer_count_; ++i) {
                size_t count = queues_per_consumer + (i < remainder ? 1 : 0);
                for (size_t j = 0; j < count; ++j) {
                    consumer_states_[i].active_queues.push_back(current_queue++);
                }
            }
        }
    }

    void advance_pointer(ConsumerState& state) {
        state.current_index = (state.current_index + 1) % state.active_queues.size();
    }

    void deactivate_queue(ConsumerState& state, size_t qidx) {
        auto it = std::find(state.active_queues.begin(), state.active_queues.end(), qidx);
        if (it != state.active_queues.end()) {
            state.active_queues.erase(it);
            if (state.current_index >= state.active_queues.size()) {
                state.current_index = 0;
            }
        }
    }

    std::vector<std::unique_ptr<DataQueue<T>>> queues_;
    std::vector<ConsumerState> consumer_states_;
    
    size_t producer_count_;
    size_t consumer_count_;
    std::atomic<bool> terminated_{false};
};
