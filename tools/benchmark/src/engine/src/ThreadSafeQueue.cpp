#include "ThreadSafeQueue.h"

void ThreadSafeQueue::enqueue(DAGNode* node) {
    std::lock_guard<std::mutex> lock(mtx_);
    queue_.push(node);
    cv_.notify_one();
}

DAGNode* ThreadSafeQueue::dequeue() {
    std::unique_lock<std::mutex> lock(mtx_);
    cv_.wait(lock, [this] { return !queue_.empty() || stop_; });
    if (stop_) return nullptr;
    auto* node = queue_.front();
    queue_.pop();
    return node;
}

void ThreadSafeQueue::stop() {
    std::lock_guard<std::mutex> lock(mtx_);
    stop_ = true;
    cv_.notify_all();
}