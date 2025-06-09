
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
// 数据队列模块

// 数据队列管理模块
template<typename T>
class DataPipeline {
public:
    void push(T&& data) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(data));
        cond_.notify_one();
    }

    bool try_pop(T& data) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) return false;
        data = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cond_;
};
