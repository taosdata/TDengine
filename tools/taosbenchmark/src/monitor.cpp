
class PerformanceMonitor {
    public:
        void print_stats() const {
            std::cout << "┌───────────────────────────┐\n"
                      << "│     写入性能统计       │\n"
                      << "├──────────────┬──────────┤\n"
                      << "│ 总记录数    │ " << total_records_ << " │\n"
                      << "│ 吞吐量      │ " << throughput_ << " rec/s │\n"
                      << "│ 队列深度    │ " << queue_size_ << " │\n"
                      << "└──────────────┴──────────┘\n";
        }
    
        void update(const AsyncWriteEngine& engine) {
            total_records_ = engine.get_total_written();
            queue_size_ = engine.get_queue_size();
            // 计算吞吐量...
        }
};


class TelemetryCollector {
    public:
        void add_metric(const std::string& name, std::function<double()> collector);
        void export_prometheus(std::ostream& out);
};


class MetricCollector {
public:
    void record_query(double latency, bool success) {
        metrics_.total_queries++;
        if (success) metrics_.succeeded++;
        else metrics_.failed++;
        
        std::lock_guard<std::mutex> lock(latency_mutex_);
        metrics_.latencies.push_back(latency);
    }

    void print_report() const {
        std::cout << "┌─────────────────────────────┐\n"
                  << "│        查询性能报告        │\n"
                  << "├───────────────┬───────────┤\n"
                  << "│ 总查询次数    │ " << metrics_.total_queries << " │\n"
                  << "│ 成功次数      │ " << metrics_.succeeded << " │\n"
                  << "│ 平均延迟(ms)  │ " << calculate_avg() << " │\n"
                  << "└───────────────┴───────────┘\n";
    }

private:
    QueryMetrics metrics_;
    mutable std::mutex latency_mutex_;
};



struct QueryMetrics {
    std::atomic<size_t> total_queries{0};
    std::atomic<size_t> succeeded{0};
    std::atomic<size_t> failed{0};
    std::vector<double> latencies; // 单位：毫秒
};



struct DatabaseStats {
    size_t generated_records;
    size_t written_records;
    size_t queue_size;
    std::map<std::string, size_t> error_codes;
};

class StatsCollector {
public:
    void update(size_t db_index, const DatabaseStats& stats) {
        std::lock_guard<std::mutex> lock(mutex_);
        db_stats_[db_index] = stats;
    }

    void print_dashboard() const {
        // 实现类似taosBenchmark的实时监控界面
        std::cout << "\033[2J\033[H"; // 清屏
        for (const auto& [idx, stat] : db_stats_) {
            std::cout << fmt::format("DB{}: {} records/s | Queue: {}\n", 
                idx, stat.throughput, stat.queue_size);
        }
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<size_t, DatabaseStats> db_stats_;
};



