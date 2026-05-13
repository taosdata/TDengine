#pragma once
#include "ISinkPlugin.hpp"
#include "ISinkContext.hpp"
#include "TimeIntervalStrategy.hpp"
#include "TimeRecorder.hpp"
#include "LogUtils.hpp"
#include "CheckpointAction.hpp"
#include "ActionRegisterInfo.hpp"
#include <functional>

class BaseSinkPlugin : public ISinkPlugin {
public:
    explicit BaseSinkPlugin(const InsertDataConfig& config,
                            const ColumnConfigInstanceVector& col_instances,
                            const ColumnConfigInstanceVector& tag_instances,
                            std::shared_ptr<ActionRegisterInfo> action_info = nullptr);
    virtual ~BaseSinkPlugin() = default;
    bool prepare() override {
        return true;
    }

    bool connect_with_source(std::optional<ConnectorSource>& conn_source) override {
        (void)conn_source;
        return connect();
    }

    // Public method implementations
    std::string get_timestamp_precision() const noexcept override { return timestamp_precision_; }
    const ActionMetrics& get_play_metrics() const noexcept override { return play_metrics_; }
    const ActionMetrics& get_write_metrics() const noexcept override { return write_metrics_; }
    std::chrono::steady_clock::time_point start_write_time() const noexcept override {
        return start_write_time_;
    }
    std::chrono::steady_clock::time_point end_write_time() const noexcept override {
        return end_write_time_;
    }

protected:
    // Public member variables
    const InsertDataConfig& config_;
    const ColumnConfigInstanceVector& col_instances_;
    const ColumnConfigInstanceVector& tag_instances_;
    std::unique_ptr<const ISinkContext> context_;
    std::string timestamp_precision_;
    TimeIntervalStrategy time_strategy_;
    std::chrono::steady_clock::time_point start_write_time_;
    std::chrono::steady_clock::time_point end_write_time_;

    // State tracking
    bool first_write_ = true;
    int64_t last_start_time_ = 0;
    int64_t last_end_time_ = 0;

    // Performance statistics
    ActionMetrics play_metrics_;
    ActionMetrics write_metrics_;

    // Public utility methods
    void apply_time_interval_strategy(int64_t current_start, int64_t current_end);
    std::string get_format_description() const;
    void update_write_state(const BaseInsertData& data, bool success);
    void notify(const BaseInsertData& data, bool success);
    std::shared_ptr<ActionRegisterInfo> action_info_;

    template<typename Func>
    bool execute_with_retry(Func&& operation, const std::string& error_context) {
        const size_t MAX_RETRIES = config_.failure_handling.max_retries;
        size_t current_retry_count = 0;

        do {
            try {
                bool success = operation();
                if (success) return true;
            } catch (const std::exception& e) {
                LogUtils::error("{} failed reason: {}", error_context, e.what());
                break;
            }

            current_retry_count++;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(config_.failure_handling.retry_interval_ms)
            );

        } while (current_retry_count <= MAX_RETRIES);

        // Failure handling strategy
        if (config_.failure_handling.on_failure == "exit") {
            throw std::runtime_error(error_context + " operation failed");
        }

        LogUtils::error("{} failed after {} retries", error_context, MAX_RETRIES);
        return false;
    }

    void update_play_metrics(const BaseInsertData& data);
};