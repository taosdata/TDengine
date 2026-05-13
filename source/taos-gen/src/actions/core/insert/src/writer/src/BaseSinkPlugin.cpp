#include "BaseSinkPlugin.hpp"
#include <stdexcept>
#include <chrono>
#include <iostream>

BaseSinkPlugin::BaseSinkPlugin(const InsertDataConfig& config,
                               const ColumnConfigInstanceVector& col_instances,
                               const ColumnConfigInstanceVector& tag_instances,
                               std::shared_ptr<ActionRegisterInfo> action_info)
    : config_(config),
      col_instances_(col_instances),
      tag_instances_(tag_instances),
      timestamp_precision_(config.timestamp_precision),
      time_strategy_(config.time_interval, config.timestamp_precision),
      start_write_time_(std::chrono::steady_clock::now()),
      end_write_time_(std::chrono::steady_clock::now()),
      action_info_(action_info) {

    // Validate timestamp precision
    if (timestamp_precision_.empty()) {
        timestamp_precision_ = "ms";
    } else if (timestamp_precision_ != "ms" &&
               timestamp_precision_ != "us" &&
               timestamp_precision_ != "ns") {
        throw std::invalid_argument("Invalid timestamp precision: " + timestamp_precision_);
    }
}

void BaseSinkPlugin::apply_time_interval_strategy(int64_t current_start, int64_t current_end) {
    time_strategy_.apply_wait_strategy(
        current_start, current_end,
        last_start_time_, last_end_time_,
        first_write_
    );

    if (first_write_) {
        start_write_time_ = time_strategy_.last_write_time();
    }
}

std::string BaseSinkPlugin::get_format_description() const {
    return config_.data_format.format_type;
}

void BaseSinkPlugin::update_write_state(const BaseInsertData& data, bool /* success */ ) {
    end_write_time_ = std::chrono::steady_clock::now();
    last_start_time_ = data.start_time;
    last_end_time_ = data.end_time;
    first_write_ = false;
}

void BaseSinkPlugin::notify(const BaseInsertData& data, bool success) {
    if (action_info_ && action_info_->action && success) {
        std::shared_ptr<ActionBase>& ptr = action_info_->action.value();
        if (ptr) {
            const auto& checkpoint_data = data.get_block()->checkpoint_data_list_;
            std::any payload = std::cref(checkpoint_data);
            ptr->notify(payload);
        }
    }
}

void BaseSinkPlugin::update_play_metrics(const BaseInsertData& data) {
    if (time_strategy_.is_literal_strategy()) {
        double now = TimestampUtils::convert_to_timestamp("us");
        double start_time = TimestampUtils::convert_timestamp_precision_double(data.start_time, timestamp_precision_, "us");
        double elapsed_ms = (now - start_time) / 1000.0;
        elapsed_ms = elapsed_ms < 0 ? 0.0 : elapsed_ms;
        play_metrics_.add_sample(elapsed_ms);
    }
}
