#pragma once
#include <cstdint>
#include "InsertDataConfig.hpp"

struct CheckpointActionConfig {
    bool enabled = false;
    int interval_sec = 60; // Checkpoint interval in seconds
    uint64_t start_timestamp = 0;
    int tableCount = 0;
    std::string timestamp_precision = "ms";
    int timestamp_step = 1;

    CheckpointActionConfig() = default;
    CheckpointActionConfig(const InsertDataConfig& config){
        enabled = config.checkpoint_info.enabled;
        interval_sec = config.checkpoint_info.interval_sec;
        if (enabled) {
            TimestampGeneratorConfig ts_config = config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config;
            this->start_timestamp = TimestampUtils::parse_timestamp(ts_config.start_timestamp, ts_config.timestamp_precision);
            this->timestamp_step = static_cast<int>(std::get<Timestamp>(ts_config.timestamp_step));
            this->timestamp_precision = ts_config.timestamp_precision;
            this->tableCount = config.schema.tbname.generator.count - config.schema.tbname.generator.from;
        }
    }
};
