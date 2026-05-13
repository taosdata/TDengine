#pragma once
#include <cstdint>
#include "InsertDataConfig.hpp"

struct CheckpointData {
    std::string table_name;
    int64_t last_checkpoint_time = 0;
    int writeCount = 0;

    CheckpointData() = default;
    CheckpointData(const std::string& name, int64_t timestamp, int count)
        : table_name(name), last_checkpoint_time(timestamp), writeCount(count) {}
};