#pragma once
#include <cstdint>

struct CheckpointInfo {
    bool enabled = false;
    size_t interval_sec = 60; // Checkpoint interval in seconds
};