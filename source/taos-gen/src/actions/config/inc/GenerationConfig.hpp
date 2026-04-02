#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>
#include <optional>

struct GenerationConfig {
    struct InterlaceMode {
        bool enabled = false;
        size_t rows = 1;
    } interlace_mode;

    struct DataCache {
        bool enabled = true;
        size_t num_cached_batches = 10000;
    } data_cache;

    struct FlowControl {
        bool enabled = false;
        int64_t rate_limit = 0;
    } flow_control;

    struct DataDisorder {
        bool enabled = false;

        struct Interval {
            std::variant<int64_t, std::string> time_start;
            std::variant<int64_t, std::string> time_end;
            double ratio = 0.0;
            int latency_range = 0;
        };
        std::vector<Interval> intervals;
    } data_disorder;

    std::optional<size_t> generate_threads;
    int64_t rows_per_table = 10000;
    size_t rows_per_batch = 10000;
    bool tables_reuse_data = true;

    GenerationConfig() {
        if (data_cache.enabled) {
            size_t batches_needed = 0;
            if (interlace_mode.enabled) {
                batches_needed = (static_cast<size_t>(rows_per_table) + interlace_mode.rows - 1) / interlace_mode.rows;
            } else {
                batches_needed = (static_cast<size_t>(rows_per_table) + rows_per_batch - 1) / rows_per_batch;
            }

            data_cache.num_cached_batches = std::min<size_t>(batches_needed, data_cache.num_cached_batches);
        }
    }
};