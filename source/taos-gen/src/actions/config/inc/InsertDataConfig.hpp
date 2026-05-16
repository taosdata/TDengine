#pragma once

#include "TableNameConfig.hpp"
#include "TagsConfig.hpp"
#include "ColumnsConfig.hpp"
#include "CheckpointInfo.hpp"
#include "DatabaseInfo.hpp"
#include "SuperTableInfo.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"
#include "SchemaConfig.hpp"
#include "PluginExtensions.hpp"
#include <string>
#include <vector>

struct InsertDataConfig {
    SchemaConfig schema;
    PluginExtensions extensions;

    std::string target_type = "tdengine";
    std::string timestamp_precision = "ms";
    DataFormat data_format;

    size_t insert_threads = 8;
    size_t queue_capacity = 10;
    double queue_warmup_ratio = 0.0;
    bool shared_queue = false;
    std::string thread_allocation = "index_range";
    bool thread_affinity = false;
    bool thread_realtime = false;

    struct FailureHandling {
        size_t max_retries = 0;
        int retry_interval_ms = 1000;
        std::string on_failure = "exit";            // exit or warn_and_continue
    } failure_handling;

    struct TimeInterval {
        bool enabled = false;                       // Enable interval control
        std::string interval_strategy = "fixed";    // Time interval strategy type
        std::string wait_strategy = "sleep";        // Wait strategy type: sleep or busy_wait

        struct FixedInterval {
            int base_interval = 1000;               // Fixed interval value in milliseconds
            int random_deviation = 0;               // Random deviation
        } fixed_interval;

        struct DynamicInterval {
            int min_interval = -1;                  // Minimum interval threshold
            int max_interval = -1;                  // Maximum interval threshold
        } dynamic_interval;
    } time_interval;

    CheckpointInfo checkpoint_info;
};