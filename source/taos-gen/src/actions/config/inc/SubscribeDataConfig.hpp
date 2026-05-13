#pragma once

#include <string>
#include <vector>
#include <map>
#include <optional>
#include "TDengineConfig.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"

struct SubscribeDataConfig {
    struct Source {
        TDengineConfig connection_info; // Database connection info
    } source;

    struct Control {
        DataFormat data_format;   // Data format config
        DataChannel data_channel; // Data channel config

        struct SubscribeControl {
            std::string log_path = "result.txt"; // Log file path
            bool enable_dryrun = false;         // Enable dry run

            struct Execution {
                int consumer_concurrency = 1; // Number of concurrent consumers
                int poll_timeout = 1000;      // Poll timeout (ms)
            } execution;

            struct Topic {
                std::string name; // Topic name
                std::string sql;  // SQL to create topic
            };
            std::vector<Topic> topics; // Subscribed topics list

            struct Commit {
                std::string mode = "auto"; // Commit mode (auto or manual)
            } commit;

            struct GroupID {
                std::string strategy;       // Group ID strategy (shared, independent, custom)
                std::optional<std::string> custom_id; // Custom Group ID (required if strategy is custom)
            } group_id;

            struct Output {
                std::string path;         // Output file path
                std::string file_prefix;  // Output file prefix
                std::optional<int> expected_rows; // Expected rows per consumer
            } output;

            std::map<std::string, std::string> advanced; // Advanced config (key-value map)
        } subscribe_control;
    } control;
};
