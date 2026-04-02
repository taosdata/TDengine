#pragma once
#include "ISinkPlugin.hpp"
#include "TimeIntervalStrategy.hpp"
#include <fstream>
#include <filesystem>

class FileSystemWriter : public ISinkPlugin {
public:
    FileSystemWriter(const InsertDataConfig::Target& target);
    ~FileSystemWriter();

    bool connect() override;
    void write(const BaseInsertData& data) override;
    void close() noexcept override;

    void set_time_interval_config(
        const InsertDataConfig::TimeInterval& config) override;

    std::string get_timestamp_precision() const override;

private:
    // Get current file path
    std::filesystem::path get_current_file_path(int64_t timestamp);

    // Write content
    void write(const std::string& data, int64_t timestamp);

    // Time interval strategy executor
    TimeIntervalStrategy time_strategy_;

    // File output stream
    std::ofstream output_file_;

    // Configuration info
    InsertDataConfig::Target target_config_;

    // Current file info
    std::string current_file_path_;
    int64_t current_file_end_time_ = 0;
};