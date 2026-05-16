#pragma once

#include <vector>
#include <optional>
#include <memory>
#include <unordered_map>
#include <queue>
#include "InsertDataConfig.hpp"
#include "RowGenerator.hpp"
#include "TimestampGenerator.hpp"
#include "TableNameCSVReader.hpp"
#include "ICSVRowSource.hpp"
#include "MemoryPool.hpp"

class RowDataGenerator {
public:
    RowDataGenerator(const std::string& table_name,
                    const InsertDataConfig& config,
                    const ColumnConfigInstanceVector& instances,
                    bool use_cache = false,
                    std::shared_ptr<ICSVRowSource> external_csv_source = nullptr);

    // Get next row data
    std::optional<RowData> next_row();
    int next_row(MemoryPool::TableBlock& table_block);

    // Check if there is more data
    bool has_more() const;

    // Get the number of generated rows
    int64_t generated_rows() const { return generated_rows_; }

    // Reset generator state
    void reset();

private:
    // Delayed queue element
    struct DelayedRow {
        int64_t deliver_timestamp;
        RowData row;
        bool operator<(const DelayedRow& other) const {
            return deliver_timestamp > other.deliver_timestamp;
        }
    };

    struct DisorderInterval {
        int64_t start_time;
        int64_t end_time;
        double ratio;
        int latency_range;
    };

    void init_cached_row();

    // Initialize cache
    void init_cache();

    // Initialize disorder
    void init_disorder();

    // Initialize raw data source
    void init_raw_source();

    // Apply disorder strategy
    bool apply_disorder(RowData& row);

    // Process delay queue
    void process_delay_queue();

    // Fetch a row from raw source
    std::optional<std::reference_wrapper<RowData>> fetch_raw_row();

    // Initialize generator components
    void init_generator();

    // Initialize CSV reader
    void init_csv_reader();

    // Get data from generator
    void generate_from_generator();

private:
    const std::string& table_name_;
    const InsertDataConfig& config_;
    const ColumnsConfig& columns_config_;
    const ColumnConfigInstanceVector& instances_;
    const std::string& target_precision_;
    const bool use_cache_;

    // Initialize cache line memory
    RowData cached_row_;

    // Data source components
    std::unique_ptr<RowGenerator> row_generator_;
    std::unique_ptr<TimestampGenerator> timestamp_generator_;  // Only for generator source_type

    // Unified CSV data source (preload and streaming)
    std::shared_ptr<ICSVRowSource> csv_source_;

    // State management
    int64_t generated_rows_ = 0;
    int64_t total_rows_ = 0;
    bool use_generator_ = false;
    bool source_exhausted_ = false;

    // Disorder management
    std::priority_queue<DelayedRow> delay_queue_;
    std::vector<RowData> cache_;

    std::vector<DisorderInterval> disorder_intervals_;

    // Timestamp state
    int64_t current_timestamp_ = 0;
};