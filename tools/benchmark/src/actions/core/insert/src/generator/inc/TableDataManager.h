#pragma once

#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include <optional>
#include <atomic>
#include "RowDataGenerator.h"
#include "InsertDataConfig.h"
#include "RateLimiter.h"

class TableDataManager {
public:
    struct TableState {
        std::unique_ptr<RowDataGenerator> generator;
        int64_t rows_generated = 0;         // Total rows generated for this table
        int64_t interlace_counter = 0;      // Current row count in interlace mode
        bool completed = false;             // Whether this table is completed

        TableState(const TableState&) = delete;
        TableState& operator=(const TableState&) = delete;
    
        TableState(TableState&&) = default;
        TableState& operator=(TableState&&) = default;
    
        TableState() = default;
    };
    
    explicit TableDataManager(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances);
    
    // Initialize the table data manager
    bool init(const std::vector<std::string>& table_names);
    
    // Get the next batch of data
    std::optional<MultiBatch> next_multi_batch();
    
    // Check if there is more data available
    bool has_more() const;
    
    // Get the current table name
    std::string current_table() const;
    
    // Get table states
    const std::unordered_map<std::string, TableState>& table_states() const;

    // Acquire tokens for flow control
    void acquire_tokens(int64_t tokens);

    size_t get_total_rows_generated() const;

private:
    const InsertDataConfig& config_;
    const ColumnConfigInstanceVector& col_instances_;
    std::unordered_map<std::string, TableState> table_states_;
    std::vector<std::string> table_order_;      // Order of table names
    size_t current_table_index_ = 0;            // Current table index
    int64_t interlace_rows_ = 1;                // Number of rows to generate per table in interlace mode
    
    // Get the next table with available data
    TableState* get_next_active_table();
    
    // Calculate number of rows to generate for current table
    int64_t calculate_rows_to_generate(TableState& state) const;
    
    // Switch to the next table
    void advance_to_next_table();

    // Get batch data respecting size limits
    MultiBatch collect_batch_data(int64_t max_rows);

    // Flow control
    std::unique_ptr<RateLimiter> rate_limiter_;

    std::atomic<size_t> total_rows_generated_{0};
};