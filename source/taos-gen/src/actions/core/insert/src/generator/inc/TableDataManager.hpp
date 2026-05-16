#pragma once

#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include <optional>
#include <atomic>
#include "RowDataGenerator.hpp"
#include "InsertDataConfig.hpp"
#include "RateLimiter.hpp"
#include "MemoryPool.hpp"

class TableDataManager {
public:
    struct TableState {
        std::string table_name;
        std::unique_ptr<RowDataGenerator> generator;
        int64_t rows_generated = 0;         // Total rows generated for this table
        int64_t interlace_counter = 0;      // Current row count in interlace mode
        bool completed = false;             // Whether this table is completed
        const MemoryPool::TableBlock::Tags* tags_ptr = nullptr;

        TableState(const TableState&) = delete;
        TableState& operator=(const TableState&) = delete;

        TableState(TableState&&) = default;
        TableState& operator=(TableState&&) = default;

        TableState() = default;
    };

    explicit TableDataManager(MemoryPool& pool,
                              const InsertDataConfig& config,
                              const ColumnConfigInstanceVector& col_instances,
                              const ColumnConfigInstanceVector& tag_instances);

    // Initialize the table data manager
    bool init(const std::vector<std::string>& table_names);

    // Get the next batch of data
    std::optional<MemoryPool::MemoryBlock*> next_multi_batch();

    // Check if there is more data available
    bool has_more() const;

    // Get the current table name
    std::string current_table() const;

    // Get table states
    const std::vector<TableState>&  table_states() const;

    // Acquire tokens for flow control
    void acquire_tokens(int64_t tokens);

    size_t get_total_rows_generated() const;

    const ColumnConfigInstanceVector& get_column_instances() const;

private:
    MemoryPool& pool_;
    const InsertDataConfig& config_;
    const ColumnConfigInstanceVector& col_instances_;
    const ColumnConfigInstanceVector& tag_instances_;
    std::vector<TableState> table_states_;
    size_t prev_table_index_ = 0;
    size_t current_table_index_ = 0;            // Current table index
    size_t active_table_count_ = 0;             // Count of active tables with data available
    int64_t interlace_rows_ = 1;                // Number of rows to generate per table in interlace mode
    size_t sequence_num_ = 0;

    // Generate tag values for a given table
    std::vector<ColumnType> generate_tags_for_table(const std::string& table_name);

    // Get the next table with available data
    TableState* get_next_active_table();

    // Calculate number of rows to generate for current table
    size_t calculate_rows_to_generate(TableState& state) const;

    // Switch to the next table
    void advance_to_next_table();

    // Get batch data respecting size limits
    MemoryPool::MemoryBlock* collect_batch_data(size_t max_rows);

    // Flow control
    std::unique_ptr<RateLimiter> rate_limiter_;

    std::atomic<size_t> total_rows_generated_{0};
};