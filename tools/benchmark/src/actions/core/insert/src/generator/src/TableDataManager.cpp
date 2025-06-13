#include "TableDataManager.h"
#include <algorithm>
#include <limits>
#include <iostream>


TableDataManager::TableDataManager(const InsertDataConfig& config)
    : config_(config) {
    
    // Set interlace rows
    if (config_.control.data_generation.interlace_mode.enabled) {
        interlace_rows_ = config_.control.data_generation.interlace_mode.rows;
    } else {
        interlace_rows_ = std::numeric_limits<int64_t>::max();
    }

    // 初始化流控
    if (config_.control.data_generation.flow_control.enabled) {
        rate_limiter_ = std::make_unique<RateLimiter>(
            config_.control.data_generation.flow_control.rate_limit
        );
    }
}

bool TableDataManager::init(const std::vector<std::string>& table_names) {
    if (table_names.empty()) {
        std::cerr << "TableDataManager initialized with empty table list" << std::endl;
        return false;
    }
    
    table_order_ = table_names;

    // Create RowDataGenerator for each table
    for (const auto& table_name : table_names) {
        TableState state;
        try {
            state.generator = std::make_unique<RowDataGenerator>(
                table_name,
                config_.source.columns,
                config_.control,
                config_.target.timestamp_precision
            );
            state.rows_generated = 0;
            state.interlace_counter = 0;
            state.completed = false;
            
            table_states_[table_name] = std::move(state);
        } catch (const std::exception& e) {
            std::cerr << "Failed to create RowDataGenerator for table: " << table_name 
                     << " - " << e.what() << std::endl;
            return false;
        }
    }
    
    current_table_index_ = 0;
    return true;
}

void TableDataManager::acquire_tokens(int64_t tokens) {
    rate_limiter_->acquire(tokens);
}

std::optional<MultiBatch> TableDataManager::next_multi_batch() {
    if (!has_more()) {
        return std::nullopt;
    }

    // Get maximum rows per request from config
    int64_t max_rows = config_.control.insert_control.per_request_rows;
    if (max_rows <= 0) {
        max_rows = std::numeric_limits<int64_t>::max();
    }

    return collect_batch_data(max_rows);
}

MultiBatch TableDataManager::collect_batch_data(int64_t max_rows) {
    MultiBatch result;
    
    while (result.total_rows < max_rows && has_more()) {
        TableState* table_state = get_next_active_table();
        if (!table_state) break;
        
        const std::string& table_name = table_order_[current_table_index_];
        std::vector<RowData> batch;
        int64_t actual_generated = 0;
        
        // Calculate how many rows we can still add
        int64_t remaining_batch_space = max_rows - result.total_rows;
        
        // Calculate rows to generate for this table
        int64_t rows_to_generate = std::min(
            calculate_rows_to_generate(*table_state),
            remaining_batch_space
        );
    
        // Generate data
        for (int64_t i = 0; i < rows_to_generate; ++i) {
            if (auto row = table_state->generator->next_row()) {
                if (row->timestamp < 0) {
                    --i;
                    continue;
                }
                batch.push_back(std::move(*row));
                table_state->rows_generated++;
                table_state->interlace_counter++;
                actual_generated++;
            } else {
                table_state->completed = true;
                break;
            }
        }
        
        // Update state
        if (table_state->interlace_counter >= interlace_rows_ || 
            table_state->completed || 
            table_state->rows_generated >= config_.control.data_generation.per_table_rows) {
            advance_to_next_table();
        }

        if (!batch.empty()) {
            int64_t batch_size = batch.size();

            // Handle flow control
            if (rate_limiter_) {
                acquire_tokens(batch_size);
            }
            result.total_rows += batch_size;
            result.table_batches.emplace_back(table_name, std::move(batch));
        }
    }
    
    return result;
}

bool TableDataManager::has_more() const {
    for (const auto& [_, state] : table_states_) {
        if (!state.completed && state.rows_generated < config_.control.data_generation.per_table_rows) {
            return true;
        }
    }
    return false;
}

std::string TableDataManager::current_table() const {
    if (table_order_.empty()) return "";
    return table_order_[current_table_index_];
}

const std::unordered_map<std::string, TableDataManager::TableState>& TableDataManager::table_states() const {
    return table_states_;
}

TableDataManager::TableState* TableDataManager::get_next_active_table() {
    if (table_order_.empty()) return nullptr;
    
    // Loop through tables to find one with available data
    size_t start_index = current_table_index_;
    size_t attempts = 0;
    
    do {
        const std::string& table_name = table_order_[current_table_index_];
        auto it = table_states_.find(table_name);
        if (it != table_states_.end()) {
            TableState& state = it->second;
            
            // Check if table still has data and is not completed
            if (!state.completed && 
                state.rows_generated < config_.control.data_generation.per_table_rows && 
                state.generator->has_more()) {
                return &state;
            }
        }
        
        // Move to next table
        current_table_index_ = (current_table_index_ + 1) % table_order_.size();
        attempts++;
        
    } while (attempts < table_order_.size());
    
    return nullptr;
}

int64_t TableDataManager::calculate_rows_to_generate(TableState& state) const {
    // Calculate remaining row limit
    int64_t remaining_rows = std::min(
        config_.control.data_generation.per_table_rows - state.rows_generated,
        state.generator->has_more() ? std::numeric_limits<int64_t>::max() : 0
    );
    
    // Calculate number of rows to generate this time
    int64_t rows_to_generate = std::min(
        interlace_rows_ - state.interlace_counter,
        remaining_rows
    );
    
    // Consider flow control
    // if (config_.control.data_generation.flow_control.rate_limit > 0) {
    //     rows_to_generate = std::min(
    //         rows_to_generate,
    //         config_.control.data_generation.flow_control.rate_limit / static_cast<int>(table_order_.size())
    //     );
    // }
    
    return std::max(static_cast<int64_t>(1), rows_to_generate);
}

void TableDataManager::advance_to_next_table() {
    if (table_order_.empty()) return;
    
    // Reset interlace counter for current table
    const std::string& current_table_name = table_order_[current_table_index_];
    if (auto it = table_states_.find(current_table_name); it != table_states_.end()) {
        it->second.interlace_counter = 0;
    }
    
    // Move to next table
    current_table_index_ = (current_table_index_ + 1) % table_order_.size();
}