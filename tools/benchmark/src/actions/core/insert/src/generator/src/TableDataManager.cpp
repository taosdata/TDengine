#include "TableDataManager.h"
#include <algorithm>
#include <limits>
#include <iostream>


TableDataManager::TableDataManager(const InsertDataConfig& config)
    : config_(config) {
    
    // 设置交错行数
    if (config_.control.data_generation.interlace_mode.enabled) {
        interlace_rows_ = config_.control.data_generation.interlace_mode.rows;
    } else {
        interlace_rows_ = std::numeric_limits<int64_t>::max();
    }
}

bool TableDataManager::init(const std::vector<std::string>& table_names) {
    if (table_names.empty()) {
        std::cerr << "TableDataManager initialized with empty table list" << std::endl;
        return false;
    }
    
    table_order_ = table_names;

    // 为每个表创建RowDataGenerator
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

std::optional<std::pair<std::string, std::vector<RowData>>> TableDataManager::next_batch() {
    TableState* table_state = get_next_active_table();
    if (!table_state) {
        return std::nullopt;
    }
    
    const std::string& table_name = table_order_[current_table_index_];
    std::vector<RowData> batch;
    
    // 计算本次需要生成的行数
    int64_t rows_to_generate = calculate_rows_to_generate(*table_state);
    
    // 生成数据
    for (int64_t i = 0; i < rows_to_generate; i++) {
        if (auto row = table_state->generator->next_row()) {
            batch.push_back(std::move(*row));
            table_state->rows_generated++;
            table_state->interlace_counter++;
        } else {
            // 该表没有更多数据了
            table_state->completed = true;
            break;
        }
    }
    
    // 更新状态
    if (table_state->interlace_counter >= interlace_rows_ || 
        table_state->completed || 
        table_state->rows_generated >= config_.control.data_generation.per_table_rows) {
        advance_to_next_table();
    }
    
    return std::make_pair(table_name, std::move(batch));
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
    
    // 循环查找有数据的表
    size_t start_index = current_table_index_;
    size_t attempts = 0;
    
    do {
        const std::string& table_name = table_order_[current_table_index_];
        auto it = table_states_.find(table_name);
        if (it != table_states_.end()) {
            TableState& state = it->second;
            
            // 检查表是否还有数据且未完成
            if (!state.completed && 
                state.rows_generated < config_.control.data_generation.per_table_rows && 
                state.generator->has_more()) {
                return &state;
            }
        }
        
        // 移动到下一个表
        current_table_index_ = (current_table_index_ + 1) % table_order_.size();
        attempts++;
        
    } while (attempts < table_order_.size());
    
    return nullptr;
}

int64_t TableDataManager::calculate_rows_to_generate(TableState& state) const {
    // 计算剩余行数限制
    int64_t remaining_rows = std::min(
        config_.control.data_generation.per_table_rows - state.rows_generated,
        state.generator->has_more() ? std::numeric_limits<int64_t>::max() : 0
    );
    
    // 计算本次应生成的行数
    int64_t rows_to_generate = std::min(
        interlace_rows_ - state.interlace_counter,
        remaining_rows
    );
    
    // 考虑流量控制
    // if (config_.control.data_generation.flow_control.rate_limit > 0) {
    //     rows_to_generate = std::min(
    //         rows_to_generate,
    //         config_.control.data_generation.flow_control.rate_limit / static_cast<int>(table_order_.size())
    //     );
    // }
    
    return std::max(1LL, rows_to_generate);
}

void TableDataManager::advance_to_next_table() {
    if (table_order_.empty()) return;
    
    // 重置当前表的交错计数器
    const std::string& current_table_name = table_order_[current_table_index_];
    if (auto it = table_states_.find(current_table_name); it != table_states_.end()) {
        it->second.interlace_counter = 0;
    }
    
    // 移动到下一个表
    current_table_index_ = (current_table_index_ + 1) % table_order_.size();
}