#pragma once

#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include <optional>
#include "RowDataGenerator.h"
#include "InsertDataConfig.h"

class TableDataManager {
public:
    struct TableState {
        std::unique_ptr<RowDataGenerator> generator;
        int64_t rows_generated = 0;         // 该表已生成的总行数
        int64_t interlace_counter = 0;      // 交错模式下当前已生成的行数
        bool completed = false;             // 该表是否已完成
    };
    
    explicit TableDataManager(const InsertDataConfig& config);
    
    // 初始化表数据管理器
    bool init(const std::vector<std::string>& table_names);
    
    // 获取下一批数据（一个表的多行数据）
    std::optional<std::pair<std::string, std::vector<RowData>>> next_batch();
    
    // 检查是否还有更多数据
    bool has_more() const;
    
    // 获取当前表名
    std::string current_table() const;
    
    // 获取表状态
    const std::unordered_map<std::string, TableState>& table_states() const;
    
private:
    const InsertDataConfig& config_;
    std::unordered_map<std::string, TableState> table_states_;
    std::vector<std::string> table_order_;      // 表名顺序
    size_t current_table_index_ = 0;            // 当前表索引
    int64_t interlace_rows_ = 1;                // 交错模式下每个表每次生成的行数
    
    // 获取下一个有数据的表
    TableState* get_next_active_table();
    
    // 计算当前表需要生成的行数
    int64_t calculate_rows_to_generate(TableState& state) const;
    
    // 切换到下一个表
    void advance_to_next_table();
};