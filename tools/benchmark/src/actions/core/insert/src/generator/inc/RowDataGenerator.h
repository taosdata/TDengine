#pragma once

#include <vector>
#include <optional>
#include <memory>
#include <unordered_map>
#include "InsertDataConfig.h"
#include "RowGenerator.h"
#include "TimestampGenerator.h"
#include "ColumnsCSV.h"
#include "TableNameCSV.h"


class RowDataGenerator {
public:
    RowDataGenerator(const std::string& table_name, 
                    const ColumnsConfig& columns_config,
                    const InsertDataConfig::Control& control,
                    const std::string& target_precision);
    
    // 获取下一行数据
    std::optional<RowData> next_row();
    
    // 检查是否还有更多数据
    bool has_more() const;
    
    // 获取已生成行数
    int generated_rows() const { return generated_rows_; }
    
    // 重置生成器状态
    void reset();

private:
    // 初始化生成器组件
    void init_generator();
    
    // 初始化CSV读取器
    void init_csv_reader();
    
    // 从生成器获取数据
    RowData generate_from_generator();
    
    // 从CSV获取数据
    std::optional<RowData> generate_from_csv();
    

    std::string table_name_;
    ColumnsConfig columns_config_;
    InsertDataConfig::Control control_;
    std::string target_precision_;

    // 数据源组件
    std::unique_ptr<RowGenerator> row_generator_;
    std::unique_ptr<ColumnsCSV> columns_csv_;
    std::unique_ptr<TimestampGenerator> timestamp_generator_;
    
    // CSV数据缓存
    std::vector<RowData> csv_rows_;
    size_t csv_row_index_ = 0;
    std::string csv_precision_;
    
    // 状态管理
    int generated_rows_ = 0;
    int total_rows_ = 0;
    bool use_generator_ = false;
    
    // 时间戳状态
    // int64_t last_timestamp_ = 0;
    // int64_t timestamp_step_ = 1;
};