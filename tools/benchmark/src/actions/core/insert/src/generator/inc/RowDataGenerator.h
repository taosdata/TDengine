#pragma once

#include <vector>
#include <optional>
#include <memory>
#include <unordered_map>
#include <queue>
#include "InsertDataConfig.h"
#include "RowGenerator.h"
#include "TimestampGenerator.h"
#include "ColumnsCSV.h"
#include "TableNameCSV.h"


class RowDataGenerator {
public:
    RowDataGenerator(const std::string& table_name, 
                    const ColumnsConfig& columns_config,
                    const ColumnConfigInstanceVector& instances,
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
    // 延迟队列元素
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


    // 初始化缓存
    void init_cache();
    
    // 初始化乱序
    void init_disorder();

    // 初始化原始数据源
    void init_raw_source();

    // 应用乱序策略
    bool apply_disorder(RowData& row);
    
    // 处理延迟队列
    void process_delay_queue();
    
    // 从原始源获取一行数据
    std::optional<RowData> fetch_raw_row();

    // 初始化生成器组件
    void init_generator();
    
    // 初始化CSV读取器
    void init_csv_reader();
    
    // 从生成器获取数据
    RowData generate_from_generator();
    
    // 从CSV获取数据
    std::optional<RowData> generate_from_csv();
    
    const std::string& table_name_;
    const ColumnsConfig& columns_config_;
    const ColumnConfigInstanceVector& instances_;
    const InsertDataConfig::Control& control_;
    const std::string& target_precision_;

    // 数据源组件
    std::unique_ptr<RowGenerator> row_generator_;
    std::unique_ptr<ColumnsCSV> columns_csv_;
    std::unique_ptr<TimestampGenerator> timestamp_generator_;
    
    // CSV数据
    std::vector<RowData> csv_rows_;
    size_t csv_row_index_ = 0;
    std::string csv_precision_;
    
    // 状态管理
    int generated_rows_ = 0;
    int total_rows_ = 0;
    bool use_generator_ = false;

    // 乱序管理
    std::priority_queue<DelayedRow> delay_queue_;
    std::vector<RowData> cache_;

    std::vector<DisorderInterval> disorder_intervals_;
    
    // 时间戳状态
    int64_t current_timestamp_ = 0;
    // int64_t last_timestamp_ = 0;
    // int64_t timestamp_step_ = 1;
    // int64_t precision_factor_ = 1;
    // std::unordered_map<std::string, int64_t> last_timestamps_;
};