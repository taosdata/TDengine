#include "RowDataGenerator.h"
#include <stdexcept>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include "ColumnGeneratorFactory.h"
#include "TimestampGenerator.h"
#include "StringUtils.h"


RowDataGenerator::RowDataGenerator(const std::string& table_name, 
                                  const ColumnsConfig& columns_config,
                                  const ColumnConfigInstanceVector& instances,
                                  const InsertDataConfig::Control& control,
                                  const std::string& target_precision)
    : table_name_(table_name), 
      columns_config_(columns_config),
      instances_(instances),
      control_(control),
      target_precision_(target_precision) {

    init_raw_source();

    if (control_.data_generation.data_cache.enabled) {
        init_cache();
    }

    if (control_.data_quality.data_disorder.enabled) {
        init_disorder();
    }
}

void RowDataGenerator::init_cache() {
    // 预生成数据填充缓存
    cache_.clear();
    cache_.reserve(control_.data_generation.data_cache.cache_size);
    while (cache_.size() < control_.data_generation.data_cache.cache_size && has_more()) {
        if (auto row = fetch_raw_row()) {
            cache_.push_back(*row);
        } else {
            break;
        }
    }
}

void RowDataGenerator::init_disorder() {
    // 初始化乱序区间
    disorder_intervals_.clear();
    for (const auto& interval : control_.data_quality.data_disorder.intervals) {
        DisorderInterval disorder_interval;
        disorder_interval.start_time = TimestampUtils::parse_timestamp(interval.time_start, target_precision_);
        disorder_interval.end_time = TimestampUtils::parse_timestamp(interval.time_end, target_precision_);
        disorder_interval.ratio = interval.ratio;
        disorder_interval.latency_range = interval.latency_range;
        disorder_intervals_.push_back(disorder_interval);
    }
}

void RowDataGenerator::init_raw_source() {
    if (columns_config_.source_type == "generator") {
        init_generator();
        total_rows_ = control_.data_generation.per_table_rows;
    } else if (columns_config_.source_type == "csv") {
        init_csv_reader();
        total_rows_ = csv_rows_.size();
    } else {
        throw std::invalid_argument("Unsupported source_type: " + columns_config_.source_type);
    }
    
    // 初始化时间戳生成器
    if (columns_config_.source_type == "generator") {
        timestamp_generator_ = std::make_unique<TimestampGenerator>(
            columns_config_.generator.timestamp_strategy.timestamp_config
        );
    } else if (columns_config_.source_type == "csv" && columns_config_.csv.timestamp_strategy.strategy_type == "generator") {
        timestamp_generator_ = std::make_unique<TimestampGenerator>(
            std::get<TimestampGeneratorConfig>(columns_config_.csv.timestamp_strategy.timestamp_config)
        );
    }
}

void RowDataGenerator::init_generator() {
    use_generator_ = true;
    
    // 创建行生成器  
    row_generator_ = std::make_unique<RowGenerator>(instances_);
}

void RowDataGenerator::init_csv_reader() {
    use_generator_ = false;

    csv_precision_ = columns_config_.csv.timestamp_strategy.get_precision();

    // 创建ColumnsCSV读取器
    columns_csv_ = std::make_unique<ColumnsCSV>(columns_config_.csv, instances_);

    // TODO: ColumnsCSV 需要支持表名索引接口
    // 获取所有表数据
    std::vector<TableData> all_tables = columns_csv_->generate();
    
    // 查找当前表的数据
    bool found = false;
    for (const auto& table_data : all_tables) {
        if (table_data.table_name == table_name_) {
            found = true;

            for (size_t i = 0; i < table_data.rows.size(); i++) {
                RowData row;
                // row.table_name = table_name_;
                row.timestamp = TimestampUtils::convert_timestamp_precision(table_data.timestamps[i], csv_precision_, target_precision_);
                row.columns = table_data.rows[i];
                csv_rows_.push_back(row);
            }
            break;
        }
    }
    
    if (!found) {
        throw std::runtime_error("Table '" + table_name_ + "' not found in CSV file");
    }
}


std::optional<RowData> RowDataGenerator::next_row() {
    if (generated_rows_ >= total_rows_) {
        return std::nullopt;
    }

    // 处理延迟队列
    process_delay_queue();
    
    // 优先从缓存中取数据
    if (!cache_.empty()) {
        auto row = cache_.back();
        cache_.pop_back();
        generated_rows_++;
        return row;
    }
    
    // 从原始源获取数据
    auto row_opt = fetch_raw_row();
    if (!row_opt) {
        return std::nullopt;
    }
    
    // 更新当前时间轴
    current_timestamp_ = row_opt->timestamp;
    
    // 应用乱序策略
    auto delay = apply_disorder(*row_opt);
    if (!delay) {
        generated_rows_++;
    }
    
    return row_opt;
}

bool RowDataGenerator::apply_disorder(RowData& row) {
    if (!control_.data_quality.data_disorder.enabled) {
        return false;
    }
    
    // 检查数据时间戳是否在乱序区间
    for (const auto& interval : disorder_intervals_) {
        if (row.timestamp >= interval.start_time && row.timestamp < interval.end_time) {
            // 按概率决定是否延迟
            if (static_cast<double>(rand()) / RAND_MAX < interval.ratio) {
                int latency = rand() % interval.latency_range;
                int64_t deliver_time = row.timestamp + latency;
                
                // 放入延迟队列
                delay_queue_.push(DelayedRow{deliver_time, row});
                row.timestamp = -1;
                return true;
            }
        }
    }
    return false;
}

void RowDataGenerator::process_delay_queue() {
    while (!delay_queue_.empty() && delay_queue_.top().deliver_timestamp <= current_timestamp_) {
        cache_.push_back(delay_queue_.top().row);
        delay_queue_.pop();
    }
}

std::optional<RowData> RowDataGenerator::fetch_raw_row() {
    RowData row_data;
    
    try {
        if (use_generator_) {
            row_data = generate_from_generator();
        } else {
            if (auto csv_row = generate_from_csv()) {
                row_data = std::move(*csv_row);
            } else {
                total_rows_ = generated_rows_;
                return std::nullopt;
            }
        }

        return row_data;
    } catch (const std::exception& e) {
        std::cerr << "Error generating row: " << e.what() << std::endl;
        return std::nullopt;
    }
}

bool RowDataGenerator::has_more() const {
    return generated_rows_ < total_rows_;
}

void RowDataGenerator::reset() {
    generated_rows_ = 0;
    csv_row_index_ = 0;
    
    if (timestamp_generator_) {
        timestamp_generator_->reset();
    }
}

RowData RowDataGenerator::generate_from_generator() {
    RowData row_data;
    // row_data.table_name = table_name_;
    
    // 生成时间戳
    row_data.timestamp = timestamp_generator_->generate();
    
    // 生成列数据
    row_data.columns = row_generator_->generate();
    
    return row_data;
}

std::optional<RowData> RowDataGenerator::generate_from_csv() {
    return csv_rows_[csv_row_index_++];;
}
