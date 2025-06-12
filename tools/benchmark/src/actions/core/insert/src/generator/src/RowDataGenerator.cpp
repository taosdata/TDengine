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
                                  const InsertDataConfig::Control& control,
                                  const std::string& target_precision)
    : table_name_(table_name), 
      columns_config_(columns_config),
      control_(control),
      target_precision_(target_precision) {

    if (columns_config.source_type == "generator") {
        init_generator();
        total_rows_ = control.data_generation.per_table_rows;
    } else if (columns_config.source_type == "csv") {
        init_csv_reader();
        total_rows_ = csv_rows_.size();
    } else {
        throw std::invalid_argument("Unsupported source_type: " + columns_config.source_type);
    }
    
    // 初始化时间戳生成器
    if (columns_config.source_type == "generator") {
        timestamp_generator_ = std::make_unique<TimestampGenerator>(
            columns_config.generator.timestamp_strategy.timestamp_config
        );
    } else if (columns_config.source_type == "csv" && columns_config.csv.timestamp_strategy.strategy_type == "generator") {
        timestamp_generator_ = std::make_unique<TimestampGenerator>(
            std::get<TimestampGeneratorConfig>(columns_config.csv.timestamp_strategy.timestamp_config)
        );
    }
}

void RowDataGenerator::init_generator() {
    use_generator_ = true;
    
    // 创建行生成器
    auto col_instances = ColumnConfigInstanceFactory::create(columns_config_.generator.schema);    

    row_generator_ = std::make_unique<RowGenerator>(
        col_instances
    );
    
    // 设置时间戳步长
    // timestamp_step_ = columns_config_.generator.timestamp_strategy.timestamp_config.timestamp_step;
}

void RowDataGenerator::init_csv_reader() {
    use_generator_ = false;

    csv_precision_ = columns_config_.csv.timestamp_strategy.get_precision();

    // 创建ColumnsCSV读取器
    auto col_instances = ColumnConfigInstanceFactory::create(columns_config_.csv.schema); 

    columns_csv_ = std::make_unique<ColumnsCSV>(columns_config_.csv, col_instances);

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
                row.table_name = table_name_;
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
    
    RowData row_data;
    row_data.table_name = table_name_;
    
    try {
        if (use_generator_) {
            row_data = generate_from_generator();
        } else {
            if (auto csv_row = generate_from_csv()) {
                row_data = *csv_row;
            } else {
                total_rows_ = generated_rows_;
                return std::nullopt;
            }
        }
        
        generated_rows_++;
        return row_data;
    } catch (const std::exception& e) {
        // 记录错误并返回空值
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
    row_data.table_name = table_name_;
    
    // 生成时间戳
    row_data.timestamp = timestamp_generator_->generate();
    
    // 生成列数据
    row_data.columns = row_generator_->generate();
    
    return row_data;
}

std::optional<RowData> RowDataGenerator::generate_from_csv() {
    return csv_rows_[csv_row_index_++];;
}
