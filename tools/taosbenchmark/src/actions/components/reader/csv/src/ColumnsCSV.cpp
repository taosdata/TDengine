#include "ColumnsCSV.h"
#include <stdexcept>
#include <algorithm>
#include <sstream>
#include <cctype>
#include <charconv>
#include <cmath>
#include <locale>
#include <memory>
#include <string>
#include <string_view>
#include <iomanip>
#include <ctime>
#include <unordered_map>
#include "ColumnType.h"



// 时间单位乘数（毫秒）
constexpr int64_t YEAR_MS   = 365 * 24 * 60 * 60 * 1000LL;
constexpr int64_t MONTH_MS  = 30 * 24 * 60 * 60 * 1000LL;
constexpr int64_t DAY_MS    = 24 * 60 * 60 * 1000LL;
constexpr int64_t HOUR_MS   = 60 * 60 * 1000LL;
constexpr int64_t MINUTE_MS = 60 * 1000LL;
constexpr int64_t SECOND_MS = 1000LL;

ColumnsCSV::ColumnsCSV(const ColumnsConfig::CSV& config, std::optional<ColumnTypeVector> column_types)
    : config_(config), column_types_(column_types) {

    validate_config();
}

void ColumnsCSV::validate_config() {
    // 验证文件路径非空
    if (config_.file_path.empty()) {
        throw std::invalid_argument("CSV file path is empty for columns data");
    }
    
    // Create a CSV reader to get total columns
    CSVReader reader(
        config_.file_path, 
        config_.has_header, 
        config_.delimiter.empty() ? ',' : config_.delimiter[0]
    );

    const size_t total_columns = reader.column_count();

    // 验证 tbname_index
    const int tbname_index = config_.tbname_index;
    if (tbname_index >= 0 && static_cast<size_t>(tbname_index) >= total_columns) {
        std::stringstream ss;
        ss << "tbname_index (" << tbname_index << ") exceeds column count (" 
            << total_columns << ") in CSV file: " << config_.file_path;
        throw std::out_of_range(ss.str());
    }

    size_t actual_columns = total_columns;
    if (tbname_index >= 0) actual_columns--;

    // 验证时间戳策略配置
    if (std::holds_alternative<TimestampOriginalConfig>(config_.timestamp_strategy.timestamp_config)) {
        const auto& ts_config = std::get<TimestampOriginalConfig>(config_.timestamp_strategy.timestamp_config);
        
        // 验证时间戳索引有效
        if (ts_config.timestamp_index < 0) {
            throw std::invalid_argument("Timestamp column index must be non-negative");
        }

        if (ts_config.timestamp_index >= total_columns) {
            std::stringstream ss;
            ss << "timestamp_index (" << ts_config.timestamp_index
               << ") exceeds column count (" << total_columns
               << ") in CSV file: " << config_.file_path;
            throw std::out_of_range(ss.str());
        }

        actual_columns--;
    }

    total_columns_ = total_columns;
    actual_columns_ = actual_columns;

    // 验证列类型大小
    if (column_types_ && column_types_->size() != actual_columns) {
        std::stringstream ss;
        ss << "Column types size (" << column_types_->size()
           << ") does not match number of actual columns (" << actual_columns
           << ") in file: " << config_.file_path;
        throw std::invalid_argument(ss.str());
    }
}

ColumnType ColumnsCSV::convert_to_type(const std::string& value, const ColumnType& target_type) const {
    std::string trimmed = value;
    trim(trimmed);
    
    try {
        return std::visit([&](auto&& arg) -> ColumnType {
            using T = std::decay_t<decltype(arg)>;
            
            if constexpr (std::is_same_v<T, bool>) {
                std::string lower;
                std::transform(trimmed.begin(), trimmed.end(), std::back_inserter(lower), 
                              [](unsigned char c) { return std::tolower(c); });
                
                if (lower == "true" || lower == "1" || lower == "t") {
                    return true;
                }
                if (lower == "false" || lower == "0" || lower == "f") {
                    return false;
                }
                throw std::runtime_error("Invalid boolean value: " + trimmed);
            }
            else if constexpr (std::is_same_v<T, int8_t>) {
                return static_cast<int8_t>(std::stoi(trimmed));
            }
            else if constexpr (std::is_same_v<T, int16_t>) {
                return static_cast<int16_t>(std::stoi(trimmed));
            }
            else if constexpr (std::is_same_v<T, int32_t>) {
                return std::stoi(trimmed);
            }
            else if constexpr (std::is_same_v<T, int64_t>) {
                return std::stoll(trimmed);
            }
            else if constexpr (std::is_same_v<T, float>) {
                return std::stof(trimmed);
            }
            else if constexpr (std::is_same_v<T, double>) {
                return std::stod(trimmed);
            }
            else if constexpr (std::is_same_v<T, std::string>) {
                return trimmed;
            }
            else if constexpr (std::is_same_v<T, std::u16string>) {
                std::u16string utf16;
                for (char c : trimmed) {
                    utf16.push_back(static_cast<char16_t>(c));
                }
                return utf16;
            }
            else {
                return trimmed; // 默认返回字符串
            }
        }, target_type);
        
    } catch (const std::exception& e) {
        std::stringstream ss;
        ss << "Failed to convert value '" << value << "': " << e.what();
        throw std::runtime_error(ss.str());
    }
}

void ColumnsCSV::trim(std::string& str) {
    str.erase(str.begin(), std::find_if(str.begin(), str.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    
    str.erase(std::find_if(str.rbegin(), str.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), str.end());
}

std::vector<TableData> ColumnsCSV::generate_table_data() const {
    try {
        // 创建 CSV 读取器
        CSVReader reader(
            config_.file_path, 
            config_.has_header, 
            config_.delimiter.empty() ? ',' : config_.delimiter[0]
        );

        // 读取所有行
        auto rows = reader.read_all();

        // 时间戳策略相关变量
        std::optional<size_t> timestamp_index;
        const int tbname_index = config_.tbname_index;
        bool is_generator_mode = false;
        TimestampGeneratorConfig gen_config;
        TimestampOriginalConfig ts_config;

        std::unordered_map<std::string, std::unique_ptr<TimestampGenerator>> table_ts_generators;
        std::unordered_map<std::string, int64_t> table_first_raw_ts;

        if (std::holds_alternative<TimestampOriginalConfig>(config_.timestamp_strategy.timestamp_config)) {
            ts_config = std::get<TimestampOriginalConfig>(config_.timestamp_strategy.timestamp_config);
            timestamp_index = ts_config.timestamp_index;
        } else if (std::holds_alternative<TimestampGeneratorConfig>(config_.timestamp_strategy.timestamp_config)) {
            gen_config = std::get<TimestampGeneratorConfig>(config_.timestamp_strategy.timestamp_config);
            is_generator_mode = true;
        }

        // 准备结果容器
        std::vector<TableData> table_data;
        std::unordered_map<std::string, TableData> table_map;

        // 处理每一行
        for (size_t row_idx = 0; row_idx < rows.size(); ++row_idx) {
            const auto& row = rows[row_idx];
            
            // 验证行有足够列
            if (row.size() < total_columns_) {
                std::stringstream ss;
                ss << "Row " << (row_idx + 1) << " has only " << row.size() 
                   << " columns, expected " << total_columns_
                   << " in file: " << config_.file_path;
                throw std::out_of_range(ss.str());
            }
            
            // 获取表名
            std::string table_name = "default_table";
            if (tbname_index >= 0) {
                table_name = row[static_cast<size_t>(tbname_index)];
                trim(table_name);
            }
            
            // 获取或创建 TableData
            auto& data = table_map[table_name];
            if (data.table_name.empty()) {
                data.table_name = table_name;
            }

            // 处理时间戳
            int64_t timestamp = 0;
            
            if (timestamp_index) {
                // original模式
                const auto& raw_value = row[*timestamp_index];
                int64_t raw_ts = ts_config.parse_timestamp_value(raw_value, ts_config.precision);

                if (ts_config.offset_config) {
                    const auto& offset = *ts_config.offset_config;
                    if (offset.offset_type == "absolute") {
                        // 绝对模式
                        int64_t& first_raw_ts = table_first_raw_ts[table_name];
                        if (first_raw_ts == 0) {
                            first_raw_ts = raw_ts;
                        }
                        timestamp = offset.absolute_value + (raw_ts - first_raw_ts);
                    } else if (offset.offset_type == "relative") {
                        // 相对模式
                        auto [years, months, days, seconds] = offset.relative_offset;
                        timestamp = raw_ts +
                            years   * YEAR_MS +
                            months  * MONTH_MS +
                            days    * DAY_MS +
                            seconds * SECOND_MS;
                    } else {
                        throw std::runtime_error("Unsupported offset type: " + offset.offset_type);
                    }
                } else {
                    // 无offset
                    timestamp = raw_ts;
                }
            } else if (is_generator_mode) {
                // generator模式
                auto& gen_ptr = table_ts_generators[table_name];
                if (!gen_ptr) {
                    gen_ptr = TimestampGenerator::create(gen_config);
                }
                timestamp = gen_ptr->generate().value;
            }

            data.timestamps.push_back(timestamp);


            // 处理普通列
            std::vector<ColumnType> data_row;
            data_row.reserve(actual_columns_);

            size_t type_index = 0;

            for (size_t col_idx = 0; col_idx < total_columns_; ++col_idx) {
                // 跳过表名列和时间戳列
                if (static_cast<int>(col_idx) == tbname_index) continue;
                if (timestamp_index && col_idx == *timestamp_index) continue;
                
                // 转换值类型
                if (column_types_) {
                    // 使用提供的列类型
                    const ColumnType& target_type = (*column_types_)[type_index];
                    data_row.push_back(convert_to_type(row[col_idx], target_type));
                    type_index++;
                } else {
                    // 默认作为字符串处理
                    std::string val = row[col_idx];
                    trim(val);
                    data_row.push_back(val);
                }
            }
            
            data.data_rows.push_back(std::move(data_row));
        }

        // 转换为 std::vector
        table_data.reserve(table_map.size());
        for (auto& [_, data] : table_map) {
            table_data.push_back(std::move(data));
        }
        return table_data;

    } catch (const std::exception& e) {
        std::stringstream ss;
        ss << "Failed to generate table data from CSV: " << config_.file_path 
           << " - " << e.what();
        throw std::runtime_error(ss.str());
    }
}