#include "ColumnsCSVReader.hpp"
#include "CsvNullUtils.hpp"
#include "FilePathResolver.hpp"
#include "LogUtils.hpp"
#include "TypeConverter.hpp"
#include "TimestampUtils.hpp"
#include <stdexcept>
#include <algorithm>
#include <sstream>
#include <cctype>
#include <cmath>
#include <locale>
#include <memory>
#include <string>
#include <iomanip>
#include <unordered_map>
#include <cctz/civil_time.h>
#include <cctz/time_zone.h>


ColumnsCSVReader::ColumnsCSVReader(const ColumnsCSV& config, std::optional<ColumnConfigInstanceVector> instances)
    : config_(config), instances_(instances) {

    validate_config();
}

void ColumnsCSVReader::validate_config() {
    // Validate file path is not empty
    if (config_.file_path.empty()) {
        throw std::invalid_argument("CSV file path is empty for columns data");
    }

    // Resolve file paths (supports single file, directory, glob)
    resolved_paths_ = FilePathResolver::resolve(config_.file_path);

    // Create a CSV reader to get total columns
    CSVReader reader(
        resolved_paths_,
        config_.has_header,
        config_.delimiter.empty() ? ',' : config_.delimiter[0]
    );

    const size_t total_columns = reader.column_count();

    // Validate tbname_index
    const int tbname_index = config_.tbname_index;
    if (tbname_index >= 0 && static_cast<size_t>(tbname_index) >= total_columns) {
        std::stringstream ss;
        ss << "tbname_index (" << tbname_index << ") exceeds column count ("
            << total_columns << ") in CSV file: " << config_.file_path;
        throw std::out_of_range(ss.str());
    }

    size_t actual_columns = total_columns;
    if (tbname_index >= 0) actual_columns--;

    // Validate timestamp strategy config
    if (config_.timestamp_strategy.strategy_type == "csv" ) {
        const auto& ts_config = config_.timestamp_strategy.csv;

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

    // Validate column type size
    if (instances_ && instances_->size() != actual_columns) {
        std::stringstream ss;
        ss << "Column types size (" << instances_->size()
           << ") does not match number of actual columns (" << actual_columns
           << ") in file: " << config_.file_path;
        throw std::invalid_argument(ss.str());
    }
}

template <typename T>
T ColumnsCSVReader::convert_value(const std::string& value) const {
    return TypeConverter::convert_value<T>(value);
}

ColumnType ColumnsCSVReader::convert_to_type(const std::string& value, ColumnTypeTag target_type) const {
    return TypeConverter::convert_to_type(value, target_type);
}

std::unordered_map<std::string, TableData> ColumnsCSVReader::generate() const {
    try {
        // Use resolved file paths from validate_config()
        CSVReader reader(
            resolved_paths_,
            config_.has_header,
            config_.delimiter.empty() ? ',' : config_.delimiter[0]
        );

        // Read all rows
        auto rows = reader.read_all();

        // Timestamp strategy related variables
        std::optional<size_t> timestamp_index;
        const int tbname_index = config_.tbname_index;
        bool is_generator_mode = false;
        TimestampGeneratorConfig gen_config;
        TimestampCSVConfig ts_config;

        std::unordered_map<std::string, std::unique_ptr<TimestampGenerator>> table_ts_generators;
        std::unordered_map<std::string, int64_t> table_first_raw_ts;

        if (config_.timestamp_strategy.strategy_type == "csv") {
            ts_config = config_.timestamp_strategy.csv;
            timestamp_index = ts_config.timestamp_index;
        } else if (config_.timestamp_strategy.strategy_type == "generator") {
            gen_config = config_.timestamp_strategy.generator;
            is_generator_mode = true;
        }

        // Prepare result container
        std::vector<TableData> table_data;
        std::unordered_map<std::string, TableData> table_map;

        // Process each row
        for (size_t row_idx = 0; row_idx < rows.size(); ++row_idx) {
            const auto& row = rows[row_idx];

            try {
                // Validate row has enough columns
                if (row.size() < total_columns_) {
                    std::stringstream ss;
                    ss << "Row " << (row_idx + 1) << " has only " << row.size()
                    << " columns, expected " << total_columns_
                    << " in file: " << config_.file_path;
                    throw std::out_of_range(ss.str());
                }

                // Get table name
                std::string table_name = DEFAULT_TABLE_NAME;
                if (tbname_index >= 0) {
                    table_name = row[static_cast<size_t>(tbname_index)];
                    StringUtils::trim(table_name);
                }

                // Get or create TableData
                auto& data = table_map[table_name];
                if (data.table_name.empty()) {
                    data.table_name = table_name;
                }

                // Handle timestamp
                int64_t timestamp = 0;

                if (timestamp_index) {
                    // original mode
                    const auto& raw_value = row[*timestamp_index];
                    int64_t raw_ts = TimestampUtils::parse_timestamp(raw_value, ts_config.timestamp_precision.value());

                    if (ts_config.offset_config) {
                        const auto& offset = *ts_config.offset_config;
                        if (offset.offset_type == "absolute") {
                            // absolute mode
                            int64_t& first_raw_ts = table_first_raw_ts[table_name];
                            if (first_raw_ts == 0) {
                                first_raw_ts = raw_ts;
                            }
                            timestamp = offset.absolute_value + (raw_ts - first_raw_ts);
                        } else if (offset.offset_type == "relative") {
                            // relative mode - using cctz for thread-safe time operations
                            int64_t multiplier = TimestampUtils::get_precision_multiplier(ts_config.timestamp_precision.value());
                            auto [years, months, days, hours, minutes, seconds] = offset.relative_offset;

                            // Convert timestamp to seconds
                            int64_t raw_seconds = raw_ts / multiplier;
                            int64_t fraction = raw_ts % multiplier;

                            // Use cctz for thread-safe local time conversion
                            static const cctz::time_zone local_tz = cctz::local_time_zone();
                            auto tp = std::chrono::system_clock::from_time_t(raw_seconds);
                            auto cs = cctz::convert(tp, local_tz);

                            // Apply relative offset in civil time
                            cctz::civil_second adjusted = cctz::civil_second(
                                cs.year() + years,
                                cs.month() + months,
                                cs.day() + days,
                                cs.hour() + hours,
                                cs.minute() + minutes,
                                cs.second() + seconds);

                            // Convert back to time_point
                            auto new_tp = cctz::convert(adjusted, local_tz);
                            auto new_time = std::chrono::system_clock::to_time_t(new_tp);

                            timestamp = new_time * multiplier + fraction;
                        } else {
                            throw std::runtime_error("Unsupported offset type: " + offset.offset_type);
                        }
                    } else {
                        // No offset
                        timestamp = raw_ts;
                    }
                } else if (is_generator_mode) {
                    // generator mode
                    auto& gen_ptr = table_ts_generators[table_name];
                    if (!gen_ptr) {
                        gen_ptr = TimestampGenerator::create(gen_config);
                    }
                    timestamp = gen_ptr->generate();
                }

                data.timestamps.push_back(timestamp);


                // Handle normal columns
                RowType data_row;
                data_row.reserve(actual_columns_);

                size_t index = 0;

                for (size_t col_idx = 0; col_idx < total_columns_; ++col_idx) {
                    // Skip table name and timestamp columns
                    if (static_cast<int>(col_idx) == tbname_index) continue;
                    if (timestamp_index && col_idx == *timestamp_index) continue;

                    // Convert value type
                    if (instances_) {
                        // Use provided column types
                        const ColumnConfigInstance& instance = (*instances_)[index];
                        ColumnTypeTag type_tag = instance.config().type_tag;
                        if ((ColumnTypeTraits::is_numeric(type_tag) || type_tag == ColumnTypeTag::BOOL)
                            && CsvNullUtils::is_null_text(row[col_idx])) {
                            data_row.push_back(NullValue{});
                        } else {
                            data_row.push_back(convert_to_type(row[col_idx], type_tag));
                        }
                        index++;
                    } else {
                        // Default as string
                        std::string val = row[col_idx];
                        StringUtils::trim(val);
                        data_row.push_back(val);
                    }
                }

                data.rows.push_back(std::move(data_row));
            } catch (const std::exception& e) {
                LogUtils::error("Failed to process row {} in file {}: Reason: {}. Row content: [{}]",
                    row_idx + 1, config_.file_path, e.what(), fmt::join(row, ","));
                throw;
            }
        }

        return table_map;

    } catch (const std::exception& e) {
        LogUtils::error("Failed to generate table data from CSV {}: {}", config_.file_path, e.what());
        throw;
    }
}