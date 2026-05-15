#include "StreamingCSVRowSource.hpp"
#include "CsvNullUtils.hpp"
#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <chrono>

StreamingCSVRowSource::StreamingCSVRowSource(
    const std::vector<std::string>& file_paths,
    bool has_header,
    char delimiter,
    const ColumnConfigInstanceVector& instances,
    const TimestampStrategy& timestamp_strategy,
    const std::string& csv_precision,
    const std::string& target_precision,
    bool repeat_read)
    : file_paths_(file_paths),
      has_header_(has_header),
      delimiter_(delimiter),
      instances_(instances),
      timestamp_strategy_(timestamp_strategy),
      csv_precision_(csv_precision),
      target_precision_(target_precision),
      repeat_read_(repeat_read) {

    reader_ = std::make_unique<CSVReader>(file_paths_, has_header_, delimiter_);

    if (timestamp_strategy_.strategy_type == "generator") {
        timestamp_generator_ = std::make_unique<TimestampGenerator>(
            timestamp_strategy_.generator
        );
    }
}

std::optional<RowData> StreamingCSVRowSource::next() {
    if (exhausted_) return std::nullopt;

    auto raw_row = reader_->read_next();
    if (!raw_row) {
        if (repeat_read_) {
            reader_->reset();
            first_raw_ts_ = 0;
            first_raw_ts_set_ = false;
            // Note: do NOT reset timestamp_generator_ here.
            // In preload mode, repeat_read simply cycles csv_row_index_ via modulo
            // while timestamp_generator_ continues to advance monotonically.
            // Streaming mode must match this behavior.
            raw_row = reader_->read_next();
            if (!raw_row) {
                exhausted_ = true;
                return std::nullopt;
            }
        } else {
            exhausted_ = true;
            return std::nullopt;
        }
    }

    return convert_row(*raw_row);
}

bool StreamingCSVRowSource::has_more() const {
    return !exhausted_;
}

void StreamingCSVRowSource::reset() {
    reader_->reset();
    exhausted_ = false;
    first_raw_ts_ = 0;
    first_raw_ts_set_ = false;
    if (timestamp_generator_) {
        timestamp_generator_->reset();
    }
}

RowData StreamingCSVRowSource::convert_row(const CSVRow& raw_row) {
    RowData row;

    // Determine which columns to skip
    std::optional<size_t> timestamp_index;
    if (timestamp_strategy_.strategy_type == "csv") {
        timestamp_index = timestamp_strategy_.csv.timestamp_index;
    }

    // Handle timestamp
    if (timestamp_generator_) {
        row.timestamp = TimestampUtils::convert_timestamp_precision(
            timestamp_generator_->generate(),
            timestamp_generator_->timestamp_precision(),
            target_precision_);
    } else if (timestamp_index) {
        const auto& ts_config = timestamp_strategy_.csv;
        size_t ts_idx = *timestamp_index;
        if (ts_idx < raw_row.size()) {
            std::string ts_str = raw_row[ts_idx];
            StringUtils::trim(ts_str);
            int64_t raw_ts = TimestampUtils::parse_timestamp(ts_str, ts_config.timestamp_precision.value());

            if (ts_config.offset_config) {
                const auto& offset = *ts_config.offset_config;
                if (offset.offset_type == "absolute") {
                    if (!first_raw_ts_set_) {
                        first_raw_ts_ = raw_ts;
                        first_raw_ts_set_ = true;
                    }
                    row.timestamp = offset.absolute_value + (raw_ts - first_raw_ts_);
                } else if (offset.offset_type == "relative") {
                    int64_t multiplier = TimestampUtils::get_precision_multiplier(ts_config.timestamp_precision.value());
                    auto [years, months, days, hours, minutes, seconds] = offset.relative_offset;

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

                    row.timestamp = new_time * multiplier + fraction;
                } else {
                    throw std::runtime_error("Unsupported offset type: " + offset.offset_type);
                }
            } else {
                row.timestamp = raw_ts;
            }
        }
    }

    size_t actual_columns = raw_row.size();
    if (timestamp_index) {
        size_t ts_idx = *timestamp_index;
        if (ts_idx >= raw_row.size()) {
            throw std::runtime_error(
                "CSV timestamp index out of range: index=" + std::to_string(ts_idx)
                + ", row_columns=" + std::to_string(raw_row.size()));
        }
        actual_columns -= 1;
    }

    if (actual_columns != instances_.size()) {
        throw std::runtime_error(
            "CSV row column count mismatch: expected " + std::to_string(instances_.size())
            + ", got " + std::to_string(actual_columns)
            + " (excluding timestamp column)");
    }

    // Convert columns (skip timestamp column)
    row.columns.reserve(instances_.size());
    size_t instance_idx = 0;

    for (size_t col_idx = 0; col_idx < raw_row.size(); ++col_idx) {
        if (timestamp_index && col_idx == *timestamp_index) continue;
        if (instance_idx >= instances_.size()) break;

        const auto& instance = instances_[instance_idx];
        ColumnTypeTag type_tag = instance.config().type_tag;
        if ((ColumnTypeTraits::is_numeric(type_tag) || type_tag == ColumnTypeTag::BOOL)
            && CsvNullUtils::is_null_text(raw_row[col_idx])) {
            row.columns.push_back(NullValue{});
        } else {
            row.columns.push_back(TypeConverter::convert_to_type(raw_row[col_idx], type_tag));
        }
        instance_idx++;
    }

    return row;
}
