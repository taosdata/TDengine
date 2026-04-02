#pragma once

#include "ICSVRowSource.hpp"
#include "CSVReader.hpp"
#include "TypeConverter.hpp"
#include "TimestampUtils.hpp"
#include "ColumnConfigInstance.hpp"
#include "TimestampGenerator.hpp"
#include "TimestampStrategy.hpp"
#include <memory>
#include <string>
#include <vector>

class StreamingCSVRowSource : public ICSVRowSource {
public:
    StreamingCSVRowSource(const std::vector<std::string>& file_paths,
                          bool has_header,
                          char delimiter,
                          const ColumnConfigInstanceVector& instances,
                          const TimestampStrategy& timestamp_strategy,
                          const std::string& csv_precision,
                          const std::string& target_precision,
                          bool repeat_read);

    std::optional<RowData> next() override;
    bool has_more() const override;
    void reset() override;
    size_t total_rows() const override { return 0; }

private:
    RowData convert_row(const CSVRow& raw_row);

    std::unique_ptr<CSVReader> reader_;
    std::vector<std::string> file_paths_;
    bool has_header_;
    char delimiter_;
    const ColumnConfigInstanceVector& instances_;
    TimestampStrategy timestamp_strategy_;
    std::string csv_precision_;
    std::string target_precision_;
    bool repeat_read_;
    bool exhausted_ = false;

    // Timestamp generator (when strategy is "generator")
    std::unique_ptr<TimestampGenerator> timestamp_generator_;

    // For absolute offset tracking
    int64_t first_raw_ts_ = 0;
    bool first_raw_ts_set_ = false;
};
