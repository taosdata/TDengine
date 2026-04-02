#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <variant>
#include <optional>
#include "TableData.hpp"
#include "ColumnsCSV.hpp"
#include "CSVReader.hpp"
#include "TimestampGenerator.hpp"
#include "TimestampCSVConfig.hpp"
#include "ColumnConfigInstance.hpp"

// Default table name used when tbname_index is -1 (no table name column in CSV)
constexpr const char* DEFAULT_TABLE_NAME = "default_table";

class ColumnsCSVReader {
public:
    ColumnsCSVReader(const ColumnsCSV& config, std::optional<ColumnConfigInstanceVector> instances);

    ColumnsCSVReader(const ColumnsCSVReader&) = delete;
    ColumnsCSVReader& operator=(const ColumnsCSVReader&) = delete;
    ColumnsCSVReader(ColumnsCSVReader&&) = delete;
    ColumnsCSVReader& operator=(ColumnsCSVReader&&) = delete;

    ~ColumnsCSVReader() = default;

    std::unordered_map<std::string, TableData> generate() const;

private:
    ColumnsCSV config_;
    std::optional<ColumnConfigInstanceVector> instances_;
    std::vector<std::string> resolved_paths_;
    size_t total_columns_ = 0;
    size_t actual_columns_ = 0;

    void validate_config();

    template <typename T>
    T convert_value(const std::string& value) const;

    ColumnType convert_to_type(const std::string& value, ColumnTypeTag target_type) const;
};
