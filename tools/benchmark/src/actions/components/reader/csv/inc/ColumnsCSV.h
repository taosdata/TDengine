#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <variant>
#include <optional>
#include "TableData.h"
#include "ColumnsConfig.h"
#include "CSVReader.h"
#include "TimestampGenerator.h"
#include "TimestampOriginalConfig.h"
#include "ColumnConfigInstance.h"

class ColumnsCSV {
public:
    ColumnsCSV(const ColumnsConfig::CSV& config, std::optional<ColumnConfigInstanceVector> instances);
    
    ColumnsCSV(const ColumnsCSV&) = delete;
    ColumnsCSV& operator=(const ColumnsCSV&) = delete;
    ColumnsCSV(ColumnsCSV&&) = delete;
    ColumnsCSV& operator=(ColumnsCSV&&) = delete;
    
    ~ColumnsCSV() = default;
    
    std::vector<TableData> generate() const;

private:
    ColumnsConfig::CSV config_;
    std::optional<ColumnConfigInstanceVector> instances_;
    size_t total_columns_ = 0;
    size_t actual_columns_ = 0;
    
    void validate_config();

    template <typename T>
    T convert_value(const std::string& value) const;

    ColumnType convert_to_type(const std::string& value, ColumnTypeTag target_type) const;
};
