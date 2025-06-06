#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <variant>
#include <optional>
#include "ColumnType.h"
#include "ColumnsConfig.h"
#include "CSVReader.h"
#include "TimestampGenerator.h"
#include "TimestampOriginalConfig.h"


struct TableData {
    std::string table_name;
    std::vector<int64_t> timestamps;
    std::vector<std::vector<ColumnType>> data_rows;
};

class ColumnsCSV {
public:
    ColumnsCSV(const ColumnsConfig::CSV& config, std::optional<ColumnTypeVector> column_types);
    
    ColumnsCSV(const ColumnsCSV&) = delete;
    ColumnsCSV& operator=(const ColumnsCSV&) = delete;
    ColumnsCSV(ColumnsCSV&&) = delete;
    ColumnsCSV& operator=(ColumnsCSV&&) = delete;
    
    ~ColumnsCSV() = default;
    
    std::vector<TableData> generate_table_data() const;

private:
    ColumnsConfig::CSV config_;
    std::optional<ColumnTypeVector> column_types_;
    size_t total_columns_ = 0;
    size_t actual_columns_ = 0;
    
    void validate_config();

    ColumnType convert_to_type(const std::string& value, const ColumnType& target_type) const;

    static void trim(std::string& str);
};
