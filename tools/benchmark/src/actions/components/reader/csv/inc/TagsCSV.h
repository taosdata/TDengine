#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include "ColumnType.h"
#include "TagsConfig.h"
#include "CSVReader.h"
#include "ColumnConfigInstance.h"

class TagsCSV {
public:
    TagsCSV(const TagsConfig::CSV& config, std::optional<ColumnConfigInstanceVector> instances);

    // Disable copy and move operations
    TagsCSV(const TagsCSV&) = delete;
    TagsCSV& operator=(const TagsCSV&) = delete;
    TagsCSV(TagsCSV&&) = delete;
    TagsCSV& operator=(TagsCSV&&) = delete;
    
    ~TagsCSV() = default;

    std::vector<ColumnTypeVector> generate() const;

private:
    TagsConfig::CSV config_;
    std::optional<ColumnConfigInstanceVector> instances_;
    size_t total_columns_ = 0;
    std::vector<std::pair<size_t, ColumnTypeTag>> column_type_map_;

    void validate_config();

    template <typename T>
    T convert_value(const std::string& value) const;

    ColumnType convert_to_type(const std::string& value, ColumnTypeTag target_type) const;
};