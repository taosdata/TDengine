#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include "ColumnType.hpp"
#include "TagsCSV.hpp"
#include "CSVReader.hpp"
#include "ColumnConfigInstance.hpp"

class TagsCSVReader {
public:
    TagsCSVReader(const TagsCSV& config, std::optional<ColumnConfigInstanceVector> instances);

    // Disable copy and move operations
    TagsCSVReader(const TagsCSVReader&) = delete;
    TagsCSVReader& operator=(const TagsCSVReader&) = delete;
    TagsCSVReader(TagsCSVReader&&) = delete;
    TagsCSVReader& operator=(TagsCSVReader&&) = delete;

    ~TagsCSVReader() = default;

    std::vector<ColumnTypeVector> generate() const;

private:
    TagsCSV config_;
    std::optional<ColumnConfigInstanceVector> instances_;
    std::vector<std::string> resolved_paths_;
    size_t total_columns_ = 0;
    std::vector<std::pair<size_t, ColumnTypeTag>> column_type_map_;

    void validate_config();

    template <typename T>
    T convert_value(const std::string& value) const;

    ColumnType convert_to_type(const std::string& value, ColumnTypeTag target_type) const;
};