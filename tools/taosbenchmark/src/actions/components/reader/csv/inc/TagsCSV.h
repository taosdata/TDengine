#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include "ColumnType.h"
#include "TagsConfig.h"
#include "CSVReader.h"

class TagsCSV {
public:
    // Constructor - requires the schema definition of tag columns
    TagsCSV(const TagsConfig::CSV& config, std::optional<ColumnTypeVector> tag_types);

    // Disable copy and move operations
    TagsCSV(const TagsCSV&) = delete;
    TagsCSV& operator=(const TagsCSV&) = delete;
    TagsCSV(TagsCSV&&) = delete;
    TagsCSV& operator=(TagsCSV&&) = delete;
    
    ~TagsCSV() = default;

    std::vector<ColumnTypeVector> generate_tags() const;

private:
    TagsConfig::CSV config_;
    std::optional<ColumnTypeVector> tag_types_;
    size_t total_columns_ = 0;
    std::vector<std::pair<size_t, ColumnType>> column_type_map_;

    void validate_config();

    template <typename T>
    T convert_to_type(const std::string& value) const;

    static void trim(std::string& str);
};