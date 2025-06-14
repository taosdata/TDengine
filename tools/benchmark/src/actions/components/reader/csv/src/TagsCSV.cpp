#include "TagsCSV.h"
#include <stdexcept>
#include <algorithm>
#include <sstream>
#include <cctype>
#include <charconv>
#include <cmath>
#include <locale>
#include <iomanip>
#include "ColumnType.h"
#include "CSVUtils.h"


TagsCSV::TagsCSV(const TagsConfig::CSV& config, std::optional<ColumnConfigInstanceVector> instances)
    : config_(config), instances_(instances) {
    validate_config();
}

void TagsCSV::validate_config() {
    if (config_.file_path.empty()) {
        throw std::invalid_argument("CSV file path is empty for tags data");
    }

    // Create a CSV reader to get total columns
    CSVReader reader(
        config_.file_path, 
        config_.has_header, 
        config_.delimiter.empty() ? ',' : config_.delimiter[0]
    );

    total_columns_ = reader.column_count();

    // Build valid indices
    std::vector<size_t> valid_indices_;
    valid_indices_.reserve(total_columns_);
    for (size_t i = 0; i < total_columns_; ++i) {
        if (std::find(config_.exclude_indices.begin(), 
                      config_.exclude_indices.end(), i) == config_.exclude_indices.end()) {
            valid_indices_.push_back(i);
        }
    }

    // Validate tag_types size if provided
    if (instances_ && instances_->size() != valid_indices_.size()) {
        std::stringstream ss;
        ss << "Tag types size (" << instances_->size() 
           << ") does not match number of valid columns (" << valid_indices_.size()
           << ") in file: " << config_.file_path;
        throw std::invalid_argument(ss.str());
    }

    // Initialize column_type_map_
    for (size_t i = 0; i < valid_indices_.size(); ++i) {
        ColumnTypeTag type = instances_ ? (*instances_)[i].config().type_tag : ColumnTypeTag::VARCHAR;
        column_type_map_.emplace_back(valid_indices_[i], type);
    }
}

template <typename T>
T TagsCSV::convert_value(const std::string& value) const {
    return CSVUtils::convert_value<T>(value);
}

ColumnType TagsCSV::convert_to_type(const std::string& value, ColumnTypeTag target_type) const {
    return CSVUtils::convert_to_type(value, target_type);
}

std::vector<ColumnTypeVector> TagsCSV::generate() const {
    try {
        // Create a CSV reader
        CSVReader reader(
            config_.file_path, 
            config_.has_header, 
            config_.delimiter.empty() ? ',' : config_.delimiter[0]
        );

        // Read all rows
        auto rows = reader.read_all();
    
        // Prepare result container
        std::vector<ColumnTypeVector> tags;
        tags.reserve(rows.size());

        // Process each row
        for (size_t row_idx = 0; row_idx < rows.size(); ++row_idx) {
            const auto& row = rows[row_idx];
            
            // Validate row has enough columns
            if (row.size() != total_columns_) {
                std::stringstream ss;
                ss << "Row " << (row_idx + 1) << " has only " << row.size() 
                   << " columns, expected " << total_columns_
                   << " in file: " << config_.file_path;
                throw std::out_of_range(ss.str());
            }
            
            // Prepare tag data for the current row
            ColumnTypeVector tag_row;
            tag_row.reserve(column_type_map_.size());
            
            // Process each valid column
            for (const auto& [col_idx, type] : column_type_map_) {
                tag_row.push_back(convert_to_type(row[col_idx], type));
            }

            tags.push_back(std::move(tag_row));
        }
        
        return tags;
        
    } catch (const std::exception& e) {
        std::stringstream ss;
        ss << "Failed to generate tags from CSV: " << config_.file_path 
           << " - " << e.what();
        throw std::runtime_error(ss.str());
    }
}