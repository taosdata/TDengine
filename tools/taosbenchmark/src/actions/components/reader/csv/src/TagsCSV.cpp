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


TagsCSV::TagsCSV(const TagsConfig::CSV& config, std::optional<ColumnTypeVector> tag_types)
    : config_(config) {
    validate_config();

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
    if (tag_types && tag_types->size() != valid_indices_.size()) {
        std::stringstream ss;
        ss << "Tag types size (" << tag_types->size() 
           << ") does not match number of valid columns (" << valid_indices_.size()
           << ") in file: " << config_.file_path;
        throw std::invalid_argument(ss.str());
    }

    // Initialize column_type_map_
    for (size_t i = 0; i < valid_indices_.size(); ++i) {
        ColumnType type = tag_types ? (*tag_types)[i] : std::string{};
        column_type_map_.emplace_back(valid_indices_[i], type);
    }
}

void TagsCSV::validate_config() const {
    if (config_.file_path.empty()) {
        throw std::invalid_argument("CSV file path is empty for tags data");
    }
}

void TagsCSV::trim(std::string& str) {
    str.erase(str.begin(), std::find_if(str.begin(), str.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));

    str.erase(std::find_if(str.rbegin(), str.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), str.end());
}

template <typename T>
T TagsCSV::convert_to_type(const std::string& value) const {
    std::string trimmed = value;
    trim(trimmed);

    try {
        // Convert based on type
        if constexpr (std::is_same_v<T, bool>) {
            std::string lower;
            std::transform(trimmed.begin(), trimmed.end(), std::back_inserter(lower),
                           [](unsigned char c) { return std::tolower(c); });

            if (lower == "true" || lower == "1" || lower == "t") {
                return true;
            }
            if (lower == "false" || lower == "0" || lower == "f") {
                return false;
            }
            throw std::runtime_error("Invalid boolean value: " + trimmed);
        } else if constexpr (std::is_integral_v<T>) {
            T val;
            auto result = std::from_chars(trimmed.data(), trimmed.data() + trimmed.size(), val);
            if (result.ec == std::errc()) {
                return val;
            }
            throw std::runtime_error("Invalid integer value: " + trimmed);
        } else if constexpr (std::is_floating_point_v<T>) {
            T val;
            auto result = std::from_chars(trimmed.data(), trimmed.data() + trimmed.size(), val);
            if (result.ec == std::errc()) {
                return val;
            }
            throw std::runtime_error("Invalid floating-point value: " + trimmed);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return trimmed; // Directly return the string
        } else if constexpr (std::is_same_v<T, std::u16string>) {
            // UTF-8 to UTF-16 (simplified example, actual implementation needed)
            std::u16string utf16;
            for (char c : trimmed) {
                utf16.push_back(static_cast<char16_t>(c)); // Simplified handling
            }
            return utf16;
        } else {
            throw std::runtime_error("Unsupported type");
        }
    } catch (const std::exception& e) {
        std::stringstream ss;
        ss << "Failed to convert value '" << value << "' to type: " << e.what();
        throw std::runtime_error(ss.str());
    }
}

std::vector<ColumnTypeVector> TagsCSV::generate_tags() const {
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
        std::vector<std::vector<ColumnType>> tags;
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
            std::vector<ColumnType> tag_row;
            tag_row.reserve(column_type_map_.size());
            
            // Process each valid column
            for (const auto& [col_idx, type] : column_type_map_) {
                tag_row.push_back(
                    std::visit(
                        [&](auto&& type) -> ColumnType {
                            using T = std::decay_t<decltype(type)>;
                            return convert_to_type<T>(row[col_idx]);
                        },
                        type
                    )
                );
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