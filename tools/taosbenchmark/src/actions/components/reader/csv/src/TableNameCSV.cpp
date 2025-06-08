#include "TableNameCSV.h"
#include <stdexcept>
#include <algorithm>
#include <sstream>
#include "StringUtils.h"


TableNameCSV::TableNameCSV(const TableNameConfig::CSV& config) : config_(config) {
    validate_config();
}

void TableNameCSV::validate_config() const {
    if (config_.file_path.empty()) {
        throw std::invalid_argument("CSV file path is empty for table names");
    }
    
    if (config_.tbname_index < 0) {
        throw std::invalid_argument("tbname_index must be non-negative");
    }
}

std::vector<std::string> TableNameCSV::generate() const {
    try {
        // Create a CSV reader
        CSVReader reader(
            config_.file_path, 
            config_.has_header, 
            config_.delimiter.empty() ? ',' : config_.delimiter[0]
        );
        
        // Read all rows
        auto rows = reader.read_all();
        
        // Get the number of columns
        const size_t column_count = reader.column_count();
        const size_t name_index = static_cast<size_t>(config_.tbname_index);
        
        // Validate that the column index is within bounds
        if (name_index >= column_count) {
            std::stringstream ss;
            ss << "tbname_index (" << name_index << ") exceeds column count (" 
               << column_count << ") in CSV file: " << config_.file_path;
            throw std::out_of_range(ss.str());
        }
        
        // Extract table names
        std::vector<std::string> names;
        names.reserve(rows.size());
        
        for (size_t i = 0; i < rows.size(); ++i) {
            const auto& row = rows[i];
            
            // Validate that the row has enough columns
            if (row.size() <= name_index) {
                std::stringstream ss;
                ss << "Row " << (i + 1) << " has only " << row.size() 
                   << " columns, but tbname_index is " << name_index
                   << " in file: " << config_.file_path;
                throw std::out_of_range(ss.str());
            }
            
            // Add table name
            std::string name = row[name_index];
            StringUtils::trim(name);
            names.push_back(std::move(name));
        }
        
        // Validate that no table name is empty
        if (std::any_of(names.begin(), names.end(), [](const std::string& name) {
            return name.empty();
        })) {
            throw std::runtime_error("Found empty table name in CSV file: " + config_.file_path);
        }
        
        return names;
    } catch (const std::out_of_range& e) {
        throw;
    } catch (const std::exception& e) {
        std::stringstream ss;
        ss << "Failed to generate table names from CSV: " << config_.file_path 
           << " - " << e.what();
        throw std::runtime_error(ss.str());
    }
}