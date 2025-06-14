#pragma once
#include <sstream>
#include "taos.h"
#include "IFormatter.h"
#include "FormatterFactory.h"


class SqlInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit SqlInsertDataFormatter(const DataFormat& format) : format_(format) {}

    FormatResult format(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances, const MultiBatch& batch) const {
        std::ostringstream result;
        bool empty_batch = true;
    
        result << "INSERT INTO";
    
        // Iterate through each table's data batch
        for (const auto& [table_name, rows] : batch.table_batches) {
            if (rows.empty()) continue;

            empty_batch = false;
    
            // Write table name
            result << " `" << config.target.tdengine.database_info.name 
                   << "`.`" << table_name << "` VALUES ";

            // Write rows for this table
            for (const auto& row : rows) {
                result << "(" << row.timestamp;
    
                // Add column values
                for (size_t i = 0; i < row.columns.size(); ++i) {
                    result << ",";
                    
                    // Get column type
                    auto col_type = col_instances[i].type_tag();
    
                    // Check if type needs quotes
                    bool needs_quotes = (col_type == ColumnTypeTag::NCHAR ||
                                      col_type == ColumnTypeTag::VARCHAR ||
                                      col_type == ColumnTypeTag::BINARY ||
                                      col_type == ColumnTypeTag::JSON);
    
                    // Handle unsupported types
                    if (col_type == ColumnTypeTag::VARBINARY ||
                        col_type == ColumnTypeTag::GEOMETRY) {
                        throw std::runtime_error("Unsupported column type for SQL insert: " + col_instances[i].type());
                    }
    
                    if (needs_quotes) {
                        result << "'";
                        result << row.columns[i];
                        result << "'";
                    } else {
                        result << row.columns[i];
                    }
                }
    
                result << ")";
            }
        }
        
        result << ";";

        if (empty_batch)
            return FormatResult("");
        else
            return result.str();
    }

private:
    DataFormat format_;

    inline static bool registered_ = []() {
        FormatterFactory::instance().register_formatter<InsertDataConfig>(
            "sql",
            [](const DataFormat& format) {
                return std::make_unique<SqlInsertDataFormatter>(format);
            });
        return true;
    }();
};