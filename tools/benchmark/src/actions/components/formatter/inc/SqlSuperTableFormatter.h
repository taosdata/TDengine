#pragma once
#include <sstream>
#include "taos.h"
#include "IFormatter.h"
#include "FormatterFactory.h"



class SqlSuperTableFormatter final : public ISuperTableFormatter {
public:
    explicit SqlSuperTableFormatter(const DataFormat& format) : format_(format) {}


    FormatResult format(const CreateSuperTableConfig& config) const override {
        std::ostringstream result;
        result << "CREATE TABLE IF NOT EXISTS `" 
               << config.database_info.name << "`.`" 
               << config.super_table_info.name << "` (ts TIMESTAMP";
    
        // columns
        if (!config.super_table_info.columns.empty()) {
            result << ", ";
            append_fields(result, config.super_table_info.columns, ", ");
        }
        result << ")";
    
        // tags
        if (!config.super_table_info.tags.empty()) {
            result << " TAGS (";
            append_fields(result, config.super_table_info.tags, ", ");
            result << ")";
        }
    
        result << ";";
        return result.str();
    }


private:
    DataFormat format_;

    template <typename T>
    void append_fields(std::ostringstream& result, const std::vector<T>& fields, const std::string& separator) const {
        for (size_t i = 0; i < fields.size(); ++i) {
            if (i > 0) {
                result << separator;
            }
            result << generate_column_or_tag(fields[i]);
        }
    }
    

    std::string generate_column_or_tag(const ColumnConfig& field) const {
        std::ostringstream oss;
        oss << field.name << " " << field.type;
    
        if (field.is_var_length()) {
            oss << "(" << field.len.value() << ")";
        } else if (field.type_tag == ColumnTypeTag::DECIMAL) {
            oss << "(" << field.precision.value_or(10) << "," << field.scale.value_or(0) << ")";
        }
    
        if (field.primary_key) {
            oss << " PRIMARY KEY";
        }
        if (field.properties.has_value()) {
            oss << " " << field.properties.value();
        }
    
        return oss.str();
    }


    inline static bool registered_ = []() {
        FormatterFactory::instance().register_formatter<CreateSuperTableConfig>(
            "sql",
            [](const DataFormat& format) {
                return std::make_unique<SqlSuperTableFormatter>(format);
            });
        return true;
    }();
};