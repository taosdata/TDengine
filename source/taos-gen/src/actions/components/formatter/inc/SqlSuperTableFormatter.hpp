#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "taos.h"
#include <sstream>

class SqlSuperTableFormatter final : public ISuperTableFormatter {
public:
    explicit SqlSuperTableFormatter(const DataFormat& format) : format_(format) {}

    FormatResult format(const CreateSuperTableConfig& config) const override {
        std::ostringstream result;
        result << "CREATE TABLE IF NOT EXISTS `"
               << config.tdengine.database << "`.`"
               << config.schema.name << "` (ts TIMESTAMP";

        // columns
        auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
        if (!col_instances.empty()) {
            result << ", ";
            append_fields(result, col_instances, ", ");
        }
        result << ")";

        // tags
        auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());
        if (!tag_instances.empty()) {
            result << " TAGS (";
            append_fields(result, tag_instances, ", ");
            result << ")";
        }

        result << ";";
        return result.str();
    }


private:
    const DataFormat& format_;

    template <typename T>
    void append_fields(std::ostringstream& result, const std::vector<T>& fields, const std::string& separator) const {
        for (size_t i = 0; i < fields.size(); ++i) {
            if (i > 0) {
                result << separator;
            }
            result << generate_column_or_tag(fields[i]);
        }
    }

    std::string generate_column_or_tag(const ColumnConfigInstance& field) const {
        std::ostringstream oss;
        oss << "`" << field.name() << "` " << field.type();

        const auto& config = field.config();

        // if (field.is_var_length()) {
        //     oss << "(" << field.len.value() << ")";
        // } else if (field.type_tag == ColumnTypeTag::DECIMAL) {
        //     oss << "(" << field.precision.value_or(10) << "," << field.scale.value_or(0) << ")";
        // }

        if (config.primary_key) {
            oss << " PRIMARY KEY";
        }
        if (config.properties.has_value()) {
            oss << " " << config.properties.value();
        }

        return oss.str();
    }


    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<CreateSuperTableConfig>(
            "sql",
            [](const DataFormat& format) {
                return std::make_unique<SqlSuperTableFormatter>(format);
            });
        return true;
    }();
};