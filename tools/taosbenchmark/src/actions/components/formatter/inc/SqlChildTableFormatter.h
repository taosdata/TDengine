#pragma once
#include <sstream>
#include "taos.h"
#include "IFormatter.h"
#include "FormatterFactory.h"


class SqlChildTableFormatter final : public IChildTableFormatter {
public:
    explicit SqlChildTableFormatter(const DataFormat& format) : format_(format) {}


    std::string format(const CreateChildTableConfig& config, std::string table_name, RowType tags) const {
        std::ostringstream result;
        result << "CREATE TABLE IF NOT EXISTS `" 
               << config.database_info.name << "`.`"  << table_name << "` USING `"
               << config.database_info.name << "`.`"  << config.super_table_info.name << "` TAGS (";

        for (size_t i = 0; i < tags.size(); ++i) {
            if (i > 0) {
                result << ", ";
            }
            std::visit([&result](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::u16string>) {
                    result << "'" << value << "'";
                } else {
                    result << value;
                }
            }, tags[i]);
        }
    
        result << ");";

        return result.str();
    }

    FormatResult format(const CreateChildTableConfig& config, std::vector<std::string> table_names, std::vector<RowType> tags) const override {
        std::ostringstream result;
        for (size_t i = 0; i < table_names.size(); ++i) {
            if (i > 0) {
                result << "\n";
            }
            result << format(config, table_names[i], tags[i]);
        }
    

        return result.str();
    }


private:
    DataFormat format_;

    inline static bool registered_ = []() {
        FormatterFactory::instance().register_formatter<CreateChildTableConfig>(
            "sql",
            [](const DataFormat& format) {
                return std::make_unique<SqlChildTableFormatter>(format);
            });
        return true;
    }();
};