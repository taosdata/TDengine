#pragma once
#include "IFormatter.hpp"
#include "FormatterFactory.hpp"


class SqlDatabaseFormatter final : public IDatabaseFormatter {
public:
    explicit SqlDatabaseFormatter(const DataFormat& format) : format_(format) {}

    FormatResult format(const CreateDatabaseConfig& config) const override {
        std::vector<std::string> stmts;
        if (config.tdengine.drop_if_exists) {
            stmts.push_back("DROP DATABASE IF EXISTS `" + config.tdengine.database + "`");
        }
        std::string create_stmt = "CREATE DATABASE IF NOT EXISTS `" + config.tdengine.database + "`";
        if (config.tdengine.properties.has_value()) {
            create_stmt += " " + config.tdengine.properties.value();
        }
        stmts.push_back(std::move(create_stmt));
        return stmts;
    }


private:
    const DataFormat& format_;

    // Register SqlDatabaseFormatter to FormatterFactory
    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<CreateDatabaseConfig>(
            "sql",
            [](const DataFormat& format) {
                return std::make_unique<SqlDatabaseFormatter>(format);
            });
        return true;
    }();
};