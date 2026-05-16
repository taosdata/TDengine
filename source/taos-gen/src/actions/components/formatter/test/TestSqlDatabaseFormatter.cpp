#include <iostream>
#include <cassert>
#include "FormatterRegistrar.hpp"


void test_format_drop_database() {
    DataFormat format;
    format.format_type = "sql";
    CreateDatabaseConfig config;
    config.tdengine.database = "test_db";
    config.tdengine.drop_if_exists = true;

    auto formatter = FormatterFactory::create_formatter<CreateDatabaseConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::vector<std::string>>(result));
    const auto& stmts = std::get<std::vector<std::string>>(result);
    (void)stmts;
    assert(stmts.size() == 2);
    assert(stmts[0] == "DROP DATABASE IF EXISTS `test_db`");
    std::cout << "test_format_drop_database passed!" << std::endl;
}

void test_format_create_database_without_properties() {
    DataFormat format;
    format.format_type = "sql";
    CreateDatabaseConfig config;
    config.tdengine.database = "test_db";
    config.tdengine.drop_if_exists = false;

    auto formatter = FormatterFactory::create_formatter<CreateDatabaseConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::vector<std::string>>(result));
    const auto& stmts = std::get<std::vector<std::string>>(result);
    (void)stmts;
    assert(stmts.size() == 1);
    assert(stmts[0] == "CREATE DATABASE IF NOT EXISTS `test_db`");
    std::cout << "test_format_create_database_without_properties passed!" << std::endl;
}

void test_format_create_database_with_properties() {
    DataFormat format;
    format.format_type = "sql";
    CreateDatabaseConfig config;
    config.tdengine.database = "test_db";
    config.tdengine.drop_if_exists = true;
    config.tdengine.properties = "KEEP 3650";

    auto formatter = FormatterFactory::create_formatter<CreateDatabaseConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::vector<std::string>>(result));
    const auto& stmts = std::get<std::vector<std::string>>(result);
    (void)stmts;
    assert(stmts.size() == 2);
    assert(stmts[0] == "DROP DATABASE IF EXISTS `test_db`");
    assert(stmts[1] == "CREATE DATABASE IF NOT EXISTS `test_db` KEEP 3650");
    std::cout << "test_format_create_database_with_properties passed!" << std::endl;
}

int main() {
    test_format_drop_database();
    test_format_create_database_without_properties();
    test_format_create_database_with_properties();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}