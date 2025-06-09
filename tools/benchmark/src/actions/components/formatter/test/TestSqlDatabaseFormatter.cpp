#include <iostream>
#include <cassert>
#include "FormatterRegistrar.h"


void test_format_drop_database() {
    DataFormat format;
    format.format_type = "sql";
    CreateDatabaseConfig config;
    config.database_info.name = "test_db";
    config.database_info.drop_if_exists = true;

    auto formatter = FormatterFactory::instance().create_formatter<CreateDatabaseConfig>(format);
    FormatResult result = formatter->format(config, true);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == "DROP DATABASE IF EXISTS `test_db`");
    std::cout << "test_format_drop_database passed!" << std::endl;
}

void test_format_create_database_without_properties() {
    DataFormat format;
    format.format_type = "sql";
    CreateDatabaseConfig config;
    config.database_info.name = "test_db";
    config.database_info.drop_if_exists = false;

    auto formatter = FormatterFactory::instance().create_formatter<CreateDatabaseConfig>(format);
    FormatResult result = formatter->format(config, false);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == "CREATE DATABASE IF NOT EXISTS `test_db`");
    std::cout << "test_format_create_database_without_properties passed!" << std::endl;
}

void test_format_create_database_with_properties() {
    DataFormat format;
    format.format_type = "sql";
    CreateDatabaseConfig config;
    config.database_info.name = "test_db";
    config.database_info.properties = "KEEP 3650";

    auto formatter = FormatterFactory::instance().create_formatter<CreateDatabaseConfig>(format);
    FormatResult result = formatter->format(config, false);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == "CREATE DATABASE IF NOT EXISTS `test_db` KEEP 3650");
    std::cout << "test_format_create_database_with_properties passed!" << std::endl;
}

int main() {
    test_format_drop_database();
    test_format_create_database_without_properties();
    test_format_create_database_with_properties();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}