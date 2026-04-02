#include <iostream>
#include <cassert>
#include "FormatterRegistrar.hpp"
#include "SqlSuperTableFormatter.hpp"

void test_format_create_super_table_with_columns_and_tags() {
    DataFormat format;
    format.format_type = "sql";

    CreateSuperTableConfig config;
    config.tdengine.database = "test_db";
    config.schema.name = "test_table";

    // Add columns
    config.schema.columns = {
        {"col1", "INT", "random", 1, 10},
        {"col2_", "BINARY(10)", 2, "random"}
    };

    // Add tags
    config.schema.tags = {
        {"tag1", "FLOAT", "random", 0, 200},
        {"tag2", "NCHAR(20)", "random"},
    };
    config.schema.apply();

    auto formatter = FormatterFactory::create_formatter<CreateSuperTableConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) ==
        "CREATE TABLE IF NOT EXISTS `test_db`.`test_table` (ts TIMESTAMP, `col1` INT, `col2_1` BINARY(10), `col2_2` BINARY(10)) TAGS (`tag1` FLOAT, `tag2` NCHAR(20));");
    std::cout << "test_format_create_super_table_with_columns_and_tags passed!" << std::endl;
}

void test_format_create_super_table_without_columns() {
    DataFormat format;
    format.format_type = "sql";

    CreateSuperTableConfig config;
    config.tdengine.database = "test_db";
    config.schema.name = "test_table";

    // No columns
    config.schema.columns = {};

    // Add tags
    config.schema.tags = {
        {"tag1", "FLOAT", "random", 0, 200}
    };
    config.schema.apply();

    auto formatter = FormatterFactory::create_formatter<CreateSuperTableConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) ==
        "CREATE TABLE IF NOT EXISTS `test_db`.`test_table` (ts TIMESTAMP) TAGS (`tag1` FLOAT);");
    std::cout << "test_format_create_super_table_without_columns passed!" << std::endl;
}

void test_format_create_super_table_without_tags() {
    DataFormat format;
    format.format_type = "sql";

    CreateSuperTableConfig config;
    config.tdengine.database = "test_db";
    config.schema.name = "test_table";

    // Add columns
    config.schema.columns = {
        {"col1", "INT", "random", 1, 10},
    };

    // No tags
    config.schema.tags = {};
    config.schema.apply();

    auto formatter = FormatterFactory::create_formatter<CreateSuperTableConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) ==
        "CREATE TABLE IF NOT EXISTS `test_db`.`test_table` (ts TIMESTAMP, `col1` INT);");
    std::cout << "test_format_create_super_table_without_tags passed!" << std::endl;
}

void test_format_create_super_table_with_empty_config() {
    DataFormat format;
    format.format_type = "sql";

    CreateSuperTableConfig config;
    config.tdengine.database = "test_db";
    config.schema.name = "test_table";

    // No columns and tags
    config.schema.columns = {};
    config.schema.tags = {};
    config.schema.apply();

    auto formatter = FormatterFactory::create_formatter<CreateSuperTableConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) ==
        "CREATE TABLE IF NOT EXISTS `test_db`.`test_table` (ts TIMESTAMP);");
    std::cout << "test_format_create_super_table_with_empty_config passed!" << std::endl;
}

int main() {
    test_format_create_super_table_with_columns_and_tags();
    test_format_create_super_table_without_columns();
    test_format_create_super_table_without_tags();
    test_format_create_super_table_with_empty_config();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}