#include <iostream>
#include <cassert>
#include "FormatterRegistrar.h"
#include "SqlSuperTableFormatter.h"

void test_format_create_super_table_with_columns_and_tags() {
    DataFormat format;
    format.format_type = "sql";

    CreateSuperTableConfig config;
    config.database_info.name = "test_db";
    config.super_table_info.name = "test_table";

    // 添加列
    config.super_table_info.columns = {
        {"col1", "INT", "random", 1, 10},
        {"col2", "BINARY", "random", 10}
    };

    // 添加标签
    config.super_table_info.tags = {
        {"tag1", "FLOAT", "random", 0, 200},
        {"tag2", "NCHAR", "random", 20},
    };

    auto formatter = FormatterFactory::instance().create_formatter<CreateSuperTableConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == 
        "CREATE TABLE IF NOT EXISTS `test_db`.`test_table` (ts TIMESTAMP, col1 INT, col2 BINARY(10)) TAGS (tag1 FLOAT, tag2 NCHAR(20));");
    std::cout << "test_format_create_super_table_with_columns_and_tags passed!" << std::endl;
}

void test_format_create_super_table_without_columns() {
    DataFormat format;
    format.format_type = "sql";

    CreateSuperTableConfig config;
    config.database_info.name = "test_db";
    config.super_table_info.name = "test_table";

    // 没有列
    config.super_table_info.columns = {};

    // 添加标签
    config.super_table_info.tags = {
        {"tag1", "FLOAT", "random", 0, 200}
    };

    auto formatter = FormatterFactory::instance().create_formatter<CreateSuperTableConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == 
        "CREATE TABLE IF NOT EXISTS `test_db`.`test_table` (ts TIMESTAMP) TAGS (tag1 FLOAT);");
    std::cout << "test_format_create_super_table_without_columns passed!" << std::endl;
}

void test_format_create_super_table_without_tags() {
    DataFormat format;
    format.format_type = "sql";

    CreateSuperTableConfig config;
    config.database_info.name = "test_db";
    config.super_table_info.name = "test_table";

    // 添加列
    config.super_table_info.columns = {
        {"col1", "INT", "random", 1, 10},
    };

    // 没有标签
    config.super_table_info.tags = {};

    auto formatter = FormatterFactory::instance().create_formatter<CreateSuperTableConfig>(format);
    FormatResult result = formatter->format(config);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == 
        "CREATE TABLE IF NOT EXISTS `test_db`.`test_table` (ts TIMESTAMP, col1 INT);");
    std::cout << "test_format_create_super_table_without_tags passed!" << std::endl;
}

void test_format_create_super_table_with_empty_config() {
    DataFormat format;
    format.format_type = "sql";

    CreateSuperTableConfig config;
    config.database_info.name = "test_db";
    config.super_table_info.name = "test_table";

    // 没有列和标签
    config.super_table_info.columns = {};
    config.super_table_info.tags = {};

    auto formatter = FormatterFactory::instance().create_formatter<CreateSuperTableConfig>(format);
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