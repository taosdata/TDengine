#include <iostream>
#include <cassert>
#include "FormatterRegistrar.h"
#include "SqlChildTableFormatter.h"


void test_format_create_child_table_single() {
    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.database_info.name = "test_db";
    config.super_table_info.name = "test_super_table";

    std::string table_name = "child_table_1";
    RowType tags = {3.14, std::string("California")};

    SqlChildTableFormatter formatter(format);
    std::string result = formatter.format(config, table_name, tags);

    assert(result == "CREATE TABLE IF NOT EXISTS `test_db`.`child_table_1` USING `test_db`.`test_super_table` TAGS (3.14, 'California');");
    std::cout << "test_format_create_child_table_single passed!" << std::endl;
}

void test_format_create_child_table_multiple() {
    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.database_info.name = "test_db";
    config.super_table_info.name = "test_super_table";

    std::vector<std::string> table_names = {"child_table_1", "child_table_2"};
    std::vector<RowType> tags = {
        {3.14, std::string("California")},
        {2.71, std::string("New York")}
    };

    auto formatter = FormatterFactory::instance().create_formatter<CreateChildTableConfig>(format);
    FormatResult result = formatter->format(config, table_names, tags);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) ==
           "CREATE TABLE IF NOT EXISTS `test_db`.`child_table_1` USING `test_db`.`test_super_table` TAGS (3.14, 'California');\n"
           "CREATE TABLE IF NOT EXISTS `test_db`.`child_table_2` USING `test_db`.`test_super_table` TAGS (2.71, 'New York');");
    std::cout << "test_format_create_child_table_multiple passed!" << std::endl;
}

void test_format_create_child_table_empty_tags() {
    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.database_info.name = "test_db";
    config.super_table_info.name = "test_super_table";

    std::string table_name = "child_table_1";
    RowType tags = {};

    SqlChildTableFormatter formatter(format);
    std::string result = formatter.format(config, table_name, tags);

    assert(result == "CREATE TABLE IF NOT EXISTS `test_db`.`child_table_1` USING `test_db`.`test_super_table` TAGS ();");
    std::cout << "test_format_create_child_table_empty_tags passed!" << std::endl;
}

int main() {
    test_format_create_child_table_single();
    test_format_create_child_table_multiple();
    test_format_create_child_table_empty_tags();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}