#include <iostream>
#include <cassert>
#include "FormatterRegistrar.hpp"
#include "SqlChildTableFormatter.hpp"


void test_format_create_child_table_single() {
    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.tdengine.database = "test_db";
    config.schema.name = "test_super_table";

    std::string table_name = "child_table_1";
    RowType tags = {3.14, std::string("California")};

    SqlChildTableFormatter formatter(format);
    std::string result = formatter.format(config, table_name, tags);

    assert(result == "CREATE TABLE IF NOT EXISTS `test_db`.`child_table_1` USING `test_db`.`test_super_table` TAGS (3.140000, 'California')");
    std::cout << "test_format_create_child_table_single passed!" << std::endl;
}

void test_format_create_child_table_multiple() {
    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.tdengine.database = "test_db";
    config.schema.name = "test_super_table";

    std::vector<std::string> table_names = {"child_table_1", "child_table_2"};
    std::vector<RowType> tags = {
        {3.14, std::string("California")},
        {2.71, std::string("New York")}
    };

    auto formatter = FormatterFactory::create_formatter<CreateChildTableConfig>(format);
    FormatResult result = formatter->format(config, table_names, tags);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) ==
           "CREATE TABLE IF NOT EXISTS `test_db`.`child_table_1` USING `test_db`.`test_super_table` TAGS (3.140000, 'California') "
           "IF NOT EXISTS `test_db`.`child_table_2` USING `test_db`.`test_super_table` TAGS (2.710000, 'New York');");
    std::cout << "test_format_create_child_table_multiple passed!" << std::endl;
}

void test_format_create_child_table_empty_tags() {
    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.tdengine.database = "test_db";
    config.schema.name = "test_super_table";

    std::string table_name = "child_table_1";
    RowType tags = {};

    SqlChildTableFormatter formatter(format);
    std::string result = formatter.format(config, table_name, tags);

    assert(result == "CREATE TABLE IF NOT EXISTS `test_db`.`child_table_1` USING `test_db`.`test_super_table` TAGS ()");
    std::cout << "test_format_create_child_table_empty_tags passed!" << std::endl;
}

void test_format_escape_single_quote_string() {
    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.tdengine.database = "test_db";
    config.schema.name = "test_super_table";

    std::string table_name = "child_table_quote";
    RowType tags = {3.14, std::string("Xi'an")};

    SqlChildTableFormatter formatter(format);
    std::string result = formatter.format(config, table_name, tags);

    assert(result == "CREATE TABLE IF NOT EXISTS `test_db`.`child_table_quote` USING `test_db`.`test_super_table` TAGS (3.140000, 'Xi''an')");
    std::cout << "test_format_escape_single_quote_string passed!" << std::endl;
}

void test_format_escape_single_quote_u16string() {
    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.tdengine.database = "test_db";
    config.schema.name = "test_super_table";

    std::string table_name = "child_table_quote_u16";
    RowType tags = {3.14, std::u16string(u"北'京")};

    SqlChildTableFormatter formatter(format);
    std::string result = formatter.format(config, table_name, tags);

    assert(result == "CREATE TABLE IF NOT EXISTS `test_db`.`child_table_quote_u16` USING `test_db`.`test_super_table` TAGS (3.140000, '北''京')");
    std::cout << "test_format_escape_single_quote_u16string passed!" << std::endl;
}

int main() {
    test_format_create_child_table_single();
    test_format_create_child_table_multiple();
    test_format_create_child_table_empty_tags();
    test_format_escape_single_quote_string();
    test_format_escape_single_quote_u16string();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}