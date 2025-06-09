#include "ColumnConfig.h"
#include <iostream>
#include <cassert>

void test_get_type_tag() {
    assert(ColumnConfig::get_type_tag("bool") == ColumnTypeTag::BOOL);
    assert(ColumnConfig::get_type_tag("tinyint") == ColumnTypeTag::TINYINT);
    assert(ColumnConfig::get_type_tag("varchar") == ColumnTypeTag::VARCHAR);
    assert(ColumnConfig::get_type_tag("json") == ColumnTypeTag::JSON);

    try {
        ColumnConfig::get_type_tag("unsupported_type");
        assert(false); // Should not reach here
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "Unsupported type: unsupported_type");
    }
}

void test_column_config_default_constructor() {
    ColumnConfig config;
    assert(config.name.empty());
    assert(config.type.empty());
    assert(config.type_tag == ColumnTypeTag::UNKNOWN);
    assert(config.primary_key == false);
    assert(config.count == 1);
}

void test_column_config_with_name_type() {
    ColumnConfig config("column1", "int");
    assert(config.name == "column1");
    assert(config.type == "int");
    assert(config.gen_type == std::nullopt);
    assert(config.type_tag == ColumnTypeTag::INT);
}

void test_column_config_with_name_type_gen_type() {
    ColumnConfig config("column1", "int", "random");
    assert(config.name == "column1");
    assert(config.type == "int");
    assert(config.gen_type == "random");
    assert(config.type_tag == ColumnTypeTag::INT);
}

void test_column_config_with_name_type_gen_type_len() {
    ColumnConfig config("column2", "varchar", "random", 255);
    assert(config.name == "column2");
    assert(config.type == "varchar");
    assert(config.gen_type == "random");
    assert(config.len.has_value());
    assert(config.len.value() == 255);
    assert(config.type_tag == ColumnTypeTag::VARCHAR);
}

void test_column_config_with_name_type_gen_type_min_max() {
    ColumnConfig config("column3", "double", "random", 0.0, 100.0);
    assert(config.name == "column3");
    assert(config.type == "double");
    assert(config.gen_type == "random");
    assert(config.min.has_value());
    assert(config.min.value() == 0.0);
    assert(config.max.has_value());
    assert(config.max.value() == 100.0);
    assert(config.type_tag == ColumnTypeTag::DOUBLE);
}

void test_calc_type_tag() {
    ColumnConfig config("column4", "int", "random");
    config.type = "varchar";
    config.calc_type_tag();
    assert(config.type_tag == ColumnTypeTag::VARCHAR);
}

int main() {
    test_get_type_tag();
    test_column_config_default_constructor();
    test_column_config_with_name_type();
    test_column_config_with_name_type_gen_type();
    test_column_config_with_name_type_gen_type_len();
    test_column_config_with_name_type_gen_type_min_max();
    test_calc_type_tag();

    std::cout << "All tests passed!" << std::endl;
    return 0;
}