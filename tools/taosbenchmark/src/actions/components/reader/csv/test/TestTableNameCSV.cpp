#include <iostream>
#include <cassert>
#include <fstream>
#include "TableNameCSV.h"


void test_validate_config_empty_file_path() {
    TableNameConfig::CSV config;
    config.file_path = "";
    config.tbname_index = 0;

    try {
        TableNameCSV table_name_csv(config);
        assert(false && "Expected exception for empty file path");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_empty_file_path passed\n";
    }
}

void test_validate_config_negative_index() {
    TableNameConfig::CSV config;
    config.file_path = "test.csv";
    config.tbname_index = -1;

    try {
        TableNameCSV table_name_csv(config);
        assert(false && "Expected exception for negative index");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_negative_index passed\n";
    }
}

void test_generate_names_valid_csv() {
    std::ofstream test_file("valid.csv");
    test_file << "name,age,city\n";
    test_file << "table1,30,New York\n";
    test_file << "table2,25,Los Angeles\n";
    test_file.close();

    TableNameConfig::CSV config;
    config.file_path = "valid.csv";
    config.has_header = true;
    config.tbname_index = 0;

    TableNameCSV table_name_csv(config);
    auto names = table_name_csv.generate();

    assert(names.size() == 2 && "Expected 2 table names");
    assert(names[0] == "table1" && "Expected first table name to be 'table1'");
    assert(names[1] == "table2" && "Expected second table name to be 'table2'");
    std::cout << "test_generate_names_valid_csv passed\n";
}

void test_generate_names_invalid_column_index() {
    std::ofstream test_file("invalid_index.csv");
    test_file << "name,age,city\n";
    test_file << "table1,30,New York\n";
    test_file.close();

    TableNameConfig::CSV config;
    config.file_path = "invalid_index.csv";
    config.has_header = true;
    config.tbname_index = 5; // Invalid index

    try {
        TableNameCSV table_name_csv(config);
        table_name_csv.generate();
        assert(false && "Expected exception for invalid column index");
    } catch (const std::out_of_range& e) {
        std::cout << "test_generate_names_invalid_column_index passed\n";
    }
}

void test_generate_names_empty_table_name() {
    std::ofstream test_file("empty_name.csv");
    test_file << "name,age,city\n";
    test_file << ",30,New York\n"; // Empty table name
    test_file.close();

    TableNameConfig::CSV config;
    config.file_path = "empty_name.csv";
    config.has_header = true;
    config.tbname_index = 0;

    try {
        TableNameCSV table_name_csv(config);
        table_name_csv.generate();
        assert(false && "Expected exception for empty table name");
    } catch (const std::runtime_error& e) {
        std::cout << "test_generate_names_empty_table_name passed\n";
    }
}

int main() {
    test_validate_config_empty_file_path();
    test_validate_config_negative_index();
    test_generate_names_valid_csv();
    test_generate_names_invalid_column_index();
    test_generate_names_empty_table_name();

    std::cout << "All tests passed!\n";
    return 0;
}