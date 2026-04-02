#include "TagsCSVReader.hpp"
#include <iostream>
#include <cassert>
#include <fstream>

void test_validate_config_empty_file_path() {
    TagsCSV config;
    config.file_path = "";
    config.has_header = true;

    try {
        TagsCSVReader tags_csv(config, std::nullopt);
        assert(false && "Expected exception for empty file path");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_empty_file_path passed\n";
    }
}

void test_validate_config_mismatched_tag_types() {
    TagsCSV config;
    config.file_path = "test.csv";
    config.has_header = true;

    std::ofstream test_file("test.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file.close();

    ColumnConfigVector tag_configs = {
        {"name", "varchar(20)"},
        {"age", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(tag_configs);

    try {
        TagsCSVReader tags_csv(config, instances);
        assert(false && "Expected exception for mismatched tag types size");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_mismatched_tag_types passed\n";
    }
}

void test_generate_tags_valid_csv() {
    TagsCSV config;
    config.file_path = "valid.csv";
    config.has_header = true;

    std::ofstream test_file("valid.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file << "Bob,25,Los Angeles\n";
    test_file.close();

    ColumnConfigVector tag_configs = {
        {"name", "varchar(20)"},
        {"age", "int"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(tag_configs);

    TagsCSVReader tags_csv(config, instances);
    auto tags = tags_csv.generate();

    assert(tags.size() == 2 && "Expected 2 rows of tags");
    assert(std::get<std::string>(tags[0][0]) == "Alice" && "Expected first tag to be 'Alice'");
    assert(std::get<int32_t>(tags[0][1]) == 30 && "Expected second tag to be 30");
    assert(std::get<std::string>(tags[0][2]) == "New York" && "Expected third tag to be 'New York'");
    std::cout << "test_generate_tags_valid_csv passed\n";
}

void test_generate_tags_excluded_columns() {
    TagsCSV config;
    config.file_path = "excluded.csv";
    config.has_header = true;
    config.exclude_indices = {1}; // Exclude the second column

    std::ofstream test_file("excluded.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file << "Bob,25,Los Angeles\n";
    test_file.close();

    ColumnConfigVector tag_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(tag_configs);


    TagsCSVReader tags_csv(config, instances);
    auto tags = tags_csv.generate();

    assert(tags.size() == 2 && "Expected 2 rows of tags");
    assert(std::get<std::string>(tags[0][0]) == "Alice" && "Expected first tag to be 'Alice'");
    assert(std::get<std::string>(tags[0][1]) == "New York" && "Expected second tag to be 'New York'");
    std::cout << "test_generate_tags_excluded_columns passed\n";
}

void test_generate_tags_default_tag_types() {
    TagsCSV config;
    config.file_path = "default.csv";
    config.has_header = true;

    std::ofstream test_file("default.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file << "Bob,25,Los Angeles\n";
    test_file.close();

    TagsCSVReader tags_csv(config, std::nullopt); // No tag types provided
    auto tags = tags_csv.generate();

    assert(tags.size() == 2 && "Expected 2 rows of tags");
    assert(std::get<std::string>(tags[0][0]) == "Alice" && "Expected first tag to be 'Alice'");
    assert(std::get<std::string>(tags[0][1]) == "30" && "Expected second tag to be '30'");
    assert(std::get<std::string>(tags[0][2]) == "New York" && "Expected third tag to be 'New York'");
    std::cout << "test_generate_tags_default_tag_types passed\n";
}

int main() {
    test_validate_config_empty_file_path();
    test_validate_config_mismatched_tag_types();
    test_generate_tags_valid_csv();
    test_generate_tags_excluded_columns();
    test_generate_tags_default_tag_types();

    std::cout << "All tests passed!\n";
    return 0;
}