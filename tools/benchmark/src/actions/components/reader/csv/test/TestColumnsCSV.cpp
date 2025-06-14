#include <iostream>
#include <cassert>
#include <fstream>
#include "ColumnsCSV.h"


void test_validate_config_empty_file_path() {
    ColumnsConfig::CSV config;
    config.file_path = "";
    config.has_header = true;

    try {
        ColumnsCSV columns_csv(config, std::nullopt);
        assert(false && "Expected exception for empty file path");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_empty_file_path passed\n";
    }
}

void test_validate_config_mismatched_column_types() {
    ColumnsConfig::CSV config;
    config.file_path = "test.csv";
    config.has_header = true;

    // Explicitly set timestamp strategy to generator mode
    config.timestamp_strategy.timestamp_config = TimestampGeneratorConfig{};

    std::ofstream test_file("test.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar", std::nullopt},
        {"age", "int", std::nullopt}            // Mismatched size
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    try {
        ColumnsCSV columns_csv(config, instances);
        assert(false && "Expected exception for mismatched column types size");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_mismatched_column_types passed\n";
    }
}

void test_generate_table_data_with_default_timestamp() {
    ColumnsConfig::CSV config;
    config.file_path = "timestamp.csv";
    config.has_header = true;

    // Default timestamp strategy (TimestampOriginalConfig)
    std::ofstream test_file("timestamp.csv");
    test_file << "timestamp,name,city\n"; // First column is the timestamp in milliseconds
    test_file << "1622505600000,Alice,New York\n";
    test_file << "1622592000000,Bob,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar", std::nullopt},
        {"city", "varchar", std::nullopt}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSV columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    assert(table_data[0].timestamps.size() == 2 && "Expected 2 timestamps");
    assert(table_data[0].timestamps[0] == 1622505600000 && "Expected first timestamp to match");
    assert(table_data[0].timestamps[1] == 1622592000000 && "Expected second timestamp to match");
    assert(table_data[0].rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table_data[0].rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<std::string>(table_data[0].rows[0][1]) == "New York" && "Expected second column to be 'New York'");
    std::cout << "test_generate_table_data_with_default_timestamp passed\n";
}

void test_generate_table_data_with_timestamp() {
    ColumnsConfig::CSV config;
    config.file_path = "timestamp.csv";
    config.has_header = true;
    config.timestamp_strategy.timestamp_config = TimestampOriginalConfig{1, "ms", std::nullopt};

    std::ofstream test_file("timestamp.csv");
    test_file << "name,timestamp,city\n";
    test_file << "Alice,1622505600000,New York\n";
    test_file << "Bob,1622592000000,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar", std::nullopt},
        {"city", "varchar", std::nullopt}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSV columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    assert(table_data[0].timestamps.size() == 2 && "Expected 2 timestamps");
    assert(table_data[0].timestamps[0] == 1622505600000 && "Expected first timestamp to match");
    assert(table_data[0].timestamps[1] == 1622592000000 && "Expected second timestamp to match");
    assert(table_data[0].rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table_data[0].rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<std::string>(table_data[0].rows[0][1]) == "New York" && "Expected second column to be 'New York'");
    std::cout << "test_generate_table_data_with_timestamp passed\n";
}

void test_generate_table_data_with_generated_timestamp() {
    ColumnsConfig::CSV config;
    config.file_path = "generated_timestamp.csv";
    config.has_header = true;

    // Explicitly set timestamp strategy to generator mode
    config.timestamp_strategy.timestamp_config = TimestampGeneratorConfig{};

    std::ofstream test_file("generated_timestamp.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file << "Bob,25,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar", std::nullopt},
        {"age", "int", std::nullopt},
        {"city", "varchar", std::nullopt}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSV columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    assert(table_data[0].timestamps.size() == 2 && "Expected 2 timestamps");
    assert(table_data[0].rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table_data[0].rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<int32_t>(table_data[0].rows[0][1]) == 30 && "Expected second column to be 30");
    assert(std::get<std::string>(table_data[0].rows[0][2]) == "New York" && "Expected third column to be 'New York'");
    std::cout << "test_generate_table_data_with_generated_timestamp passed\n";
}


void test_generate_table_data_include_tbname() {
    ColumnsConfig::CSV config;
    config.file_path = "include_tbname.csv";
    config.has_header = true;
    config.tbname_index = 0; // table name column
    config.timestamp_strategy.timestamp_config = TimestampGeneratorConfig{};

    std::ofstream test_file("include_tbname.csv");
    test_file << "table_name,age,city\n";
    test_file << "table1,30,New York\n";
    test_file << "table2,25,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"age", "int", std::nullopt},
        {"city", "varchar", std::nullopt}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSV columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    // 验证表数量
    assert(table_data.size() == 2 && "Expected 2 tables");

    // 使用循环查找表名并验证数据
    bool table1_found = false;
    bool table2_found = false;

    for (const auto& table : table_data) {
        if (table.table_name == "table1") {
            table1_found = true;
            assert(table.timestamps.size() == 1 && "Expected 1 timestamp for table1");
            assert(table.rows.size() == 1 && "Expected 1 row of data for table1");
            assert(std::get<int32_t>(table.rows[0][0]) == 30 && "Expected first column to be 30 for table1");
            assert(std::get<std::string>(table.rows[0][1]) == "New York" && "Expected second column to be 'New York' for table1");
        } else if (table.table_name == "table2") {
            table2_found = true;
            assert(table.timestamps.size() == 1 && "Expected 1 timestamp for table2");
            assert(table.rows.size() == 1 && "Expected 1 row of data for table2");
            assert(std::get<int32_t>(table.rows[0][0]) == 25 && "Expected first column to be 25 for table2");
            assert(std::get<std::string>(table.rows[0][1]) == "Los Angeles" && "Expected second column to be 'Los Angeles' for table2");
        }
    }

    // 验证两个表都被找到
    assert(table1_found && "Expected table1 to be found");
    assert(table2_found && "Expected table2 to be found");

    std::cout << "test_generate_table_data_include_tbname passed\n";
}

void test_generate_table_data_default_column_types() {
    ColumnsConfig::CSV config;
    config.file_path = "default.csv";
    config.has_header = true;

    std::ofstream test_file("default.csv");
    test_file << "timestamp,name,age,city\n";
    test_file << "1622505600000,Alice,30,New York\n";
    test_file << "1622592000000,Bob,25,Los Angeles\n";
    test_file.close();

    ColumnsCSV columns_csv(config, std::nullopt); // No column types provided
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    assert(table_data[0].rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table_data[0].rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<std::string>(table_data[0].rows[0][1]) == "30" && "Expected second column to be '30'");
    assert(std::get<std::string>(table_data[0].rows[0][2]) == "New York" && "Expected third column to be 'New York'");
    std::cout << "test_generate_table_data_default_column_types passed\n";
}

int main() {
    test_validate_config_empty_file_path();
    test_validate_config_mismatched_column_types();
    test_generate_table_data_with_default_timestamp();
    test_generate_table_data_with_timestamp();
    test_generate_table_data_with_generated_timestamp();
    test_generate_table_data_include_tbname();
    test_generate_table_data_default_column_types();

    std::cout << "All tests passed!\n";
    return 0;
}