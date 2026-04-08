#include <iostream>
#include <cassert>
#include <fstream>
#include "ColumnsCSVReader.hpp"


void test_validate_config_empty_file_path() {
    ColumnsCSV config;
    config.file_path = "";
    config.has_header = true;

    try {
        ColumnsCSVReader columns_csv(config, std::nullopt);
        assert(false && "Expected exception for empty file path");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_empty_file_path passed\n";
    }
}

void test_validate_config_mismatched_column_types() {
    ColumnsCSV config;
    config.file_path = "test.csv";
    config.has_header = true;

    // Explicitly set timestamp strategy to generator mode
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    std::ofstream test_file("test.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"}            // Mismatched size
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    try {
        ColumnsCSVReader columns_csv(config, instances);
        assert(false && "Expected exception for mismatched column types size");
    } catch (const std::invalid_argument& e) {
        std::cout << "test_validate_config_mismatched_column_types passed\n";
    }
}

void test_generate_table_data_with_default_timestamp() {
    ColumnsCSV config;
    config.enabled = true;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.enabled = true;
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.timestamp_strategy.csv.timestamp_precision = "ms";
    config.file_path = "timestamp.csv";
    config.has_header = true;

    // Default timestamp strategy (TimestampCSVConfig)
    std::ofstream test_file("timestamp.csv");
    test_file << "timestamp,name,city\n"; // First column is the timestamp in milliseconds
    test_file << "1622505600000,Alice,New York\n";
    test_file << "1622592000000,Bob,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    const auto& table_pair = *table_data.begin();
    const auto& table = table_pair.second;

    (void)table;
    assert(table.timestamps.size() == 2 && "Expected 2 timestamps");
    assert(table.timestamps[0] == 1622505600000 && "Expected first timestamp to match");
    assert(table.timestamps[1] == 1622592000000 && "Expected second timestamp to match");
    assert(table.rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table.rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<std::string>(table.rows[0][1]) == "New York" && "Expected second column to be 'New York'");
    std::cout << "test_generate_table_data_with_default_timestamp passed\n";
}

void test_generate_table_data_with_timestamp() {
    ColumnsCSV config;
    config.enabled = true;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.enabled = true;
    config.timestamp_strategy.csv.timestamp_index = 1;
    config.timestamp_strategy.csv.timestamp_precision = "ms";
    config.file_path = "timestamp.csv";
    config.has_header = true;

    std::ofstream test_file("timestamp.csv");
    test_file << "name,timestamp,city\n";
    test_file << "Alice,1622505600000,New York\n";
    test_file << "Bob,1622592000000,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    const auto& table_pair = *table_data.begin();
    const auto& table = table_pair.second;

    (void)table;
    assert(table.timestamps.size() == 2 && "Expected 2 timestamps");
    assert(table.timestamps[0] == 1622505600000 && "Expected first timestamp to match");
    assert(table.timestamps[1] == 1622592000000 && "Expected second timestamp to match");
    assert(table.rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table.rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<std::string>(table.rows[0][1]) == "New York" && "Expected second column to be 'New York'");
    std::cout << "test_generate_table_data_with_timestamp passed\n";
}

void test_generate_table_data_with_generated_timestamp() {
    ColumnsCSV config;
    config.file_path = "generated_timestamp.csv";
    config.has_header = true;

    // Explicitly set timestamp strategy to generator mode
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    std::ofstream test_file("generated_timestamp.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file << "Bob,25,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    const auto& table_pair = *table_data.begin();
    const auto& table = table_pair.second;

    (void)table;
    assert(table.timestamps.size() == 2 && "Expected 2 timestamps");
    assert(table.rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table.rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<int32_t>(table.rows[0][1]) == 30 && "Expected second column to be 30");
    assert(std::get<std::string>(table.rows[0][2]) == "New York" && "Expected third column to be 'New York'");
    std::cout << "test_generate_table_data_with_generated_timestamp passed\n";
}

void test_generate_table_data_include_tbname() {
    ColumnsCSV config;
    config.file_path = "include_tbname.csv";
    config.has_header = true;
    config.tbname_index = 0; // table name column
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    std::ofstream test_file("include_tbname.csv");
    test_file << "table_name,age,city\n";
    test_file << "table1,30,New York\n";
    test_file << "table2,25,Los Angeles\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"age", "int"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    // Verify table count
    assert(table_data.size() == 2 && "Expected 2 tables");

    // Check table names and data
    assert(table_data.find("table1") != table_data.end() && "Expected table1 to be found");
    assert(table_data.find("table2") != table_data.end() && "Expected table2 to be found");

    const auto& table1 = table_data.at("table1");
    (void)table1;
    assert(table1.timestamps.size() == 1 && "Expected 1 timestamp for table1");
    assert(table1.rows.size() == 1 && "Expected 1 row of data for table1");
    assert(std::get<int32_t>(table1.rows[0][0]) == 30 && "Expected first column to be 30 for table1");
    assert(std::get<std::string>(table1.rows[0][1]) == "New York" && "Expected second column to be 'New York' for table1");

    const auto& table2 = table_data.at("table2");
    assert(table2.timestamps.size() == 1 && "Expected 1 timestamp for table2");
    assert(table2.rows.size() == 1 && "Expected 1 row of data for table2");
    (void)table2;
    assert(std::get<int32_t>(table2.rows[0][0]) == 25 && "Expected first column to be 25 for table2");
    assert(std::get<std::string>(table2.rows[0][1]) == "Los Angeles" && "Expected second column to be 'Los Angeles' for table2");

    std::cout << "test_generate_table_data_include_tbname passed\n";
}

void test_generate_table_data_default_column_types() {
    ColumnsCSV config;
    config.enabled = true;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.enabled = true;
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.timestamp_strategy.csv.timestamp_precision = "ms";
    config.file_path = "default.csv";
    config.has_header = true;

    std::ofstream test_file("default.csv");
    test_file << "timestamp,name,age,city\n";
    test_file << "1622505600000,Alice,30,New York\n";
    test_file << "1622592000000,Bob,25,Los Angeles\n";
    test_file.close();

    ColumnsCSVReader columns_csv(config, std::nullopt); // No column types provided
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    const auto& table_pair = *table_data.begin();
    const auto& table = table_pair.second;

    (void)table;
    assert(table.rows.size() == 2 && "Expected 2 rows of data");
    assert(std::get<std::string>(table.rows[0][0]) == "Alice" && "Expected first column to be 'Alice'");
    assert(std::get<std::string>(table.rows[0][1]) == "30" && "Expected second column to be '30'");
    assert(std::get<std::string>(table.rows[0][2]) == "New York" && "Expected third column to be 'New York'");
    std::cout << "test_generate_table_data_default_column_types passed\n";

}

void test_generate_with_invalid_data_format() {
    ColumnsCSV config;
    config.file_path = "invalid_format.csv";
    config.has_header = true;
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    std::ofstream test_file("invalid_format.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,30,New York\n";
    test_file << "Bob,not_a_number,Los Angeles\n"; // Invalid integer literal
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);

    try {
        columns_csv.generate();
        assert(false && "Expected an exception for invalid data format");
    } catch (const std::exception& e) {
        std::string error_message = e.what();
        // Check if the error message indicates a conversion failure
        assert((error_message.find("stoll") != std::string::npos
             || error_message.find("Invalid integer") != std::string::npos
             || error_message.find("convert") != std::string::npos)
             && "Error message should indicate a conversion failure");
        std::cout << "test_generate_with_invalid_data_format passed\n";
    }
}

void test_generate_with_numeric_null_literals() {
    ColumnsCSV config;
    config.file_path = "null_literals.csv";
    config.has_header = true;
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    std::ofstream test_file("null_literals.csv");
    test_file << "name,age,city\n";
    test_file << "Alice,,Shenzhen\n";
    test_file << "Bob,NULL,Shanghai\n";
    test_file << "Carol,NA,Beijing\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    const auto& table = table_data.begin()->second;
    (void)table;
    assert(table.rows.size() == 3 && "Expected 3 rows");

    assert(std::holds_alternative<std::monostate>(table.rows[0][1]));
    assert(std::holds_alternative<std::monostate>(table.rows[1][1]));
    assert(std::holds_alternative<std::monostate>(table.rows[2][1]));

    assert(std::get<std::string>(table.rows[0][0]) == "Alice");
    assert(std::get<std::string>(table.rows[0][2]) == "Shenzhen");

    std::cout << "test_generate_with_numeric_null_literals passed\n";
}

void test_generate_with_bool_null_literals() {
    ColumnsCSV config;
    config.file_path = "bool_null_literals.csv";
    config.has_header = true;
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    std::ofstream test_file("bool_null_literals.csv");
    test_file << "name,enabled,city\n";
    test_file << "Alice,,Shenzhen\n";
    test_file << "Bob,NULL,Shanghai\n";
    test_file << "Carol,NA,Beijing\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"enabled", "bool"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    const auto& table = table_data.begin()->second;
    (void)table;
    assert(table.rows.size() == 3 && "Expected 3 rows");

    assert(std::holds_alternative<std::monostate>(table.rows[0][1]));
    assert(std::holds_alternative<std::monostate>(table.rows[1][1]));
    assert(std::holds_alternative<std::monostate>(table.rows[2][1]));

    std::remove("bool_null_literals.csv");
    std::cout << "test_generate_with_bool_null_literals passed\n";
}

void test_generate_with_relative_offset_minutes() {
    ColumnsCSV config;
    config.enabled = true;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.enabled = true;
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.timestamp_strategy.csv.timestamp_precision = "s";

    // Set up relative offset with minutes
    config.timestamp_strategy.csv.offset_config = TimestampCSVConfig::OffsetConfig(
        "relative",
        std::string("+30m"),  // Add 30 minutes
        "s"
    );

    config.file_path = "relative_offset_minutes.csv";
    config.has_header = true;

    std::ofstream test_file("relative_offset_minutes.csv");
    test_file << "timestamp,name,city\n";
    test_file << "1622505600,Alice,New York\n";  // 2021-06-01 00:00:00 UTC
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    const auto& table = table_data.begin()->second;
    (void)table;
    assert(table.timestamps.size() == 1 && "Expected 1 timestamp");
    // Original: 1622505600, after +30m: 1622505600 + 1800 = 1622507400
    assert(table.timestamps[0] == 1622507400 && "Expected timestamp to be offset by 30 minutes");

    std::cout << "test_generate_with_relative_offset_minutes passed\n";
}

void test_generate_with_relative_offset_months() {
    ColumnsCSV config;
    config.enabled = true;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.enabled = true;
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.timestamp_strategy.csv.timestamp_precision = "s";

    // Set up relative offset with months (uppercase M)
    config.timestamp_strategy.csv.offset_config = TimestampCSVConfig::OffsetConfig(
        "relative",
        std::string("+2M"),  // Add 2 months
        "s"
    );

    config.file_path = "relative_offset_months.csv";
    config.has_header = true;

    std::ofstream test_file("relative_offset_months.csv");
    test_file << "timestamp,name,city\n";
    test_file << "1622505600,Alice,New York\n";  // 2021-06-01 00:00:00 UTC
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    const auto& table = table_data.begin()->second;
    (void)table;
    assert(table.timestamps.size() == 1 && "Expected 1 timestamp");
    // After +2M, should be around August 2021
    assert(table.timestamps[0] > 1622505600 && "Expected timestamp to be offset by 2 months");

    std::cout << "test_generate_with_relative_offset_months passed\n";
}

void test_generate_with_relative_offset_mixed() {
    ColumnsCSV config;
    config.enabled = true;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.enabled = true;
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.timestamp_strategy.csv.timestamp_precision = "s";

    // Set up relative offset with mixed units
    config.timestamp_strategy.csv.offset_config = TimestampCSVConfig::OffsetConfig(
        "relative",
        std::string("+1d2h30m"),  // Add 1 day, 2 hours, 30 minutes
        "s"
    );

    config.file_path = "relative_offset_mixed.csv";
    config.has_header = true;

    std::ofstream test_file("relative_offset_mixed.csv");
    test_file << "timestamp,name,city\n";
    test_file << "1622505600,Alice,New York\n";  // 2021-06-01 00:00:00 UTC
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    const auto& table = table_data.begin()->second;
    (void)table;
    assert(table.timestamps.size() == 1 && "Expected 1 timestamp");
    // 1d = 86400s, 2h = 7200s, 30m = 1800s, total = 95400s
    int64_t expected = 1622505600 + 86400 + 7200 + 1800;
    (void)expected;
    assert(table.timestamps[0] == expected && "Expected timestamp to be offset by 1d2h30m");

    std::cout << "test_generate_with_relative_offset_mixed passed\n";
}

void test_generate_with_absolute_offset() {
    ColumnsCSV config;
    config.enabled = true;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.enabled = true;
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.timestamp_strategy.csv.timestamp_precision = "s";

    // Set up absolute offset
    config.timestamp_strategy.csv.offset_config = TimestampCSVConfig::OffsetConfig(
        "absolute",
        int64_t(1700000000),  // Starting timestamp
        "s"
    );

    config.file_path = "absolute_offset.csv";
    config.has_header = true;

    std::ofstream test_file("absolute_offset.csv");
    test_file << "timestamp,name,city\n";
    test_file << "1622505600,Alice,New York\n";   // First timestamp
    test_file << "1622505700,Bob,Los Angeles\n";  // +100 seconds
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    const auto& table = table_data.begin()->second;
    (void)table;
    assert(table.timestamps.size() == 2 && "Expected 2 timestamps");
    // First timestamp should be the absolute value
    assert(table.timestamps[0] == 1700000000 && "Expected first timestamp to be absolute value");
    // Second timestamp should be absolute value + delta (100)
    assert(table.timestamps[1] == 1700000100 && "Expected second timestamp to maintain delta");

    std::cout << "test_generate_with_absolute_offset passed\n";
}

void test_generate_with_negative_relative_offset() {
    ColumnsCSV config;
    config.enabled = true;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.enabled = true;
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.timestamp_strategy.csv.timestamp_precision = "s";

    // Set up negative relative offset
    config.timestamp_strategy.csv.offset_config = TimestampCSVConfig::OffsetConfig(
        "relative",
        std::string("-1d"),  // Subtract 1 day
        "s"
    );

    config.file_path = "negative_offset.csv";
    config.has_header = true;

    std::ofstream test_file("negative_offset.csv");
    test_file << "timestamp,name,city\n";
    test_file << "1622505600,Alice,New York\n";
    test_file.close();

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    ColumnsCSVReader columns_csv(config, instances);
    auto table_data = columns_csv.generate();

    assert(table_data.size() == 1 && "Expected 1 table");
    const auto& table = table_data.begin()->second;

    assert(table.timestamps.size() == 1 && "Expected 1 timestamp");
    // 1d = 86400s
    int64_t expected = 1622505600 - 86400;
    (void)table;
    (void)expected;
    assert(table.timestamps[0] == expected && "Expected timestamp to be offset by -1d");

    std::cout << "test_generate_with_negative_relative_offset passed\n";
}

int main() {
    test_validate_config_empty_file_path();
    test_validate_config_mismatched_column_types();
    test_generate_table_data_with_default_timestamp();
    test_generate_table_data_with_timestamp();
    test_generate_table_data_with_generated_timestamp();
    test_generate_table_data_include_tbname();
    test_generate_table_data_default_column_types();
    test_generate_with_invalid_data_format();
    test_generate_with_numeric_null_literals();
    test_generate_with_bool_null_literals();
    test_generate_with_relative_offset_minutes();
    test_generate_with_relative_offset_months();
    test_generate_with_relative_offset_mixed();
    test_generate_with_absolute_offset();
    test_generate_with_negative_relative_offset();

    std::cout << "All tests passed!\n";
    return 0;
}