#include "RowDataGenerator.hpp"
#include "CSVDataManager.hpp"
#include <cassert>
#include <iostream>


void test_generator_mode_basic() {
    // Setup basic configuration
    ColumnsConfig columns_config;
    columns_config.source_type = "generator";

    // Setup timestamp strategy
    auto& ts_config = columns_config.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{1000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";

    // Setup control parameters
    InsertDataConfig config;
    config.schema.columns = {
        {"col1", "INT", "random", 1, 100},
        {"col2", "FLOAT", "random", 0.0, 1.0}
    };
    config.schema.generation.rows_per_table = 5;
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    // Create generator
    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);
    RowDataGenerator generator("test_table", config, instances);

    // Verify row generation
    int count = 0;
    while (auto row = generator.next_row()) {
        assert(row->timestamp == 1000 + count * 10);
        assert(row->columns.size() == 2);
        count++;
    }

    (void)count;
    assert(count == 5);
    assert(!generator.has_more());

    std::cout << "test_generator_mode_basic passed.\n";
}

void test_generator_reset() {
    ColumnsConfig columns_config;
    columns_config.source_type = "generator";

    auto& ts_config = columns_config.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{1000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";

    InsertDataConfig config;
    config.schema.columns.emplace_back(ColumnConfig{"col1", "INT", "random", 1, 100});
    config.schema.generation.rows_per_table = 3;
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);
    RowDataGenerator generator("test_table", config, instances);

    // First round generation
    std::vector<int32_t> first_round;
    while (auto row = generator.next_row()) {
        first_round.push_back(row->timestamp);
    }

    // Reset generator
    generator.reset();

    // Second round generation
    std::vector<int32_t> second_round;
    while (auto row = generator.next_row()) {
        second_round.push_back(row->timestamp);
    }

    // Verify timestamps are the same in both rounds
    assert(first_round.size() == second_round.size());
    for (size_t i = 0; i < first_round.size(); i++) {
        assert(first_round[i] == second_round[i]);
    }

    std::cout << "test_generator_reset passed.\n";
}

void test_generator_with_cache() {
    ColumnsConfig columns_config;
    columns_config.source_type = "generator";

    auto& ts_config = columns_config.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{1000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";

    InsertDataConfig config;
    config.schema.columns.emplace_back(ColumnConfig{"col1", "INT", "random", 1, 100});
    config.schema.generation.rows_per_table = 10;
    config.schema.generation.data_cache.enabled = true;
    config.schema.generation.data_cache.num_cached_batches = 1;
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);
    RowDataGenerator generator("test_table", config, instances);

    // First batch should come from cache
    std::vector<RowData> first_batch;
    for (int i = 0; i < 5; i++) {
        auto row = generator.next_row();
        assert(row);
        first_batch.push_back(*row);
    }

    // Second batch should be generated on-the-fly
    std::vector<RowData> second_batch;
    for (int i = 0; i < 5; i++) {
        auto row = generator.next_row();
        assert(row);
        second_batch.push_back(*row);
    }

    assert(!generator.next_row());
    std::cout << "test_generator_with_cache passed.\n";
}

void test_generator_with_disorder() {
    ColumnsConfig columns_config;
    columns_config.source_type = "generator";

    auto& ts_config = columns_config.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{1000};
    ts_config.timestamp_step = 1;
    ts_config.timestamp_precision = "ms";

    InsertDataConfig config;
    config.schema.columns.emplace_back(ColumnConfig{"col1", "INT", "random", 1, 100});
    config.schema.generation.rows_per_table = 30;
    config.schema.generation.data_disorder.enabled = true;
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    // Add a disorder interval
    GenerationConfig::DataDisorder::Interval interval;
    interval.time_start = "1000";
    interval.time_end = "1100";
    interval.ratio = 1.0;  // 100% disorder
    interval.latency_range = 20;
    config.schema.generation.data_disorder.intervals.push_back(interval);

    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);
    RowDataGenerator generator("test_table", config, instances);

    // Collect all rows
    std::vector<int64_t> timestamps;
    std::vector<RowData> rows;
    while (auto row = generator.next_row()) {
        if (row->timestamp >= 0) {  // Skip delayed rows
            timestamps.push_back(row->timestamp);
            rows.push_back(*row);
        }
    }

    for (const auto& row : rows) {
        std::cout << "Row ==> " << "Timestamp: " << row.timestamp << ", Columns: " << row.columns <<"\n";
    }

    // Verify some disorder occurred
    bool found_disorder = false;
    for (size_t i = 1; i < timestamps.size(); i++) {
        if (timestamps[i] < timestamps[i-1]) {
            found_disorder = true;
            break;
        }
    }
    (void)found_disorder;
    assert(found_disorder);

    std::cout << "test_generator_with_disorder passed.\n";
}

void setup_test_csv() {
    CSVDataManager::reset();
    std::ofstream test_file("test_data.csv");
    test_file << "table,timestamp,age,city\n";
    test_file << "table1,1622505600000,12,New York\n";
    test_file << "table1,1622505601000,25,Boston\n";
    test_file << "table2,1622592000000,85,Los Angeles\n";
    test_file.close();
}

void cleanup_test_csv() {
    std::remove("test_data.csv");
}

void test_csv_mode_basic() {
    setup_test_csv();

    ColumnsConfig columns_config;
    columns_config.source_type = "csv";

    // Configure CSV data source
    columns_config.csv.loading_mode = "preload";
    columns_config.csv.file_path = "test_data.csv";
    columns_config.csv.has_header = true;
    columns_config.csv.delimiter = ",";
    columns_config.csv.tbname_index = 0;

    // Configure timestamp strategy
    TimestampCSVConfig ts_config;
    ts_config.timestamp_index = 1;
    ts_config.timestamp_precision = "ms";

    columns_config.csv.timestamp_strategy.strategy_type = "csv";
    columns_config.csv.timestamp_strategy.csv = ts_config;

    // Configure data columns

    InsertDataConfig config;
    config.schema.columns.emplace_back(ColumnConfig{"age", "INT"});
    config.schema.columns.emplace_back(ColumnConfig{"city", "VARCHAR(20)"});
    config.schema.generation.rows_per_table = 3;
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);

    // Verify data for table1
    {
        RowDataGenerator generator("table1", config, instances);

        // Verify first row
        auto row1 = generator.next_row();
        assert(row1);
        assert(row1->timestamp == 1622505600000);
        assert(row1->columns.size() == 2);
        assert(std::get<int32_t>(row1->columns[0]) == 12);
        assert(std::get<std::string>(row1->columns[1]) == "New York");

        // Verify second row
        auto row2 = generator.next_row();
        assert(row2);
        assert(row2->timestamp == 1622505601000);
        assert(row2->columns.size() == 2);
        assert(std::get<int32_t>(row2->columns[0]) == 25);
        assert(std::get<std::string>(row2->columns[1]) == "Boston");

        // Verify no more data
        assert(!generator.next_row());
        assert(!generator.has_more());
    }

    // Verify data for table2
    {
        RowDataGenerator generator("table2", config, instances);

        auto row = generator.next_row();
        assert(row);
        assert(row->timestamp == 1622592000000);
        assert(row->columns.size() == 2);
        assert(std::get<int32_t>(row->columns[0]) == 85);
        assert(std::get<std::string>(row->columns[1]) == "Los Angeles");

        assert(!generator.next_row());
        assert(!generator.has_more());
    }

    // Verify non-existent table
    try {
        RowDataGenerator generator("table3", config, instances);
        assert(false && "Should throw exception for non-existent table");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("not found in CSV file") != std::string::npos);
    }

    cleanup_test_csv();
    std::cout << "test_csv_mode_basic passed.\n";
}

void test_csv_mode_with_numeric_null_cell() {
    // 1. Setup a CSV file where numeric cell is empty and should map to NULL
    CSVDataManager::reset();
    std::ofstream test_file("invalid_data.csv");
    test_file << "table,timestamp,age,city\n";
    test_file << "table1,1622505600000,,New York\n"; // 'age' is empty
    test_file.close();

    // 2. Configure to use the invalid CSV
    ColumnsConfig columns_config;
    columns_config.source_type = "csv";
    columns_config.csv.file_path = "invalid_data.csv";
    columns_config.csv.has_header = true;
    columns_config.csv.tbname_index = 0;

    TimestampCSVConfig ts_config;
    ts_config.timestamp_index = 1;
    ts_config.timestamp_precision = "ms";
    columns_config.csv.timestamp_strategy.strategy_type = "csv";
    columns_config.csv.timestamp_strategy.csv = ts_config;
    columns_config.csv.loading_mode = "preload";

    InsertDataConfig config;
    // Define schema where 'age' is an INT
    config.schema.columns.emplace_back(ColumnConfig{"age", "INT"});
    config.schema.columns.emplace_back(ColumnConfig{"city", "VARCHAR(20)"});
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);

    // 3. Verify numeric empty cell is treated as NULL (std::monostate)
    {
        RowDataGenerator generator("table1", config, instances);

        auto row = generator.next_row();
        assert(row);
        assert(row->columns.size() == 2);
        assert(std::holds_alternative<std::monostate>(row->columns[0]));
        assert(std::get<std::string>(row->columns[1]) == "New York");
        assert(!generator.next_row());
        assert(!generator.has_more());
        std::cout << "test_csv_mode_with_numeric_null_cell passed.\n";
    }

    // Cleanup
    std::remove("invalid_data.csv");
}

void test_csv_mode_with_non_numeric_literal_throws() {
    CSVDataManager::reset();
    std::ofstream test_file("invalid_literal.csv");
    test_file << "table,timestamp,age,city\n";
    test_file << "table1,1622505600000,abc,New York\n";
    test_file.close();

    ColumnsConfig columns_config;
    columns_config.source_type = "csv";
    columns_config.csv.file_path = "invalid_literal.csv";
    columns_config.csv.has_header = true;
    columns_config.csv.tbname_index = 0;
    columns_config.csv.loading_mode = "preload";

    TimestampCSVConfig ts_config;
    ts_config.timestamp_index = 1;
    ts_config.timestamp_precision = "ms";
    columns_config.csv.timestamp_strategy.strategy_type = "csv";
    columns_config.csv.timestamp_strategy.csv = ts_config;

    InsertDataConfig config;
    config.schema.columns.emplace_back(ColumnConfig{"age", "INT"});
    config.schema.columns.emplace_back(ColumnConfig{"city", "VARCHAR(20)"});
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);

    bool threw = false;
    try {
        RowDataGenerator generator("table1", config, instances);
        auto row = generator.next_row();
        (void)row;
    } catch (const std::exception& e) {
        threw = true;
        std::string msg = e.what();
        assert(msg.find("convert") != std::string::npos
            || msg.find("Invalid integer") != std::string::npos
            || msg.find("stoll") != std::string::npos);
    }

    (void)threw;
    assert(threw && "Expected conversion failure for non-numeric literal in INT column");
    std::remove("invalid_literal.csv");
    std::cout << "test_csv_mode_with_non_numeric_literal_throws passed.\n";
}

void test_csv_precision_conversion() {
    setup_test_csv();

    ColumnsConfig columns_config;
    columns_config.source_type = "csv";
    columns_config.csv.file_path = "test_data.csv";
    columns_config.csv.has_header = true;
    columns_config.csv.delimiter = ",";
    columns_config.csv.tbname_index = 0;

    TimestampCSVConfig ts_config;
    ts_config.timestamp_index = 1;
    ts_config.timestamp_precision = "ms";

    columns_config.csv.timestamp_strategy.strategy_type = "csv";
    columns_config.csv.timestamp_strategy.csv = ts_config;
    columns_config.csv.loading_mode = "preload";

    InsertDataConfig config;
    config.schema.columns.emplace_back(ColumnConfig{"age", "INT"});
    config.schema.columns.emplace_back(ColumnConfig{"city", "VARCHAR(20)"});
    config.schema.generation.rows_per_table = 3;
    config.timestamp_precision = "us";
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);

    // Test conversion to different precisions
    {
        RowDataGenerator generator("table1", config, instances);
        auto row = generator.next_row();
        assert(row);
        assert(row->timestamp == 1622505600000000); // ms -> us

        config.timestamp_precision = "ns";
        RowDataGenerator generator2("table1", config, instances);
        auto row2 = generator2.next_row();
        assert(row2);
        assert(row2->timestamp == 1622505600000000000); // ms -> ns
    }

    cleanup_test_csv();
    std::cout << "test_csv_precision_conversion passed.\n";
}

void test_csv_mode_default_table_shared_data() {
    CSVDataManager::reset();
    std::ofstream test_file("default_table.csv");
    test_file << "timestamp,age,city\n";
    test_file << "1622505600000,12,New York\n";
    test_file << "1622505601000,25,Boston\n";
    test_file.close();

    ColumnsConfig columns_config;
    columns_config.source_type = "csv";
    columns_config.csv.file_path = "default_table.csv";
    columns_config.csv.has_header = true;
    columns_config.csv.tbname_index = -1;

    TimestampCSVConfig ts_config;
    ts_config.timestamp_index = 0;
    ts_config.timestamp_precision = "ms";

    columns_config.csv.timestamp_strategy.strategy_type = "csv";
    columns_config.csv.timestamp_strategy.csv = ts_config;

    InsertDataConfig config;
    config.schema.columns.emplace_back(ColumnConfig{"age", "INT"});
    config.schema.columns.emplace_back(ColumnConfig{"city", "VARCHAR(20)"});
    config.schema.generation.rows_per_table = 2;
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);

    RowDataGenerator generator1("subtable_001", config, instances);
    RowDataGenerator generator2("subtable_002", config, instances);

    auto row1 = generator1.next_row();
    auto row2 = generator2.next_row();

    assert(row1);
    assert(row2);
    assert(row1->timestamp == row2->timestamp);
    assert(std::get<int32_t>(row1->columns[0]) == std::get<int32_t>(row2->columns[0]));
    assert(std::get<std::string>(row1->columns[1]) == std::get<std::string>(row2->columns[1]));

    auto row1_next = generator1.next_row();
    auto row2_next = generator2.next_row();

    assert(row1_next);
    assert(row2_next);
    assert(row1_next->timestamp == row2_next->timestamp);
    assert(std::get<int32_t>(row1_next->columns[0]) == std::get<int32_t>(row2_next->columns[0]));
    assert(std::get<std::string>(row1_next->columns[1]) == std::get<std::string>(row2_next->columns[1]));

    std::remove("default_table.csv");
    std::cout << "test_csv_mode_default_table_shared_data passed.\n";
}

void test_invalid_source_type() {
    ColumnsConfig columns_config;
    columns_config.source_type = "invalid";

    InsertDataConfig config;
    config.schema.columns.emplace_back(ColumnConfig{"col1", "INT", "random", 1, 100});
    config.schema.generation.rows_per_table = 3;
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;
    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);

    try {
        RowDataGenerator generator("test_table", config, instances);
        assert(false && "Should throw exception for invalid source type");
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()).find("Unsupported source_type") != std::string::npos);
        std::cout << "test_invalid_source_type passed.\n";
    }
}

int main() {
    test_generator_mode_basic();
    test_generator_reset();
    test_generator_with_cache();
    test_generator_with_disorder();
    test_csv_mode_basic();
    test_csv_mode_with_numeric_null_cell();
    test_csv_mode_with_non_numeric_literal_throws();
    test_csv_precision_conversion();
    test_csv_mode_default_table_shared_data();
    test_invalid_source_type();

    std::cout << "All tests passed.\n";
    return 0;
}