#include <cassert>
#include <iostream>
#include "RowDataGenerator.h"


void test_generator_mode_basic() {
    // Setup basic configuration
    ColumnsConfig columns_config;
    columns_config.source_type = "generator";
    
    // Setup timestamp strategy
    auto& ts_config = columns_config.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{1000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";
    
    // Setup schema
    auto& schema = columns_config.generator.schema;
    schema = {
        {"col1", "INT", "random", 1, 100},
        {"col2", "FLOAT", "random", 0.0, 1.0}
    };

    // Setup control parameters
    InsertDataConfig::Control control;
    control.data_generation.per_table_rows = 5;

    // Create generator
    RowDataGenerator generator("test_table", columns_config, control, "ms");

    // Verify row generation
    int count = 0;
    while (auto row = generator.next_row()) {
        assert(row->table_name == "test_table");
        assert(row->timestamp == 1000 + count * 10);
        assert(row->columns.size() == 2);
        count++;
    }

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
    
    auto& schema = columns_config.generator.schema;
    schema.emplace_back(ColumnConfig{"col1", "INT", "random", 1, 100});
    
    InsertDataConfig::Control control;
    control.data_generation.per_table_rows = 3;
    
    RowDataGenerator generator("test_table", columns_config, control, "ms");
    
    // 第一轮生成
    std::vector<int32_t> first_round;
    while (auto row = generator.next_row()) {
        first_round.push_back(row->timestamp);
    }
    
    // 重置生成器
    generator.reset();
    
    // 第二轮生成
    std::vector<int32_t> second_round;
    while (auto row = generator.next_row()) {
        second_round.push_back(row->timestamp);
    }
    
    // 验证两轮生成的时间戳相同
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
    
    auto& schema = columns_config.generator.schema;
    schema = {{"col1", "INT", "random", 1, 100}};

    InsertDataConfig::Control control;
    control.data_generation.per_table_rows = 10;
    control.data_generation.data_cache.enabled = true;
    control.data_generation.data_cache.cache_size = 5;

    RowDataGenerator generator("test_table", columns_config, control, "ms");

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
    
    auto& schema = columns_config.generator.schema;
    schema = {{"col1", "INT", "random", 1, 100}};

    InsertDataConfig::Control control;
    control.data_generation.per_table_rows = 30;
    control.data_quality.data_disorder.enabled = true;
    
    // Add a disorder interval
    InsertDataConfig::Control::DataQuality::DataDisorder::Interval interval;
    interval.time_start = "1000";
    interval.time_end = "1100";
    interval.ratio = 1.0;  // 100% disorder
    interval.latency_range = 20;
    control.data_quality.data_disorder.intervals.push_back(interval);

    RowDataGenerator generator("test_table", columns_config, control, "ms");

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
        std::cout << "Row: " << row.table_name << ", Timestamp: " << row.timestamp << ", Columns: " << row.columns <<"\n";
    }

    // Verify some disorder occurred
    bool found_disorder = false;
    for (size_t i = 1; i < timestamps.size(); i++) {
        if (timestamps[i] < timestamps[i-1]) {
            found_disorder = true;
            break;
        }
    }
    assert(found_disorder);

    std::cout << "test_generator_with_disorder passed.\n";
}

void setup_test_csv() {
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

    // 配置CSV数据源
    columns_config.csv.file_path = "test_data.csv";
    columns_config.csv.has_header = true;
    columns_config.csv.delimiter = ",";
    columns_config.csv.tbname_index = 0;

    // 配置时间戳策略
    TimestampOriginalConfig ts_config;
    ts_config.timestamp_index = 1;
    ts_config.timestamp_precision = "ms";
    
    columns_config.csv.timestamp_strategy.strategy_type = "original";
    columns_config.csv.timestamp_strategy.timestamp_config = ts_config;
    
    // 配置数据列
    auto& schema = columns_config.csv.schema;
    schema.emplace_back(ColumnConfig{"age", "INT"});
    schema.emplace_back(ColumnConfig{"city", "VARCHAR"});
    
    InsertDataConfig::Control control;
    
    // 验证table1的数据
    {
        RowDataGenerator generator("table1", columns_config, control, "ms");
        
        // 验证第一行
        auto row1 = generator.next_row();
        assert(row1);
        assert(row1->table_name == "table1");
        assert(row1->timestamp == 1622505600000);
        assert(row1->columns.size() == 2);
        assert(std::get<int32_t>(row1->columns[0]) == 12);
        assert(std::get<std::string>(row1->columns[1]) == "New York");
        
        // 验证第二行
        auto row2 = generator.next_row();
        assert(row2);
        assert(row2->table_name == "table1");
        assert(row2->timestamp == 1622505601000);
        assert(row2->columns.size() == 2);
        assert(std::get<int32_t>(row2->columns[0]) == 25);
        assert(std::get<std::string>(row2->columns[1]) == "Boston");
        
        // 验证没有更多数据
        assert(!generator.next_row());
        assert(!generator.has_more());
    }
    
    // 验证table2的数据
    {
        RowDataGenerator generator("table2", columns_config, control, "ms");
        
        auto row = generator.next_row();
        assert(row);
        assert(row->table_name == "table2");
        assert(row->timestamp == 1622592000000);
        assert(row->columns.size() == 2);
        assert(std::get<int32_t>(row->columns[0]) == 85);
        assert(std::get<std::string>(row->columns[1]) == "Los Angeles");
        
        assert(!generator.next_row());
        assert(!generator.has_more());
    }
    
    // 验证不存在的表
    try {
        RowDataGenerator generator("table3", columns_config, control, "ms");
        assert(false && "Should throw exception for non-existent table");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("not found in CSV file") != std::string::npos);
    }

    cleanup_test_csv();
    std::cout << "test_csv_mode_basic passed.\n";
}

void test_csv_precision_conversion() {
    setup_test_csv();

    ColumnsConfig columns_config;
    columns_config.source_type = "csv";
    columns_config.csv.file_path = "test_data.csv";
    columns_config.csv.has_header = true;
    columns_config.csv.delimiter = ",";
    columns_config.csv.tbname_index = 0;

    TimestampOriginalConfig ts_config;
    ts_config.timestamp_index = 1;
    ts_config.timestamp_precision = "ms";

    columns_config.csv.timestamp_strategy.strategy_type = "original";
    columns_config.csv.timestamp_strategy.timestamp_config = ts_config;

    auto& schema = columns_config.csv.schema;
    schema.emplace_back(ColumnConfig{"age", "INT"});
    schema.emplace_back(ColumnConfig{"city", "VARCHAR"});

    InsertDataConfig::Control control;

    // 测试转换到不同精度
    {
        RowDataGenerator generator("table1", columns_config, control, "us");
        auto row = generator.next_row();
        assert(row);
        assert(row->timestamp == 1622505600000000); // ms -> us

        RowDataGenerator generator2("table1", columns_config, control, "ns");
        auto row2 = generator2.next_row();
        assert(row2);
        assert(row2->timestamp == 1622505600000000000); // ms -> ns
    }

    cleanup_test_csv();
    std::cout << "test_csv_precision_conversion passed.\n";
}

void test_invalid_source_type() {
    ColumnsConfig columns_config;
    columns_config.source_type = "invalid";
    
    InsertDataConfig::Control control;
    
    try {
        RowDataGenerator generator("test_table", columns_config, control, "ms");
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
    test_csv_precision_conversion();
    test_invalid_source_type();
    
    std::cout << "All tests passed.\n";
    return 0;
}