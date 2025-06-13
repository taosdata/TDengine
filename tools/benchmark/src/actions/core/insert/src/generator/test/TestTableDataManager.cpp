#include "TableDataManager.h"
#include <cassert>
#include <iostream>

InsertDataConfig create_test_config() {
    InsertDataConfig config;
    
    // Configure columns for generator mode
    config.source.columns.source_type = "generator";
    
    // Setup timestamp strategy
    auto& ts_config = config.source.columns.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{1000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";
    
    // Setup schema
    auto& schema = config.source.columns.generator.schema;
    schema = {
        {"col1", "INT", "random", 1, 100},
        {"col2", "FLOAT", "random", 0.0, 1.0}
    };
    
    // Setup control parameters
    config.control.data_generation.per_table_rows = 100;
    config.control.insert_control.per_request_rows = 10;
    config.target.timestamp_precision = "ms";
    
    return config;
}

void test_init_with_empty_tables() {
    auto config = create_test_config();
    TableDataManager manager(config);
    
    assert(!manager.init({}));
    std::cout << "test_init_with_empty_tables passed.\n";
}

void test_init_with_valid_tables() {
    auto config = create_test_config();
    TableDataManager manager(config);
    
    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    assert(manager.init(table_names));
    assert(manager.current_table() == "test_table_1");
    
    // Verify table states initialization
    const auto& states = manager.table_states();
    assert(states.size() == 2);
    assert(states.count("test_table_1") == 1);
    assert(states.count("test_table_2") == 1);
    
    std::cout << "test_init_with_valid_tables passed.\n";
}

void test_has_more() {
    auto config = create_test_config();
    config.control.data_generation.per_table_rows = 5;
    TableDataManager manager(config);
    
    std::vector<std::string> table_names = {"test_table"};
    assert(manager.init(table_names));
    
    int rows_generated = 0;
    while (manager.has_more()) {
        auto batch = manager.next_multi_batch();
        assert(batch);
        rows_generated += batch->total_rows;
    }
    
    const auto& states = manager.table_states();
    assert(states.at("test_table").rows_generated == 5);
    
    std::cout << "test_has_more passed.\n";
}

void test_table_completion() {
    auto config = create_test_config();
    config.control.data_generation.per_table_rows = 2;
    TableDataManager manager(config);
    
    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    assert(manager.init(table_names));
    
    while (manager.has_more()) {
        manager.next_multi_batch();
    }
    
    const auto& states = manager.table_states();
    for (const auto& [_, state] : states) {
        assert(state.completed  || state.rows_generated >= config.control.data_generation.per_table_rows);
        assert(state.rows_generated == 2);
    }
    
    std::cout << "test_table_completion passed.\n";
}

void test_data_generation_basic() {
    auto config = create_test_config();
    config.control.data_generation.per_table_rows = 5;
    config.control.data_generation.interlace_mode.enabled = false;
    TableDataManager manager(config);
    
    std::vector<std::string> table_names = {"test_table_1"};
    assert(manager.init(table_names));
    
    // Verify generated data
    int row_count = 0;
    int64_t last_timestamp = 0;
    
    while (auto batch = manager.next_multi_batch()) {
        for (const auto& [table_name, rows] : batch->table_batches) {
            assert(table_name == "test_table_1");
            for (const auto& row : rows) {
                assert(row.timestamp == 1000 + row_count * 10);
                assert(row.columns.size() == 2);
                last_timestamp = row.timestamp;
                row_count++;
            }
        }
    }
    
    assert(row_count == 5);
    assert(last_timestamp == 1040);  // 1000 + (5-1)*10
    
    std::cout << "test_data_generation_basic passed.\n";
}

void test_data_generation_with_interlace() {
    auto config = create_test_config();
    config.control.data_generation.interlace_mode.enabled = true;
    config.control.data_generation.interlace_mode.rows = 2;
    config.control.data_generation.per_table_rows = 4;
    TableDataManager manager(config);
    
    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    assert(manager.init(table_names));
    
    // Verify batch generation with interlace mode
    auto batch = manager.next_multi_batch();
    assert(batch);
    assert(batch->table_batches.size() == 4);
    assert(batch->table_batches[0].second.size() == 2);
    assert(batch->table_batches[1].second.size() == 2);
    assert(batch->table_batches[2].second.size() == 2);
    assert(batch->table_batches[3].second.size() == 2);
    
    // Verify timestamps of first batch
    assert(batch->table_batches[0].second[0].timestamp == 1000);
    assert(batch->table_batches[0].second[1].timestamp == 1010);
    assert(batch->table_batches[1].second[0].timestamp == 1000);
    assert(batch->table_batches[1].second[1].timestamp == 1010);
    assert(batch->table_batches[2].second[0].timestamp == 1020);
    assert(batch->table_batches[2].second[1].timestamp == 1030);
    assert(batch->table_batches[3].second[0].timestamp == 1020);
    assert(batch->table_batches[3].second[1].timestamp == 1030);


    // Verify no more data available
    auto batch2 = manager.next_multi_batch();
    assert(!batch2);
    assert(!manager.has_more());
    
    std::cout << "test_data_generation_with_interlace passed.\n";
}

void test_per_request_rows_limit() {
    auto config = create_test_config();
    config.control.insert_control.per_request_rows = 3;
    config.control.data_generation.per_table_rows = 10;
    config.control.data_generation.interlace_mode.enabled = false;
    TableDataManager manager(config);
    
    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    assert(manager.init(table_names));
    
    // Count total rows across all batches
    int total_rows = 0;
    while (manager.has_more()) {
        auto batch = manager.next_multi_batch();
        assert(batch);
        assert(batch->total_rows <= 3);
        total_rows += batch->total_rows;
    }
    
    assert(total_rows == 20);
    
    std::cout << "test_per_request_rows_limit passed.\n";
}

void test_data_generation_with_flow_control() {
    auto config = create_test_config();
    config.control.data_generation.flow_control.enabled = true;
    config.control.data_generation.flow_control.rate_limit = 100;  // 100 rows per second
    config.control.data_generation.per_table_rows = 5;
    TableDataManager manager(config);
    
    std::vector<std::string> table_names = {"test_table_1"};
    assert(manager.init(table_names));
    
    auto start_time = std::chrono::steady_clock::now();
    
    while (auto batch = manager.next_multi_batch()) {
        for (const auto& [table_name, rows] : batch->table_batches) {
            assert(table_name == "test_table_1");
            assert(!rows.empty());
        }
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    
    // With rate limit of 100 rows/s, 5 rows should take at least 50ms
    assert(duration_ms >= 50);
    
    std::cout << "test_data_generation_with_flow_control passed.\n";
}

int main() {
    test_init_with_empty_tables();
    test_init_with_valid_tables();
    test_init_with_valid_tables();
    test_has_more();
    test_table_completion();
    test_data_generation_basic();
    test_data_generation_with_interlace();
    test_per_request_rows_limit();
    test_data_generation_with_flow_control();
    
    std::cout << "All tests passed.\n";
    return 0;
}