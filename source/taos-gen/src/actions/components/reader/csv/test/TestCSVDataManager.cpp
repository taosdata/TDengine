#include "CSVDataManager.hpp"
#include <iostream>
#include <cassert>
#include <fstream>
#include <thread>
#include <vector>
#include <numeric>


// Helper to create a dummy CSV file for tests
void create_test_file(const std::string& filename, const std::string& content) {
    std::ofstream test_file(filename);
    test_file << content;
    test_file.close();
}

void test_manager_singleton_and_reset() {
    // Test that reset() clears all caches
    const std::string filename = "reset_test.csv";
    create_test_file(filename, "col1\nval1");

    ColumnsCSV config;
    config.file_path = filename;
    config.has_header = true;
    config.tbname_index = -1;
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    ColumnConfigVector col_configs = {{"col1", "varchar(20)"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    // Get table data first time
    auto [using_default1, data1] = CSVDataManager::get_table_data(config, instances, "any_table");
    (void)using_default1;
    (void)data1;
    assert(using_default1 && "Should use default_table");
    assert(data1 != nullptr);

    // Reset the manager
    CSVDataManager::reset();

    // Get table data again and verify it still works
    auto [using_default2, data2] = CSVDataManager::get_table_data(config, instances, "any_table");
    (void)using_default2;
    (void)data2;
    assert(using_default2 && "Should use default_table after reset");
    assert(data2 != nullptr);

    std::remove(filename.c_str());
    std::cout << "test_manager_singleton_and_reset passed\n";
}

void test_get_table_data_with_specific_table() {
    CSVDataManager::reset();
    const std::string filename = "specific_table.csv";
    create_test_file(filename, "tbname,value\ntable1,100\ntable2,200");

    ColumnsCSV config;
    config.file_path = filename;
    config.has_header = true;
    config.tbname_index = 0;
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    ColumnConfigVector col_configs = {{"value", "int"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    // Find specific table
    auto [using_default1, data1] = CSVDataManager::get_table_data(config, instances, "table1");
    (void)using_default1;
    (void)data1;
    assert(!using_default1 && "Should not use default_table");
    assert(data1 != nullptr);
    assert(data1->table_name == "table1");
    assert(data1->rows.size() == 1);

    // Find another table
    auto [using_default2, data2] = CSVDataManager::get_table_data(config, instances, "table2");
    (void)using_default2;
    (void)data2;
    assert(!using_default2 && "Should not use default_table");
    assert(data2 != nullptr);
    assert(data2->table_name == "table2");

    std::remove(filename.c_str());
    std::cout << "test_get_table_data_with_specific_table passed\n";
}

void test_get_table_data_fallback_to_default() {
    CSVDataManager::reset();
    const std::string filename = "fallback.csv";
    create_test_file(filename, "value\n100\n200");

    ColumnsCSV config;
    config.file_path = filename;
    config.has_header = true;
    config.tbname_index = -1;
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    ColumnConfigVector col_configs = {{"value", "int"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    // Request non-existent table, should fallback to default_table
    auto [using_default, data] = CSVDataManager::get_table_data(config, instances, "non_existent_table");
    (void)using_default;
    (void)data;
    assert(using_default && "Should fallback to default_table");
    assert(data != nullptr);
    assert(data->table_name == DEFAULT_TABLE_NAME);
    assert(data->rows.size() == 2);

    std::remove(filename.c_str());
    std::cout << "test_get_table_data_fallback_to_default passed\n";
}

void test_get_table_data_not_found() {
    CSVDataManager::reset();
    const std::string filename = "not_found.csv";
    create_test_file(filename, "tbname,value\ntable1,100");

    ColumnsCSV config;
    config.file_path = filename;
    config.has_header = true;
    config.tbname_index = 0;
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    ColumnConfigVector col_configs = {{"value", "int"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    // Request non-existent table
    auto [using_default, data] = CSVDataManager::get_table_data(config, instances, "non_existent");
    (void)using_default;
    (void)data;
    assert(!using_default && "Should not fallback");
    assert(data == nullptr && "Should return nullptr for non-existent table");

    std::remove(filename.c_str());
    std::cout << "test_get_table_data_not_found passed\n";
}

void test_get_shared_rows_cache() {
    CSVDataManager::reset();
    const std::string filename = "shared_rows.csv";
    create_test_file(filename, "ts,value\n1000,100\n2000,200\n3000,300");

    ColumnsCSV config;
    config.file_path = filename;
    config.has_header = true;
    config.tbname_index = -1;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.timestamp_strategy.csv.timestamp_precision = "ms";

    ColumnConfigVector col_configs = {{"value", "int"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    // Get table data first
    auto [using_default, table_data] = CSVDataManager::get_table_data(config, instances, "any_table");
    assert(using_default && table_data != nullptr);

    // Get shared rows first time
    auto shared_rows1 = CSVDataManager::get_shared_rows(
        config.file_path,
        *table_data,
        "ms",
        "us"
    );
    (void)shared_rows1;
    assert(shared_rows1 != nullptr);
    assert(shared_rows1->size() == 3);
    // Verify timestamp conversion from ms to us
    assert((*shared_rows1)[0].timestamp == 1000000);  // 1000ms -> 1000000us
    assert(std::get<int32_t>((*shared_rows1)[0].columns[0]) == 100);

    // Get shared rows second time - should hit cache
    auto shared_rows2 = CSVDataManager::get_shared_rows(
        config.file_path,
        *table_data,
        "ms",
        "us"
    );
    (void)shared_rows2;
    assert(shared_rows1 == shared_rows2 && "Should return the same shared_ptr (cache hit)");

    // Get with different precision - should create new cache entry
    auto shared_rows3 = CSVDataManager::get_shared_rows(
        config.file_path,
        *table_data,
        "ms",
        "ns"
    );
    (void)shared_rows3;
    assert(shared_rows1 != shared_rows3 && "Different precision should create different cache");
    assert((*shared_rows3)[0].timestamp == 1000000000);  // 1000ms -> 1000000000ns
    assert(std::get<int32_t>((*shared_rows3)[0].columns[0]) == 100);

    std::remove(filename.c_str());
    std::cout << "test_get_shared_rows_cache passed\n";
}

void test_shared_rows_multiple_tables_share_cache() {
    CSVDataManager::reset();
    const std::string filename = "multi_tables.csv";
    create_test_file(filename, "ts,value\n1000,100\n2000,200");

    ColumnsCSV config;
    config.file_path = filename;
    config.has_header = true;
    config.tbname_index = -1;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.timestamp_strategy.csv.timestamp_precision = "ms";

    ColumnConfigVector col_configs = {{"value", "int"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    // Simulate three different subtables accessing the same CSV default_table
    auto [using_default1, table_data1] = CSVDataManager::get_table_data(config, instances, "subtable_0001");
    auto shared_rows1 = CSVDataManager::get_shared_rows(config.file_path, *table_data1, "ms", "us");

    auto [using_default2, table_data2] = CSVDataManager::get_table_data(config, instances, "subtable_0002");
    auto shared_rows2 = CSVDataManager::get_shared_rows(config.file_path, *table_data2, "ms", "us");

    auto [using_default3, table_data3] = CSVDataManager::get_table_data(config, instances, "subtable_0003");
    auto shared_rows3 = CSVDataManager::get_shared_rows(config.file_path, *table_data3, "ms", "us");

    // All three should get the exact same shared_ptr (cache hit)
    (void)using_default1;
    (void)using_default2;
    (void)using_default3;
    assert(using_default1 && using_default2 && using_default3 && "All should use default_table");

    (void)shared_rows1;
    (void)shared_rows2;
    (void)shared_rows3;
    assert(shared_rows2 == shared_rows3 && "Subtable 2 and 3 should share cache");
    assert(shared_rows1 == shared_rows2 && "Subtable 1 and 2 should share cache");
    assert(shared_rows2 == shared_rows3 && "Subtable 2 and 3 should share cache");

    assert(shared_rows1->size() == 2);
    assert((*shared_rows1)[0].timestamp == 1000000);
    assert((*shared_rows1)[1].timestamp == 2000000);
    assert(std::get<int32_t>((*shared_rows1)[0].columns[0]) == 100);
    assert(std::get<int32_t>((*shared_rows1)[1].columns[0]) == 200);

    std::remove(filename.c_str());
    std::cout << "test_shared_rows_multiple_tables_share_cache passed\n";
}

void test_different_files_different_caches() {
    CSVDataManager::reset();
    const std::string file1 = "file1.csv";
    const std::string file2 = "file2.csv";
    create_test_file(file1, "ts,value\n1000,100");
    create_test_file(file2, "ts,value\n2000,200");

    ColumnsCSV config1;
    config1.file_path = file1;
    config1.has_header = true;
    config1.tbname_index = -1;
    config1.timestamp_strategy.strategy_type = "csv";
    config1.timestamp_strategy.csv.timestamp_index = 0;
    config1.timestamp_strategy.csv.timestamp_precision = "ms";

    ColumnsCSV config2 = config1;
    config2.file_path = file2;

    ColumnConfigVector col_configs = {{"value", "int"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    auto [_, data1] = CSVDataManager::get_table_data(config1, instances, "table");
    assert(data1 != nullptr);
    auto shared_rows1 = CSVDataManager::get_shared_rows(config1.file_path, *data1, "ms", "us");

    auto [__, data2] = CSVDataManager::get_table_data(config2, instances, "table");
    assert(data2 != nullptr);
    auto shared_rows2 = CSVDataManager::get_shared_rows(config2.file_path, *data2, "ms", "us");

    // Different files should have different caches
    assert(shared_rows1 != shared_rows2 && "Different files should not share cache");
    assert((*shared_rows1)[0].timestamp == 1000000);
    assert((*shared_rows2)[0].timestamp == 2000000);

    std::remove(file1.c_str());
    std::remove(file2.c_str());
    std::cout << "test_different_files_different_caches passed\n";
}

void test_cache_propagates_error() {
    CSVDataManager::reset();
    const std::string filename = "error.csv";
    create_test_file(filename, "name,value\nAlice,not-an-int");

    ColumnsCSV config;
    config.file_path = filename;
    config.has_header = true;
    config.timestamp_strategy.generator = TimestampGeneratorConfig{};

    ColumnConfigVector col_configs = {{"name", "varchar(20)"}, {"value", "int"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    try {
        CSVDataManager::get_table_data(config, instances, "any_table");
        assert(false && "Expected an exception for invalid data format");
    } catch (const std::runtime_error& e) {
        std::string error_message = e.what();
        assert(error_message.find("Failed to load CSV file") != std::string::npos);
    }

    std::remove(filename.c_str());
    std::cout << "test_cache_propagates_error passed\n";
}

void test_multithreaded_access() {
    CSVDataManager::reset();
    const std::string filename = "multithread.csv";
    create_test_file(filename, "ts,id\n1000,1\n2000,2\n3000,3\n4000,4\n5000,5");

    ColumnsCSV config;
    config.file_path = filename;
    config.has_header = true;
    config.tbname_index = -1;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.timestamp_strategy.csv.timestamp_precision = "ms";

    ColumnConfigVector col_configs = {{"id", "int"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    std::vector<std::thread> threads;
    int num_threads = 10;
    std::vector<CSVDataManager::SharedRows> shared_rows_list(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            // All threads get table data and shared rows for the same file
            auto [using_default, table_data] = CSVDataManager::get_table_data(config, instances, "table_" + std::to_string(i));
            if (table_data) {
                shared_rows_list[i] = CSVDataManager::get_shared_rows(
                    config.file_path,
                    *table_data,
                    "ms",
                    "us"
                );
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Verify all threads got the exact same shared_ptr (cache hit)
    for (size_t i = 1; i < shared_rows_list.size(); ++i) {
        assert(shared_rows_list[0] == shared_rows_list[i] && "All threads should receive the same shared cache");
    }
    assert(shared_rows_list[0]->size() == 5);

    std::remove(filename.c_str());
    std::cout << "test_multithreaded_access passed\n";
}

void test_reset_clears_shared_rows_cache() {
    CSVDataManager::reset();
    const std::string filename = "reset_shared.csv";
    create_test_file(filename, "ts,value\n1000,100");

    ColumnsCSV config;
    config.file_path = filename;
    config.has_header = true;
    config.tbname_index = -1;
    config.timestamp_strategy.strategy_type = "csv";
    config.timestamp_strategy.csv.timestamp_index = 0;
    config.timestamp_strategy.csv.timestamp_precision = "ms";

    ColumnConfigVector col_configs = {{"value", "int"}};
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    // Get shared rows
    auto [_, data1] = CSVDataManager::get_table_data(config, instances, "table");
    assert(data1 != nullptr);
    auto shared_rows1 = CSVDataManager::get_shared_rows(config.file_path, *data1, "ms", "us");
    auto ptr1 = shared_rows1.get();

    // Reset
    CSVDataManager::reset();

    // Get shared rows again - should be a new instance
    auto [__, data2] = CSVDataManager::get_table_data(config, instances, "table");
    assert(data2 != nullptr);
    auto shared_rows2 = CSVDataManager::get_shared_rows(config.file_path, *data2, "ms", "us");
    auto ptr2 = shared_rows2.get();

    (void)ptr1;
    (void)ptr2;
    assert(ptr1 != ptr2 && "Reset should clear shared rows cache");

    std::remove(filename.c_str());
    std::cout << "test_reset_clears_shared_rows_cache passed\n";
}

int main() {
    test_manager_singleton_and_reset();
    test_get_table_data_with_specific_table();
    test_get_table_data_fallback_to_default();
    test_get_table_data_not_found();
    test_get_shared_rows_cache();
    test_shared_rows_multiple_tables_share_cache();
    test_different_files_different_caches();
    test_cache_propagates_error();
    test_multithreaded_access();
    test_reset_clears_shared_rows_cache();

    std::cout << "\nAll CSVDataManager tests passed!\n";
    return 0;
}