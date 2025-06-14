#include "TableNameManager.h"
#include <cassert>
#include <iostream>

void test_table_name_generation_with_generator() {
    InsertDataConfig config;
    config.source.table_name.source_type = "generator";
    config.source.table_name.generator.prefix = "test_table_";
    config.source.table_name.generator.count = 10;
    config.source.table_name.generator.from = 1;
    
    TableNameManager manager(config);
    auto names = manager.generate_table_names();
    
    assert(names.size() == 10);
    assert(names[0] == "test_table_1");
    assert(names[9] == "test_table_10");
    
    std::cout << "test_table_name_generation_with_generator passed.\n";
}

void test_index_range_split() {
    InsertDataConfig config;
    config.source.table_name.source_type = "generator";
    config.source.table_name.generator.prefix = "test_";
    config.source.table_name.generator.count = 10;
    config.control.data_generation.generate_threads = 3;
    config.control.insert_control.thread_allocation = "index_range";
    
    TableNameManager manager(config);
    auto split_result = manager.split_for_threads();
    
    assert(split_result.size() == 3);
    assert(split_result[0].size() == 4);  // 10/3 rounded up
    assert(split_result[1].size() == 3);
    assert(split_result[2].size() == 3);
    
    std::cout << "test_index_range_split passed.\n";
}

int main() {
    test_table_name_generation_with_generator();
    test_index_range_split();
    
    std::cout << "All tests passed.\n";
    return 0;
}