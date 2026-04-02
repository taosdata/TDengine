#include "MemoryPool.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

void test_memory_pool_basic() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    col_instances.emplace_back(ColumnConfig{"col2", "VARCHAR(16)"});

    MemoryPool pool(1, 1, 4, col_instances, tag_instances);

    auto* block = pool.acquire_block();
    assert(block != nullptr);
    assert(block->tables.size() == 1);
    assert(block->tables[0].columns.size() == 2);

    // Write a row of data
    RowData row;
    row.timestamp = 1234567890;
    row.columns = {int32_t(42), std::string("hello")};
    block->tables[0].add_row(row);

    // Verify the write
    assert(block->tables[0].timestamps[0] == 1234567890);
    auto& col1 = block->tables[0].columns[0];
    int32_t* col1_data = static_cast<int32_t*>(col1.fixed_data);
    (void)col1_data;
    assert(col1_data[0] == 42);
    auto& col2 = block->tables[0].columns[1];
    std::string str(col2.var_data, col2.lengths[0]);
    assert(str == "hello");

    // Test get_column_cell
    ColumnType cell0 = block->tables[0].get_column_cell(0, 0);
    assert(std::get<int32_t>(cell0) == 42);
    ColumnType cell1 = block->tables[0].get_column_cell(0, 1);
    assert(std::get<std::string>(cell1) == "hello");

    // Test get_column_cell_as_string
    std::string cell0_str = block->tables[0].get_column_cell_as_string(0, 0);
    std::string cell1_str = block->tables[0].get_column_cell_as_string(0, 1);
    assert(cell0_str.find("42") != std::string::npos);
    assert(cell1_str == "hello");

    pool.release_block(block);

    std::cout << "test_memory_pool_basic passed." << std::endl;
}


void test_memory_pool_multi_batch() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    // Define Data Columns
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    col_instances.emplace_back(ColumnConfig{"col2", "VARCHAR(8)"});

    // Define Tag Columns
    tag_instances.emplace_back(ColumnConfig{"tag1", "INT"});
    tag_instances.emplace_back(ColumnConfig{"tag2", "VARCHAR(16)"});

    MemoryPool pool(1, 2, 2, col_instances, tag_instances);

    // Register tags for tables t1 and t2
    std::vector<ColumnType> t1_tags = {int32_t(100), std::string("tag_value_1")};
    std::vector<ColumnType> t2_tags = {int32_t(200), std::string("tag_value_2")};

    pool.register_table_tags("t1", t1_tags);
    pool.register_table_tags("t2", t2_tags);

    MultiBatch batch;
    batch.table_batches.push_back({"t1", { {1000, {1, std::string("a")}}, {1010, {2, std::string("b")}} }});
    batch.table_batches.push_back({"t2", { {1000, {3, std::string("c")}}, {1010, {4, std::string("d")}} }});
    batch.update_metadata();

    auto* block = pool.convert_to_memory_block(std::move(batch));
    assert(block != nullptr);
    assert(block->used_tables == 2);

    if (block->used_tables >= 1) block->tables[0].tags_ptr = pool.get_table_tags("t1");
    if (block->used_tables >= 2) block->tables[1].tags_ptr = pool.get_table_tags("t2");

    // Validate table 1
    const auto& t1 = block->tables[0];
    assert(std::string(t1.table_name) == "t1");
    assert(t1.used_rows == 2);
    int32_t* t1_col1 = static_cast<int32_t*>(t1.columns[0].fixed_data);
    (void)t1_col1;
    assert(t1_col1[0] == 1 && t1_col1[1] == 2);
    std::string t1_col2_0(t1.columns[1].var_data + t1.columns[1].var_offsets[0], t1.columns[1].lengths[0]);
    std::string t1_col2_1(t1.columns[1].var_data + t1.columns[1].var_offsets[1], t1.columns[1].lengths[1]);
    assert(t1_col2_0 == "a" && t1_col2_1 == "b");

    // Validate get_column_cell and get_column_cell_as_string
    assert(std::get<int32_t>(t1.get_column_cell(0, 0)) == 1);
    assert(std::get<int32_t>(t1.get_column_cell(1, 0)) == 2);
    assert(std::get<std::string>(t1.get_column_cell(0, 1)) == "a");
    assert(std::get<std::string>(t1.get_column_cell(1, 1)) == "b");
    assert(t1.get_column_cell_as_string(0, 0) == "1");
    assert(t1.get_column_cell_as_string(1, 0) == "2");
    assert(t1.get_column_cell_as_string(0, 1) == "a");
    assert(t1.get_column_cell_as_string(1, 1) == "b");

    // Validate Tags for t1
    assert(t1.tags_ptr != nullptr);
    assert(std::get<int32_t>(t1.get_tag_cell(0, 0)) == 100);
    assert(std::get<std::string>(t1.get_tag_cell(0, 1)) == "tag_value_1");
    assert(t1.get_tag_cell_as_string(0, 0) == "100");
    assert(t1.get_tag_cell_as_string(0, 1) == "tag_value_1");

    // Validate table 2
    const auto& t2 = block->tables[1];
    assert(std::string(t2.table_name) == "t2");
    assert(t2.used_rows == 2);
    int32_t* t2_col1 = static_cast<int32_t*>(t2.columns[0].fixed_data);
    (void)t2_col1;
    assert(t2_col1[0] == 3 && t2_col1[1] == 4);
    std::string t2_col2_0(t2.columns[1].var_data + t2.columns[1].var_offsets[0], t2.columns[1].lengths[0]);
    std::string t2_col2_1(t2.columns[1].var_data + t2.columns[1].var_offsets[1], t2.columns[1].lengths[1]);
    assert(t2_col2_0 == "c" && t2_col2_1 == "d");

    // Validate get_column_cell and get_column_cell_as_string
    assert(std::get<int32_t>(t2.get_column_cell(0, 0)) == 3);
    assert(std::get<int32_t>(t2.get_column_cell(1, 0)) == 4);
    assert(std::get<std::string>(t2.get_column_cell(0, 1)) == "c");
    assert(std::get<std::string>(t2.get_column_cell(1, 1)) == "d");
    assert(t2.get_column_cell_as_string(0, 0) == "3");
    assert(t2.get_column_cell_as_string(1, 0) == "4");
    assert(t2.get_column_cell_as_string(0, 1) == "c");
    assert(t2.get_column_cell_as_string(1, 1) == "d");

    // Validate Tags for t2
    assert(t2.tags_ptr != nullptr);
    assert(std::get<int32_t>(t2.get_tag_cell(0, 0)) == 200);
    assert(std::get<std::string>(t2.get_tag_cell(0, 1)) == "tag_value_2");
    assert(t2.get_tag_cell_as_string(0, 0) == "200");
    assert(t2.get_tag_cell_as_string(0, 1) == "tag_value_2");

    pool.release_block(block);

    std::cout << "test_memory_pool_multi_batch passed." << std::endl;
}

void test_memory_pool_acquire_release() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    MemoryPool pool(1, 1, 1, col_instances, tag_instances);

    auto* block1 = pool.acquire_block();
    assert(block1 != nullptr);
    pool.release_block(block1);

    auto* block2 = pool.acquire_block();
    assert(block2 == block1); // Should be the same block reused
    pool.release_block(block2);

    std::cout << "test_memory_pool_acquire_release passed." << std::endl;
}

void test_memory_pool_get_cell_out_of_range() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    MemoryPool pool(1, 1, 1, col_instances, tag_instances);

    auto* block = pool.acquire_block();
    RowData row;
    row.timestamp = 1;
    row.columns = {int32_t(1)};
    block->tables[0].add_row(row);

    bool caught = false;
    try {
        block->tables[0].get_column_cell(1, 0); // row_index out of range
    } catch (const std::out_of_range&) {
        caught = true;
    }
    (void)caught;
    assert(caught);

    caught = false;
    try {
        block->tables[0].get_column_cell(0, 1); // col_index out of range
    } catch (const std::out_of_range&) {
        caught = true;
    }
    assert(caught);

    pool.release_block(block);
    std::cout << "test_memory_pool_get_cell_out_of_range passed." << std::endl;
}

void test_memory_pool_get_cell_null() {
    std::cout << "test_memory_pool_get_cell_null skipped (not implemented)." << std::endl;
}

void test_memory_pool_tables_reuse_data() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    col_instances.emplace_back(ColumnConfig{"col2", "VARCHAR(8)"});

    // tables_reuse_data = true
    MemoryPool pool(1, 2, 2, col_instances, tag_instances, true);

    auto* block = pool.acquire_block();
    assert(block != nullptr);
    assert(block->tables.size() == 2);

    // Write data to table 1
    RowData row1;
    row1.timestamp = 100;
    row1.columns = {int32_t(11), std::string("foo")};
    block->tables[0].add_row(row1);

    // Write data to table 2
    RowData row2;
    row2.timestamp = 200;
    row2.columns = {int32_t(22), std::string("bar")};
    block->tables[1].add_row(row2);

    // Check that table 1 and table 2 share the same data pointers
    assert(block->tables[0].columns[0].fixed_data == block->tables[1].columns[0].fixed_data);
    assert(block->tables[0].columns[1].var_data == block->tables[1].columns[1].var_data);

    // Check that data is overwritten due to shared memory
    // Table 1 row 0
    assert(std::get<int32_t>(block->tables[0].get_column_cell(0, 0)) == 22);
    assert(std::get<std::string>(block->tables[0].get_column_cell(0, 1)) == "bar");

    // Table 2 row 0
    assert(std::get<int32_t>(block->tables[1].get_column_cell(0, 0)) == 22);
    assert(std::get<std::string>(block->tables[1].get_column_cell(0, 1)) == "bar");

    // Write another row to table 1
    RowData row3;
    row3.timestamp = 300;
    row3.columns = {int32_t(33), std::string("baz")};
    block->tables[0].add_row(row3);

    // Check that table 2 row count is not affected
    assert(block->tables[1].used_rows == 1);

    pool.release_block(block);

    std::cout << "test_memory_pool_tables_reuse_data passed." << std::endl;
}

void test_memory_pool_cache_mode_basic() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    col_instances.emplace_back(ColumnConfig{"col2", "VARCHAR(16)"});

    const size_t num_blocks = 3;
    const size_t num_cache_units = 2;
    const size_t num_tables = 2;
    const size_t num_rows = 4;
    MemoryPool pool(num_blocks, num_tables, num_rows, col_instances, tag_instances, false, num_cache_units);

    assert(pool.is_cache_mode());
    assert(pool.get_cache_units_count() == 2);

    // Pre-fill cache data
    std::vector<RowData> cache_data_0, cache_data_1;
    for (size_t i = 0; i < num_rows; ++i) {
        RowData row0, row1;
        row0.timestamp = 0;
        row1.timestamp = 0;
        row0.columns = {int32_t(i * 10), std::string("cache0_t0_" + std::to_string(i))};
        row1.columns = {int32_t(i * 100), std::string("cache0_t1_" + std::to_string(i))};
        cache_data_0.push_back(row0);
        cache_data_1.push_back(row1);
    }

    bool fill_result = pool.fill_cache_unit_data(0, 0, cache_data_0);
    (void)fill_result;
    assert(fill_result);
    assert(!pool.is_cache_unit_prefilled(0));

    fill_result = pool.fill_cache_unit_data(0, 1, cache_data_1);
    assert(fill_result);
    assert(pool.is_cache_unit_prefilled(0));

    // Acquire memory block and use cache data
    auto* block = pool.acquire_block(0);
    assert(block != nullptr);
    assert(block->cache_index == 0);

    // Write timestamp data
    for (size_t i = 0; i < num_rows; ++i) {
        RowData row;
        row.timestamp = (i + 1) * 1000;
        row.columns = {int32_t(i * 999), std::string("should_be_ignored")};
        block->tables[0].add_row(row);
        block->tables[1].add_row(row);
    }

    assert(block->tables[0].used_rows == num_rows);
    assert(block->tables[1].used_rows == num_rows);

    // Verify column data
    for (size_t i = 0; i < num_rows; ++i) {
        assert(block->tables[0].timestamps[i] == static_cast<int64_t>((i + 1) * 1000));
        assert(block->tables[1].timestamps[i] == static_cast<int64_t>((i + 1) * 1000));

        assert(std::get<int32_t>(block->tables[0].get_column_cell(i, 0)) == int32_t(i * 10));
        assert(std::get<std::string>(block->tables[0].get_column_cell(i, 1)) == std::string("cache0_t0_" + std::to_string(i)));

        assert(std::get<int32_t>(block->tables[1].get_column_cell(i, 0)) == int32_t(i * 100));
        assert(std::get<std::string>(block->tables[1].get_column_cell(i, 1)) == std::string("cache0_t1_" + std::to_string(i)));
    }

    pool.release_block(block);
    std::cout << "test_memory_pool_cache_mode_basic passed." << std::endl;
}

void test_memory_pool_cache_mode_shared() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    col_instances.emplace_back(ColumnConfig{"col2", "VARCHAR(16)"});

    const size_t num_blocks = 3;
    const size_t num_cache_units = 2;
    const size_t num_tables = 2;
    const size_t num_rows = 4;
    MemoryPool pool(num_blocks, num_tables, num_rows, col_instances, tag_instances, false, num_cache_units);

    // Pre-fill cache data
    for (size_t c = 0; c < num_cache_units; ++c) {
        for (size_t t = 0; t < num_tables; ++t) {
            std::vector<RowData> cache_data;
            for (size_t i = 0; i < num_rows; ++i) {
                RowData row;
                row.timestamp = 0;
                row.columns = {int32_t(i * 10 + c * 100), std::string("cache" + std::to_string(c) + "_t" + std::to_string(t) + "_" + std::to_string(i))};
                cache_data.push_back(row);
            }
            bool fill_result = pool.fill_cache_unit_data(c, t, cache_data);
            (void)fill_result;
            assert(fill_result);
        }
    }

    auto* block0 = pool.acquire_block(0);
    auto* block1 = pool.acquire_block(1);
    auto* block2 = pool.acquire_block(2);

    // Write timestamp data
    for (size_t i = 0; i < num_rows; ++i) {
        RowData row;
        row.timestamp = 1000 + i;
        row.columns = {int32_t(999), std::string("ignored")};
        block0->tables[0].add_row(row);

        row.timestamp = 2000 + i;
        block1->tables[0].add_row(row);

        row.timestamp = 3000 + i;
        block2->tables[0].add_row(row);
    }

    for (size_t i = 0; i < num_rows; ++i) {
        assert(block0->tables[0].timestamps[i] == static_cast<int64_t>(1000 + i));
        assert(block1->tables[0].timestamps[i] == static_cast<int64_t>(2000 + i));
        assert(block2->tables[0].timestamps[i] == static_cast<int64_t>(3000 + i));

        assert(std::get<int32_t>(block0->tables[0].get_column_cell(i, 0)) == int32_t(i * 10));
        assert(std::get<int32_t>(block1->tables[0].get_column_cell(i, 0)) == int32_t(i * 10 + 100));
        assert(std::get<int32_t>(block2->tables[0].get_column_cell(i, 0)) == int32_t(i * 10));

        assert(std::get<std::string>(block0->tables[0].get_column_cell(i, 1)) == "cache0_t0_" + std::to_string(i));
        assert(std::get<std::string>(block1->tables[0].get_column_cell(i, 1)) == "cache1_t0_" + std::to_string(i));
        assert(std::get<std::string>(block2->tables[0].get_column_cell(i, 1)) == "cache0_t0_" + std::to_string(i));
    }

    pool.release_block(block0);
    pool.release_block(block1);
    pool.release_block(block2);

    std::cout << "test_memory_pool_cache_mode_shared passed." << std::endl;
}

void test_memory_pool_cache_mode_edge_cases() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});

    // Test 1: cache unit count is 0
    MemoryPool pool_no_cache(2, 1, 4, col_instances, tag_instances, false, 0);
    assert(!pool_no_cache.is_cache_mode());
    assert(pool_no_cache.get_cache_units_count() == 0);

    // Test 2: invalid cache index
    MemoryPool pool(2, 1, 4, col_instances, tag_instances, false, 1);
    bool result = pool.fill_cache_unit_data(999, 0, {});
    (void)result;
    assert(!result);

    result = pool.fill_cache_unit_data(0, 999, {});
    assert(!result);

    // Test 3: empty data fill
    result = pool.fill_cache_unit_data(0, 0, {});
    assert(result);

    std::cout << "test_memory_pool_cache_mode_edge_cases passed." << std::endl;
}

void test_memory_pool_cache_mode_performance() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    col_instances.emplace_back(ColumnConfig{"col2", "VARCHAR(32)"});

    const size_t num_blocks = 10;
    const size_t num_cache_units = 3;
    const size_t num_tables = 2;
    const size_t num_rows = 1000;
    MemoryPool pool(num_blocks, num_tables, num_rows, col_instances, tag_instances, false, num_cache_units);

    // Pre-fill cache data
    std::vector<RowData> cache_data;
    for (size_t i = 0; i < num_rows; ++i) {
        RowData row;
        row.timestamp = 0;
        row.columns = {int32_t(i), std::string("data_" + std::to_string(i))};
        cache_data.push_back(row);
    }

    for (size_t i = 0; i < num_cache_units; ++i) {
        for (size_t t = 0; t < num_tables; ++t) {
            bool fill_result = pool.fill_cache_unit_data(i, t, cache_data);
            (void)fill_result;
            assert(fill_result);
        }
    }

    std::atomic<int> completed_threads{0};
    const int num_threads = 4;
    const int iterations = 100;

    auto worker = [&](int thread_id) {
        for (int i = 0; i < iterations; ++i) {
            auto sequence_num = thread_id * iterations + i;
            auto* block = pool.acquire_block(sequence_num);

            if (block) {
                for (size_t j = 0; j < num_rows; ++j) {
                    RowData row;
                    row.timestamp = sequence_num * 100 + j;
                    row.columns = {int32_t(999), std::string("ignored")};
                    for (size_t t = 0; t < num_tables; ++t) {
                        block->tables[t].add_row(row);
                    }
                }

                for (size_t t = 0; t < num_tables; ++t) {
                    assert(block->tables[t].used_rows == num_rows);
                    for (size_t j = 0; j < num_rows; ++j) {
                        assert(block->tables[t].timestamps[j] == static_cast<int64_t >(sequence_num * 100 + j));
                        assert(std::get<int32_t>(block->tables[t].get_column_cell(j, 0)) == static_cast<int32_t>(j));
                        assert(std::get<std::string>(block->tables[t].get_column_cell(j, 1)) == "data_" + std::to_string(j));
                    }
                }
                pool.release_block(block);
            }
        }
        completed_threads++;
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    for (auto& t : threads) {
        t.join();
    }

    assert(completed_threads == num_threads);
    std::cout << "test_memory_pool_cache_mode_performance passed." << std::endl;
}

void test_memory_pool_cache_mode_with_reuse() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    col_instances.emplace_back(ColumnConfig{"col2", "VARCHAR(16)"});

    const size_t num_blocks = 2;
    const size_t num_cache_units = 1;
    const size_t num_tables = 2;
    const size_t num_rows = 4;
    MemoryPool pool(num_blocks, num_tables, num_rows, col_instances, tag_instances, true, num_cache_units);

    assert(pool.is_cache_mode());

    // Pre-fill cache data
    std::vector<RowData> cache_data;
    for (size_t i = 0; i < num_rows; ++i) {
        RowData row;
        row.timestamp = 0;
        row.columns = {int32_t(i * 5), std::string("cache_" + std::to_string(i))};
        cache_data.push_back(row);
    }

    pool.fill_cache_unit_data(0, 0, cache_data);
    pool.fill_cache_unit_data(0, 1, cache_data);

    auto* block = pool.acquire_block(0);

    for (size_t t = 0; t < num_tables; ++t) {
        for (size_t i = 0; i < num_rows; ++i) {
            RowData row;
            row.timestamp = (t + 1) * 1000 + i;
            row.columns = {int32_t(99), std::string("ignored")};
            block->tables[t].add_row(row);
        }
    }

    assert(block->tables[0].columns[0].fixed_data == block->tables[1].columns[0].fixed_data);
    assert(std::get<int32_t>(block->tables[0].get_column_cell(0, 0)) == 0);
    assert(std::get<int32_t>(block->tables[1].get_column_cell(0, 0)) == 0);

    for (size_t t = 0; t < num_tables; ++t) {
        assert(block->tables[t].used_rows == num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            assert(block->tables[t].timestamps[i] == static_cast<int64_t >((t + 1) * 1000 + i));
            assert(std::get<int32_t>(block->tables[t].get_column_cell(i, 0)) == int32_t(i * 5));
            assert(std::get<std::string>(block->tables[t].get_column_cell(i, 1)) == std::string("cache_" + std::to_string(i)));
        }
    }

    pool.release_block(block);
    std::cout << "test_memory_pool_cache_mode_with_reuse passed." << std::endl;
}

void test_memory_pool_tags_management() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    tag_instances.emplace_back(ColumnConfig{"tag1", "INT"});

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);

    // Test has_table_tags for non-existent table
    assert(!pool.has_table_tags("t1"));
    assert(pool.get_table_tags("t1") == nullptr);

    // Register tags
    std::vector<ColumnType> t1_tags = {int32_t(100)};
    auto* tags = pool.register_table_tags("t1", t1_tags);
    assert(tags != nullptr);
    assert(std::string(tags->table_name) == "t1");
    assert(pool.has_table_tags("t1"));
    assert(pool.get_table_tags("t1") == tags);

    // Test duplicate registration
    auto* tags_dup = pool.register_table_tags("t1", t1_tags);
    assert(tags_dup == nullptr); // Should fail

    // Test invalid tag count
    std::vector<ColumnType> invalid_tags = {};
    auto* tags_invalid = pool.register_table_tags("t2", invalid_tags);
    assert(tags_invalid == nullptr);
    assert(!pool.has_table_tags("t2"));

    (void)tags;
    (void)tags_dup;
    (void)tags_invalid;

    std::cout << "test_memory_pool_tags_management passed." << std::endl;
}

void test_memory_pool_tags_edge_cases() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    tag_instances.emplace_back(ColumnConfig{"tag1", "INT"});

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.acquire_block();

    // Case 1: Access tags when none assigned to the table
    bool caught = false;
    try {
        block->tables[0].get_tag_cell(0, 0);
    } catch (const std::runtime_error& e) {
        caught = true;
        // Verify error message contains expected text
        assert(std::string(e.what()).find("No tags available") != std::string::npos);
    }
    assert(caught);
    (void)caught;

    // Case 2: Assign tags, then access out of range
    std::vector<ColumnType> t1_tags = {int32_t(100)};
    pool.register_table_tags("t1", t1_tags);
    block->tables[0].tags_ptr = pool.get_table_tags("t1");

    caught = false;
    try {
        block->tables[0].get_tag_cell(0, 1);
    } catch (const std::out_of_range&) {
        caught = true;
    }
    assert(caught);

    pool.release_block(block);
    std::cout << "test_memory_pool_tags_edge_cases passed." << std::endl;
}

void test_memory_pool_bind_verification() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    tag_instances.emplace_back(ColumnConfig{"tag1", "INT"});

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);

    // Register tags
    std::vector<ColumnType> t1_tags = {int32_t(100)};
    pool.register_table_tags("t1", t1_tags);

    auto* block = pool.acquire_block();

    // Simulate data usage
    RowData row;
    row.timestamp = 12345;
    row.columns = {int32_t(1)};
    block->tables[0].table_name = "t1";
    block->tables[0].tags_ptr = pool.get_table_tags("t1");
    block->tables[0].add_row(row);
    block->used_tables = 1;

    // Trigger bind construction
    block->build_bindv();

    // Verify Bind Structure
    assert(block->bindv_.tags != nullptr);

    // Verify the first table's tag bind pointer
    TAOS_STMT2_BIND* t1_tag_binds = block->bindv_.tags[0];
    assert(t1_tag_binds != nullptr);

    // Verify the tag value inside the bind structure
    // tag1 is INT (fixed length), so buffer points to int32_t
    int32_t* tag_val_ptr = static_cast<int32_t*>(const_cast<void*>(t1_tag_binds[0].buffer));
    assert(tag_val_ptr != nullptr);
    assert(*tag_val_ptr == 100);

    // Verify column bind structure
    assert(block->bindv_.bind_cols != nullptr);
    TAOS_STMT2_BIND* t1_col_binds = block->bindv_.bind_cols[0];

    // Index 0 is timestamp, Index 1 is col1
    assert(t1_col_binds[1].buffer_type == TSDB_DATA_TYPE_INT);
    int32_t* col_val_ptr = static_cast<int32_t*>(const_cast<void*>(t1_col_binds[1].buffer));
    assert(col_val_ptr != nullptr);
    assert(col_val_ptr[0] == 1);

    (void)tag_val_ptr;
    (void)col_val_ptr;

    pool.release_block(block);
    std::cout << "test_memory_pool_bind_verification passed." << std::endl;
}

int main() {
    test_memory_pool_basic();
    test_memory_pool_multi_batch();
    test_memory_pool_acquire_release();
    test_memory_pool_get_cell_out_of_range();
    test_memory_pool_get_cell_null();
    test_memory_pool_tables_reuse_data();

    test_memory_pool_cache_mode_basic();
    test_memory_pool_cache_mode_shared();
    test_memory_pool_cache_mode_edge_cases();
    test_memory_pool_cache_mode_performance();
    test_memory_pool_cache_mode_with_reuse();

    test_memory_pool_tags_management();
    test_memory_pool_tags_edge_cases();
    test_memory_pool_bind_verification();

    std::cout << "All MemoryPool tests passed." << std::endl;
    return 0;
}