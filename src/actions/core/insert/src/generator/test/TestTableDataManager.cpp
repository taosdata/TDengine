#include "TableDataManager.hpp"
#include <cassert>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <unordered_map>

InsertDataConfig create_test_config() {
    InsertDataConfig config;

    // Configure columns for generator mode
    config.schema.columns_cfg.source_type = "generator";

    // Setup timestamp strategy
    auto& ts_config = config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{1000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";

    // Setup schema
    auto& schema = config.schema.columns_cfg.generator.schema;
    schema = {
        {"col1", "INT", "random", 1, 100},
        {"col2", "FLOAT", "random", 0.0, 1.0}
    };

    // Setup control parameters
    config.schema.generation.rows_per_table = 100;
    config.schema.generation.rows_per_batch = 10;
    config.timestamp_precision = "ms";
    config.data_format.support_tags = false;

    return config;
}

void test_init_with_empty_tables() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    assert(!manager.init({}));
    std::cout << "test_init_with_empty_tables passed.\n";
}

void test_init_with_valid_tables() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 2, 1, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    assert(manager.init(table_names));
    assert(manager.current_table() == "test_table_1");

    // Verify table states initialization
    const auto& states = manager.table_states();
    (void)states;
    assert(states.size() == 2);
    assert(states[0].table_name == "test_table_1");
    assert(states[1].table_name == "test_table_2");

    std::cout << "test_init_with_valid_tables passed.\n";
}

void test_has_more() {
    auto config = create_test_config();
    config.schema.generation.rows_per_table = 5;
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 1, 5, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    std::vector<std::string> table_names = {"test_table"};
    assert(manager.init(table_names));

    size_t rows_generated = 0;
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        assert(block);
        rows_generated += block.value()->total_rows;
        block.value()->release();
    }

    const auto& states = manager.table_states();
    (void)states;
    assert(states.size() == 1);
    assert(states[0].rows_generated == 5);
    (void)rows_generated;
    assert(rows_generated == 5);

    std::cout << "test_has_more passed.\n";
}

void test_table_completion() {
    auto config = create_test_config();
    config.schema.generation.rows_per_table = 2;
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 2, 2, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    assert(manager.init(table_names));

    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        assert(block);
        block.value()->release();
    }

    const auto& states = manager.table_states();
    for (const auto& state : states) {
        (void)state;
        assert(state.completed  || state.rows_generated >= config.schema.generation.rows_per_table);
        assert(state.rows_generated == 2);
    }

    std::cout << "test_table_completion passed.\n";
}

void test_data_generation_basic() {
    auto config = create_test_config();
    config.schema.generation.rows_per_table = 5;
    config.schema.generation.interlace_mode.enabled = false;
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 1, 5, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    std::vector<std::string> table_names = {"test_table_1"};
    assert(manager.init(table_names));

    // Verify generated data
    int row_count = 0;
    int64_t last_timestamp = 0;

    while (auto block = manager.next_multi_batch()) {
        const auto* batch = block.value();
        for (const auto& table : batch->tables) {
            assert(std::string(table.table_name) == "test_table_1");
            assert(table.used_rows == 5);
            assert(table.columns.size() == 2);

            for (size_t row_idx = 0; row_idx < table.used_rows; ++row_idx) {
                int64_t timestamp = table.timestamps[row_idx];
                assert(timestamp == 1000 + static_cast<int64_t >(row_idx) * 10);

                last_timestamp = timestamp;
                row_count++;
            }
        }
        block.value()->release();
    }

    (void)row_count;
    (void)last_timestamp;
    assert(row_count == 5);
    assert(last_timestamp == 1040);  // 1000 + (5-1)*10

    std::cout << "test_data_generation_basic passed.\n";
}

void test_data_generation_with_interlace() {
    auto config = create_test_config();
    config.schema.generation.interlace_mode.enabled = true;
    config.schema.generation.interlace_mode.rows = 2;
    config.schema.generation.rows_per_table = 4;
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 2, 4, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    bool inited = manager.init(table_names);
    (void)inited;
    assert(inited);

    // Verify batch generation with interlace mode
    auto block = manager.next_multi_batch();
    assert(block);

    const auto* batch = block.value();
    assert(batch->tables.size() == 2);
    const auto& table1 = batch->tables[0];
    const auto& table2 = batch->tables[1];
    (void)table1;
    (void)table2;
    assert(table1.used_rows == 2);
    assert(table2.used_rows == 2);

    // Verify timestamps of first batch
    assert(table1.timestamps[0] == 1000);
    assert(table1.timestamps[1] == 1010);
    assert(table2.timestamps[0] == 1000);
    assert(table2.timestamps[1] == 1010);

    block.value()->release();

    // Verify no more data available
    auto block2 = manager.next_multi_batch();
    assert(block2);

    auto batch2 = block2.value();
    assert(!manager.has_more());

    assert(batch2->tables.size() == 2);
    const auto& table1_2 = batch2->tables[0];
    const auto& table2_2 = batch2->tables[1];
    (void)table1_2;
    (void)table2_2;

    assert(table1_2.used_rows == 2);
    assert(table2_2.used_rows == 2);

    assert(table1_2.timestamps[0] == 1020);
    assert(table1_2.timestamps[1] == 1030);
    assert(table2_2.timestamps[0] == 1020);
    assert(table2_2.timestamps[1] == 1030);

    block2.value()->release();

    std::cout << "test_data_generation_with_interlace passed.\n";
}

void test_per_request_rows_limit() {
    auto config = create_test_config();
    config.schema.generation.rows_per_batch = 3;
    config.schema.generation.rows_per_table = 10;
    config.schema.generation.interlace_mode.enabled = false;
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 2, 10, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    std::vector<std::string> table_names = {"test_table_1", "test_table_2"};
    assert(manager.init(table_names));

    // Count total rows across all batches
    size_t total_rows = 0;
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        assert(block);

        const auto* batch = block.value();
        assert(batch->total_rows <= 3);
        total_rows += batch->total_rows;

        block.value()->release();
    }

    (void)total_rows;
    assert(total_rows == 20);

    std::cout << "test_per_request_rows_limit passed.\n";
}

void test_data_generation_with_flow_control() {
    auto config = create_test_config();
    config.schema.generation.flow_control.enabled = true;
    config.schema.generation.flow_control.rate_limit = 100;  // 100 rows per second
    config.schema.generation.rows_per_table = 5;
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 1, 5, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    std::vector<std::string> table_names = {"test_table_1"};
    assert(manager.init(table_names));

    auto start_time = std::chrono::steady_clock::now();

    while (auto block = manager.next_multi_batch()) {
        const auto* batch = block.value();

        for (const auto& table : batch->tables) {
            (void)table;
            assert(std::string(table.table_name) == "test_table_1");
            assert(table.used_rows > 0);
        }

        block.value()->release();
    }

    auto end_time = std::chrono::steady_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // With rate limit of 100 rows/s, 5 rows should take at least 50ms
    (void)duration_ms;
    assert(duration_ms >= 50);

    std::cout << "test_data_generation_with_flow_control passed.\n";
}

void test_data_generation_with_tags() {
    auto config = create_test_config();

    config.data_format.support_tags = true;
    config.schema.tags_cfg.source_type = "generator";
    config.schema.tags_cfg.generator.schema = {
        {"tag1", "INT", "random", 100, 200},
        {"tag2", "VARCHAR(8)", "random"}
    };

    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);

    MemoryPool pool(1, 1, 5, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    std::vector<std::string> table_names = {"test_table_tags"};
    auto init_result = manager.init(table_names);
    assert(init_result);
    (void)init_result;

    auto block_opt = manager.next_multi_batch();
    assert(block_opt.has_value());
    auto* block = block_opt.value();

    assert(block->used_tables == 1);
    const auto& table = block->tables[0];

    // Verify tags pointer is set
    assert(table.tags_ptr != nullptr);
    assert(std::string(table.tags_ptr->table_name) == "test_table_tags");

    // Verify tag values
    // tag1 is INT
    auto tag1_cell = table.get_tag_cell(0, 0);
    assert(std::holds_alternative<int32_t>(tag1_cell));
    int32_t tag1_val = std::get<int32_t>(tag1_cell);
    assert(tag1_val >= 100 && tag1_val <= 200);

    // tag2 is VARCHAR
    auto tag2_cell = table.get_tag_cell(0, 1);
    assert(std::holds_alternative<std::string>(tag2_cell));
    std::string tag2_val = std::get<std::string>(tag2_cell);
    assert(tag2_val.length() > 0);

    (void)tag1_val;
    (void)tag2_val;

    block->release();
    std::cout << "test_data_generation_with_tags passed.\n";
}

void test_tags_disabled_by_config() {
    auto config = create_test_config();

    // Disable tags explicitly
    config.data_format.support_tags = false;
    config.schema.tags_cfg.source_type = "generator";
    config.schema.tags_cfg.generator.schema = {
        {"tag1", "INT", "random", 100, 200}
    };

    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);

    MemoryPool pool(1, 1, 5, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    std::vector<std::string> table_names = {"test_table_no_tags"};
    auto init_result = manager.init(table_names);
    assert(init_result);
    (void)init_result;

    auto block_opt = manager.next_multi_batch();
    assert(block_opt.has_value());
    auto* block = block_opt.value();

    const auto& table = block->tables[0];
    assert(table.tags_ptr == nullptr);
    (void)table;

    block->release();
    std::cout << "test_tags_disabled_by_config passed.\n";
}

static void create_csv_file(const std::string& path, const std::string& content) {
    std::ofstream f(path);
    f << content;
    f.close();
}

static void remove_csv_file(const std::string& path) {
    std::remove(path.c_str());
}

void test_csv_streaming_shared_source() {
    // Prepare a CSV data file
    const std::string csv_path = "tdm_csv_stream.csv";
    create_csv_file(csv_path,
        "col1,col2\n"
        "10,0.1\n"
        "20,0.2\n"
        "30,0.3\n"
        "40,0.4\n"
        "50,0.5\n");

    InsertDataConfig config;
    // Key: source_type = "csv" + loading_mode = "streaming"
    config.schema.columns_cfg.source_type = "csv";
    config.schema.columns_cfg.csv.enabled = true;
    config.schema.columns_cfg.csv.loading_mode = "streaming";
    config.schema.columns_cfg.csv.file_path = csv_path;
    config.schema.columns_cfg.csv.has_header = true;
    config.schema.columns_cfg.csv.delimiter = ",";
    config.schema.columns_cfg.csv.repeat_read = false;
    config.schema.columns_cfg.csv.schema = {
        {"col1", "INT"},
        {"col2", "FLOAT"}
    };
    // Use generator timestamp strategy so get_precision() returns "ms"
    config.schema.columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision = "ms";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.start_timestamp = Timestamp{5000};
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_step = 100;

    config.schema.generation.rows_per_table = 5;
    config.schema.generation.rows_per_batch = 100;
    config.timestamp_precision = "ms";
    config.data_format.support_tags = false;

    auto col_inst = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.csv.schema);
    auto tag_inst = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 2, 5, col_inst, tag_inst);
    TableDataManager manager(pool, config, col_inst, tag_inst);

    // Init with 2 tables: both share the same streaming source
    bool ok = manager.init({"csv_t1", "csv_t2"});
    (void)ok;
    assert(ok);
    assert(manager.has_more());
    assert(manager.table_states().size() == 2);

    // Drain all data
    size_t total = 0;
    std::unordered_map<std::string, std::vector<int32_t>> col1_by_table;
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        if (!block.has_value()) break;

        for (size_t table_idx = 0; table_idx < block.value()->used_tables; ++table_idx) {
            auto& table_block = block.value()->tables[table_idx];
            auto& values = col1_by_table[table_block.table_name];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                values.push_back(std::get<int32_t>(table_block.get_column_cell(row_idx, 0)));
            }
        }

        total += block.value()->total_rows;
        block.value()->release();
    }
    (void)total;
    assert(total > 0 && "Should generate rows from CSV streaming source");
    assert(manager.get_total_rows_generated() == total);
    assert(col1_by_table.size() == 2);
    assert(col1_by_table["csv_t1"].size() == 5);
    assert(col1_by_table["csv_t2"].size() == 5);
    assert(col1_by_table["csv_t1"] == col1_by_table["csv_t2"]);

    remove_csv_file(csv_path);
    std::cout << "test_csv_streaming_shared_source passed.\n";
}

void test_csv_streaming_shared_source_interlace_2() {
    const std::string csv_path = "tdm_csv_stream_interlace2.csv";
    create_csv_file(csv_path,
        "col1,col2\n"
        "10,0.1\n"
        "20,0.2\n"
        "30,0.3\n"
        "40,0.4\n"
        "50,0.5\n"
        "60,0.6\n");

    InsertDataConfig config;
    config.schema.columns_cfg.source_type = "csv";
    config.schema.columns_cfg.csv.enabled = true;
    config.schema.columns_cfg.csv.loading_mode = "streaming";
    config.schema.columns_cfg.csv.file_path = csv_path;
    config.schema.columns_cfg.csv.has_header = true;
    config.schema.columns_cfg.csv.delimiter = ",";
    config.schema.columns_cfg.csv.repeat_read = false;
    config.schema.columns_cfg.csv.schema = {
        {"col1", "INT"},
        {"col2", "FLOAT"}
    };
    config.schema.columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision = "ms";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.start_timestamp = Timestamp{1000};
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_step = 10;

    config.schema.generation.interlace_mode.enabled = true;
    config.schema.generation.interlace_mode.rows = 2;
    config.schema.generation.rows_per_table = 4;
    config.schema.generation.rows_per_batch = 100;
    config.timestamp_precision = "ms";
    config.data_format.support_tags = false;

    auto col_inst = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.csv.schema);
    auto tag_inst = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 2, 4, col_inst, tag_inst);
    TableDataManager manager(pool, config, col_inst, tag_inst);

    bool ok = manager.init({"csv_i2_t1", "csv_i2_t2"});
    (void)ok;
    assert(ok);

    std::unordered_map<std::string, std::vector<int32_t>> col1_by_table;
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        if (!block.has_value()) break;

        for (size_t table_idx = 0; table_idx < block.value()->used_tables; ++table_idx) {
            auto& table_block = block.value()->tables[table_idx];
            auto& values = col1_by_table[table_block.table_name];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                values.push_back(std::get<int32_t>(table_block.get_column_cell(row_idx, 0)));
            }
        }

        block.value()->release();
    }

    assert(col1_by_table.size() == 2);
    assert(col1_by_table["csv_i2_t1"].size() == 4);
    assert(col1_by_table["csv_i2_t2"].size() == 4);
    assert(col1_by_table["csv_i2_t1"] == col1_by_table["csv_i2_t2"]);
    assert((col1_by_table["csv_i2_t1"] == std::vector<int32_t>{10, 20, 30, 40}));

    remove_csv_file(csv_path);
    std::cout << "test_csv_streaming_shared_source_interlace_2 passed.\n";
}

void test_csv_streaming_shared_source_interlace_2_repeat_read() {
    const std::string csv_path = "tdm_csv_stream_interlace2_repeat.csv";
    create_csv_file(csv_path,
        "col1,col2\n"
        "10,0.1\n"
        "20,0.2\n");

    InsertDataConfig config;
    config.schema.columns_cfg.source_type = "csv";
    config.schema.columns_cfg.csv.enabled = true;
    config.schema.columns_cfg.csv.loading_mode = "streaming";
    config.schema.columns_cfg.csv.file_path = csv_path;
    config.schema.columns_cfg.csv.has_header = true;
    config.schema.columns_cfg.csv.delimiter = ",";
    config.schema.columns_cfg.csv.repeat_read = true;
    config.schema.columns_cfg.csv.schema = {
        {"col1", "INT"},
        {"col2", "FLOAT"}
    };
    config.schema.columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision = "ms";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.start_timestamp = Timestamp{2000};
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_step = 10;

    config.schema.generation.interlace_mode.enabled = true;
    config.schema.generation.interlace_mode.rows = 2;
    config.schema.generation.rows_per_table = 4;
    config.schema.generation.rows_per_batch = 100;
    config.timestamp_precision = "ms";
    config.data_format.support_tags = false;

    auto col_inst = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.csv.schema);
    auto tag_inst = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 2, 4, col_inst, tag_inst);
    TableDataManager manager(pool, config, col_inst, tag_inst);

    bool ok = manager.init({"csv_i2_rr_t1", "csv_i2_rr_t2"});
    (void)ok;
    assert(ok);

    std::unordered_map<std::string, std::vector<int32_t>> col1_by_table;
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        if (!block.has_value()) break;

        for (size_t table_idx = 0; table_idx < block.value()->used_tables; ++table_idx) {
            auto& table_block = block.value()->tables[table_idx];
            auto& values = col1_by_table[table_block.table_name];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                values.push_back(std::get<int32_t>(table_block.get_column_cell(row_idx, 0)));
            }
        }

        block.value()->release();
    }

    assert(col1_by_table.size() == 2);
    assert(col1_by_table["csv_i2_rr_t1"].size() == 4);
    assert(col1_by_table["csv_i2_rr_t2"].size() == 4);
    assert(col1_by_table["csv_i2_rr_t1"] == col1_by_table["csv_i2_rr_t2"]);
    assert((col1_by_table["csv_i2_rr_t1"] == std::vector<int32_t>{10, 20, 10, 20}));

    remove_csv_file(csv_path);
    std::cout << "test_csv_streaming_shared_source_interlace_2_repeat_read passed.\n";
}

void test_csv_streaming_shared_source_reset_rewinds_all_cursors() {
    const std::string csv_path = "tdm_csv_stream_reset_rewind.csv";
    create_csv_file(csv_path,
        "col1,col2\n"
        "10,0.1\n"
        "20,0.2\n"
        "30,0.3\n");

    InsertDataConfig config;
    config.schema.columns_cfg.source_type = "csv";
    config.schema.columns_cfg.csv.enabled = true;
    config.schema.columns_cfg.csv.loading_mode = "streaming";
    config.schema.columns_cfg.csv.file_path = csv_path;
    config.schema.columns_cfg.csv.has_header = true;
    config.schema.columns_cfg.csv.delimiter = ",";
    config.schema.columns_cfg.csv.repeat_read = false;
    config.schema.columns_cfg.csv.tbname_index = -1;
    config.schema.columns_cfg.csv.schema = {
        {"col1", "INT"},
        {"col2", "FLOAT"}
    };
    config.schema.columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision = "ms";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.start_timestamp = Timestamp{1000};
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_step = 1;

    config.schema.generation.interlace_mode.enabled = true;
    config.schema.generation.interlace_mode.rows = 1;
    config.schema.generation.rows_per_table = 3;
    config.schema.generation.rows_per_batch = 10;
    config.timestamp_precision = "ms";
    config.data_format.support_tags = false;

    auto col_inst = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.csv.schema);
    auto tag_inst = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 2, 3, col_inst, tag_inst);
    TableDataManager manager(pool, config, col_inst, tag_inst);

    bool ok = manager.init({"csv_reset_t1", "csv_reset_t2"});
    (void)ok;
    assert(ok);

    const auto& states = manager.table_states();
    assert(states.size() == 2);

    auto row_t1_a = states[0].generator->next_row();
    auto row_t2_a = states[1].generator->next_row();
    assert(row_t1_a.has_value());
    assert(row_t2_a.has_value());
    assert(std::get<int32_t>(row_t1_a->columns[0]) == 10);
    assert(std::get<int32_t>(row_t2_a->columns[0]) == 10);

    auto row_t1_b = states[0].generator->next_row();
    auto row_t2_b = states[1].generator->next_row();
    assert(row_t1_b.has_value());
    assert(row_t2_b.has_value());
    assert(std::get<int32_t>(row_t1_b->columns[0]) == 20);
    assert(std::get<int32_t>(row_t2_b->columns[0]) == 20);

    // Reset one cursor source should rewind the shared coordinator for all cursors.
    states[0].generator->reset();

    auto row_t1_after_reset = states[0].generator->next_row();
    auto row_t2_after_reset = states[1].generator->next_row();
    assert(row_t1_after_reset.has_value());
    assert(row_t2_after_reset.has_value());
    assert(std::get<int32_t>(row_t1_after_reset->columns[0]) == 10);
    assert(std::get<int32_t>(row_t2_after_reset->columns[0]) == 10);

    remove_csv_file(csv_path);
    std::cout << "test_csv_streaming_shared_source_reset_rewinds_all_cursors passed.\n";
}

void test_csv_streaming_shared_source_reset_after_exhaustion() {
    const std::string csv_path = "tdm_csv_stream_reset_after_eof.csv";
    create_csv_file(csv_path,
        "col1,col2\n"
        "10,0.1\n"
        "20,0.2\n");

    InsertDataConfig config;
    config.schema.columns_cfg.source_type = "csv";
    config.schema.columns_cfg.csv.enabled = true;
    config.schema.columns_cfg.csv.loading_mode = "streaming";
    config.schema.columns_cfg.csv.file_path = csv_path;
    config.schema.columns_cfg.csv.has_header = true;
    config.schema.columns_cfg.csv.delimiter = ",";
    config.schema.columns_cfg.csv.repeat_read = false;
    config.schema.columns_cfg.csv.tbname_index = -1;
    config.schema.columns_cfg.csv.schema = {
        {"col1", "INT"},
        {"col2", "FLOAT"}
    };
    config.schema.columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision = "ms";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.start_timestamp = Timestamp{1000};
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_step = 1;

    config.schema.generation.interlace_mode.enabled = true;
    config.schema.generation.interlace_mode.rows = 1;
    config.schema.generation.rows_per_table = 2;
    config.schema.generation.rows_per_batch = 10;
    config.timestamp_precision = "ms";
    config.data_format.support_tags = false;

    auto col_inst = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.csv.schema);
    auto tag_inst = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 2, 2, col_inst, tag_inst);
    TableDataManager manager(pool, config, col_inst, tag_inst);

    bool ok = manager.init({"csv_eof_t1", "csv_eof_t2"});
    (void)ok;
    assert(ok);

    const auto& states = manager.table_states();
    assert(states.size() == 2);

    // Drain both cursors to EOF.
    for (int i = 0; i < 2; ++i) {
        auto r1 = states[0].generator->next_row();
        auto r2 = states[1].generator->next_row();
        assert(r1.has_value());
        assert(r2.has_value());
    }
    assert(!states[0].generator->next_row().has_value());
    assert(!states[1].generator->next_row().has_value());
    assert(!states[0].generator->has_more());
    assert(!states[1].generator->has_more());

    // One reset rewinds shared source, but each generator also has its own
    // generated_rows_ counter, so reset both generators before replay checks.
    states[0].generator->reset();
    states[1].generator->reset();

    auto r1_after_reset = states[0].generator->next_row();
    auto r2_after_reset = states[1].generator->next_row();
    (void)r1_after_reset;
    (void)r2_after_reset;
    assert(r1_after_reset.has_value());
    assert(r2_after_reset.has_value());
    assert(std::get<int32_t>(r1_after_reset->columns[0]) == 10);
    assert(std::get<int32_t>(r2_after_reset->columns[0]) == 10);

    remove_csv_file(csv_path);
    std::cout << "test_csv_streaming_shared_source_reset_after_exhaustion passed.\n";
}

void test_csv_streaming_with_empty_delimiter() {
    // Verify the empty-delimiter fallback: delimiter.empty() ? ',' : delimiter[0]
    const std::string csv_path = "tdm_csv_delim.csv";
    create_csv_file(csv_path,
        "col1,col2\n"
        "1,2.5\n"
        "3,4.5\n");

    InsertDataConfig config;
    config.schema.columns_cfg.source_type = "csv";
    config.schema.columns_cfg.csv.enabled = true;
    config.schema.columns_cfg.csv.loading_mode = "streaming";
    config.schema.columns_cfg.csv.file_path = csv_path;
    config.schema.columns_cfg.csv.has_header = true;
    config.schema.columns_cfg.csv.delimiter = "";  // empty → defaults to ','
    config.schema.columns_cfg.csv.repeat_read = false;
    config.schema.columns_cfg.csv.schema = {
        {"col1", "INT"},
        {"col2", "FLOAT"}
    };
    config.schema.columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision = "ms";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.start_timestamp = Timestamp{1000};
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_step = 10;

    config.schema.generation.rows_per_table = 2;
    config.schema.generation.rows_per_batch = 100;
    config.timestamp_precision = "ms";
    config.data_format.support_tags = false;

    auto col_inst = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.csv.schema);
    auto tag_inst = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 1, 2, col_inst, tag_inst);
    TableDataManager manager(pool, config, col_inst, tag_inst);

    bool ok = manager.init({"delim_test"});
    (void)ok;
    assert(ok);

    size_t total = 0;
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        if (!block.has_value()) break;
        total += block.value()->total_rows;
        block.value()->release();
    }
    (void)total;
    assert(total > 0 && "Should parse CSV with default comma delimiter");

    remove_csv_file(csv_path);
    std::cout << "test_csv_streaming_with_empty_delimiter passed.\n";
}

void test_csv_streaming_with_custom_delimiter() {
    // Verify non-empty delimiter is used (semicolon)
    const std::string csv_path = "tdm_csv_semi.csv";
    create_csv_file(csv_path,
        "col1;col2\n"
        "100;1.5\n"
        "200;2.5\n"
        "300;3.5\n");

    InsertDataConfig config;
    config.schema.columns_cfg.source_type = "csv";
    config.schema.columns_cfg.csv.enabled = true;
    config.schema.columns_cfg.csv.loading_mode = "streaming";
    config.schema.columns_cfg.csv.file_path = csv_path;
    config.schema.columns_cfg.csv.has_header = true;
    config.schema.columns_cfg.csv.delimiter = ";";
    config.schema.columns_cfg.csv.repeat_read = false;
    config.schema.columns_cfg.csv.schema = {
        {"col1", "INT"},
        {"col2", "FLOAT"}
    };
    config.schema.columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision = "ms";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.start_timestamp = Timestamp{2000};
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_step = 50;

    config.schema.generation.rows_per_table = 3;
    config.schema.generation.rows_per_batch = 100;
    config.timestamp_precision = "ms";
    config.data_format.support_tags = false;

    auto col_inst = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.csv.schema);
    auto tag_inst = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 1, 3, col_inst, tag_inst);
    TableDataManager manager(pool, config, col_inst, tag_inst);

    bool ok = manager.init({"semi_test"});
    (void)ok;
    assert(ok);

    size_t total = 0;
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        if (!block.has_value()) break;
        total += block.value()->total_rows;
        block.value()->release();
    }
    (void)total;
    assert(total == 3 && "Should generate 3 rows from semicolon-delimited CSV");

    remove_csv_file(csv_path);
    std::cout << "test_csv_streaming_with_custom_delimiter passed.\n";
}

void test_csv_streaming_not_triggered_for_generator_source() {
    // Verify the streaming branch is NOT entered when source_type == "generator"
    // (no CSV file needed — the generator source path should work as before)
    auto config = create_test_config();
    config.schema.columns_cfg.csv.loading_mode = "streaming";  // streaming mode, but source is generator
    config.schema.generation.rows_per_table = 3;

    auto col_inst = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_inst = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 1, 3, col_inst, tag_inst);
    TableDataManager manager(pool, config, col_inst, tag_inst);

    bool ok = manager.init({"gen_tbl"});
    (void)ok;
    assert(ok);

    size_t total = 0;
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        if (!block.has_value()) break;
        total += block.value()->total_rows;
        block.value()->release();
    }
    (void)total;
    assert(total == 3);

    std::cout << "test_csv_streaming_not_triggered_for_generator_source passed.\n";
}

void test_csv_streaming_not_triggered_for_preload_mode() {
    // Verify the streaming branch is NOT entered when loading_mode == "preload"
    // even if source_type == "csv" (RowDataGenerator handles preload internally)
    const std::string csv_path = "tdm_csv_preload.csv";
    create_csv_file(csv_path,
        "col1,col2\n"
        "1,0.1\n"
        "2,0.2\n");

    InsertDataConfig config;
    config.schema.columns_cfg.source_type = "csv";
    config.schema.columns_cfg.csv.enabled = true;
    config.schema.columns_cfg.csv.loading_mode = "preload";  // NOT streaming
    config.schema.columns_cfg.csv.file_path = csv_path;
    config.schema.columns_cfg.csv.has_header = true;
    config.schema.columns_cfg.csv.delimiter = ",";
    config.schema.columns_cfg.csv.repeat_read = false;
    config.schema.columns_cfg.csv.schema = {
        {"col1", "INT"},
        {"col2", "FLOAT"}
    };
    config.schema.columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision = "ms";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.start_timestamp = Timestamp{1000};
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_step = 10;

    config.schema.generation.rows_per_table = 2;
    config.schema.generation.rows_per_batch = 100;
    config.timestamp_precision = "ms";
    config.data_format.support_tags = false;

    auto col_inst = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.csv.schema);
    auto tag_inst = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 1, 2, col_inst, tag_inst);
    TableDataManager manager(pool, config, col_inst, tag_inst);

    // Should succeed — streaming source NOT created, RowDataGenerator handles preload
    bool ok = manager.init({"preload_tbl"});
    (void)ok;
    assert(ok);

    size_t total = 0;
    while (manager.has_more()) {
        auto block = manager.next_multi_batch();
        if (!block.has_value()) break;
        total += block.value()->total_rows;
        block.value()->release();
    }
    (void)total;
    assert(total == 2);

    remove_csv_file(csv_path);
    std::cout << "test_csv_streaming_not_triggered_for_preload_mode passed.\n";
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
    test_data_generation_with_tags();
    test_tags_disabled_by_config();

    // CSV streaming shared source path
    test_csv_streaming_shared_source();
    test_csv_streaming_shared_source_interlace_2();
    test_csv_streaming_shared_source_interlace_2_repeat_read();
    test_csv_streaming_shared_source_reset_rewinds_all_cursors();
    test_csv_streaming_shared_source_reset_after_exhaustion();
    test_csv_streaming_with_empty_delimiter();
    test_csv_streaming_with_custom_delimiter();
    test_csv_streaming_not_triggered_for_generator_source();
    test_csv_streaming_not_triggered_for_preload_mode();

    std::cout << "All tests passed.\n";
    return 0;
}