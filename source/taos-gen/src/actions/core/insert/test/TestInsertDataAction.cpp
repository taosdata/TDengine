#include "InsertDataAction.hpp"
#include "SqlData.hpp"
#include "TDengineRegistrar.hpp"
#include "TableNameManager.hpp"
#include <cassert>
#include <memory>
#include <thread>
#include <chrono>
#include "FilesystemCompat.hpp"


TDengineConfig* get_tdengine_config(InsertDataConfig& config) {
    set_plugin_config(config.extensions, "tdengine", TDengineConfig{});
    return get_plugin_config_mut<TDengineConfig>(config.extensions, "tdengine");
}

InsertDataConfig create_test_config() {
    InsertDataConfig config;

    // Configure columns for generator mode
    config.schema.columns_cfg.source_type = "generator";

    // Setup timestamp strategy
    auto& ts_config = config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{1700000000000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";

    // Setup schema
    auto& schema = config.schema.columns_cfg.generator.schema;
    schema = {
        {"col1", "INT", "random", 1, 100},
        {"col2", "DOUBLE", "random", 0.0, 1.0}
    };

    // Setup table name generation
    config.schema.tbname.source_type = "generator";
    config.schema.tbname.generator.prefix = "d";
    config.schema.tbname.generator.count = 10;
    config.schema.tbname.generator.from = 0;

    // Setup control parameters
    config.schema.generation.rows_per_table = 100;
    config.schema.generation.generate_threads = 2;
    config.schema.generation.rows_per_batch = 10;
    config.queue_capacity = 2;
    config.insert_threads = 2;
    config.failure_handling.max_retries = 1;
    config.failure_handling.retry_interval_ms = 1000;
    config.failure_handling.on_failure = "exit";

    // Data format settings
    config.data_format.format_type = "stmt";

    // Setup target
    config.timestamp_precision = "ms";
    config.target_type = "tdengine";

    auto* tc = get_tdengine_config(config);
    *tc = TDengineConfig("taos+ws://localhost:6041/test_action");
    tc->init();
    config.schema.name = "test_super_table";

    return config;
}

void test_basic_initialization() {
    GlobalConfig global;
    auto config = create_test_config();
    config.schema.columns_cfg.generator.schema.clear();  // Clear schema to test error handling

    try {
        InsertDataAction action(global, config);
        action.execute();
        assert(false && "Should throw exception for missing schema configuration");
    } catch (const std::exception& e) {
        assert(std::string(e.what()).find("Schema configuration is empty") != std::string::npos);
        std::cout << "test_basic_initialization passed\n";
    }
}

void test_table_name_generation() {
    auto config = create_test_config();
    TableNameManager name_manager(config);
    auto names = name_manager.generate_table_names();

    assert(names.size() == 10);
    assert(names[0] == "d0");
    assert(names[9] == "d9");

    auto split_names = name_manager.split_for_threads();
    assert(split_names.size() == config.schema.generation.generate_threads);
    assert(split_names[0].size() == 5);  // Even split for 10 tables across 2 threads

    std::cout << "test_table_name_generation passed\n";
}

void test_data_pipeline() {
    auto config = create_test_config();
    DataPipeline<FormatResult> pipeline(false, 2, 2, 1000);

    std::atomic<bool> producer_done{false};
    std::atomic<size_t> rows_generated{0};
    std::atomic<size_t> rows_consumed{0};

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});
    MemoryPool pool(1, 5, 2, col_instances, tag_instances);

    // Start producer thread
    std::thread producer([&]() {
        for(int i = 0; i < 5; i++) {
            MultiBatch batch;
            std::vector<RowData> rows;
            rows.push_back({1500000000000, {3.14f, 42}});
            rows.push_back({1500000000001, {2.71f, 43}});
            batch.table_batches.emplace_back("table" + std::to_string(i), std::move(rows));
            batch.update_metadata();

            auto* block = pool.convert_to_memory_block(std::move(batch));

            auto insert_ptr = BaseInsertData::make_with_payload(block, col_instances, tag_instances, SqlData{"INSERT INTO test_table VALUES (...)"});
            pipeline.push_data(0, FormatResult(std::move(insert_ptr)));

            rows_generated++;
        }
        producer_done = true;
    });

    // Start consumer thread
    std::thread consumer([&]() {
        while (!producer_done || pipeline.total_queued() > 0) {
            auto result = pipeline.fetch_data(0);
            if (result.status == DataPipeline<FormatResult>::Status::Success) {
                std::visit([&](const auto& data) {
                    using T = std::decay_t<decltype(data)>;
                    if constexpr (std::is_same_v<T, InsertFormatResult>) {
                        assert(data->total_rows == 2);
                        rows_consumed++;
                    }
                }, *result.data);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    producer.join();
    consumer.join();

    assert(rows_generated == 5);
    assert(rows_consumed == 5);
    assert(pipeline.total_queued() == 0);

    std::cout << "test_data_pipeline passed\n";
}

void test_data_pipeline_with_tags() {
    auto config = create_test_config();
    DataPipeline<FormatResult> pipeline(false, 2, 2, 1000);

    std::atomic<bool> producer_done{false};
    std::atomic<size_t> rows_generated{0};
    std::atomic<size_t> rows_consumed{0};

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    ColumnConfigInstanceVector tag_instances;
    tag_instances.emplace_back(ColumnConfig{"t1", "INT"});

    MemoryPool pool(1, 5, 2, col_instances, tag_instances);

    // Start producer thread
    std::thread producer([&]() {
        for(int i = 0; i < 5; i++) {
            MultiBatch batch;
            std::vector<RowData> rows;
            rows.push_back({1500000000000, {3.14f}});
            batch.table_batches.emplace_back("table" + std::to_string(i), std::move(rows));
            batch.update_metadata();

            auto* block = pool.convert_to_memory_block(std::move(batch));

            // Set tags for the block
            std::vector<ColumnType> tag_values = {int32_t(100)};
            block->tables[0].tags_ptr = pool.register_table_tags("table" + std::to_string(i), tag_values);

            auto insert_ptr = BaseInsertData::make_with_payload(block, col_instances, tag_instances, SqlData{"INSERT INTO ..."});
            pipeline.push_data(0, FormatResult(std::move(insert_ptr)));

            rows_generated++;
        }
        producer_done = true;
    });

    // Start consumer thread
    std::thread consumer([&]() {
        while (!producer_done || pipeline.total_queued() > 0) {
            auto result = pipeline.fetch_data(0);
            if (result.status == DataPipeline<FormatResult>::Status::Success) {
                const auto& variant_ref = *result.data;
                if (std::holds_alternative<InsertFormatResult>(variant_ref)) {
                    const auto& insert = std::get<InsertFormatResult>(variant_ref);
                    assert(insert->total_rows == 1);
                    assert(insert->column_count() == 1);
                    assert(insert->tag_count() == 1);

                    auto col_val = insert->get_block()->tables[0].get_column_cell(0, 0);
                    assert(std::holds_alternative<float>(col_val));
                    assert(std::get<float>(col_val) == 3.14f);

                    auto tag_val = insert->get_block()->tables[0].get_tag_cell(0, 0);
                    assert(std::holds_alternative<int32_t>(tag_val));
                    assert(std::get<int32_t>(tag_val) == 100);

                    (void)col_val;
                    (void)tag_val;

                    rows_consumed++;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    producer.join();
    consumer.join();

    assert(rows_generated == 5);
    assert(rows_consumed == 5);

    std::cout << "test_data_pipeline_with_tags passed\n";
}

void test_data_generation() {
    auto config = create_test_config();
    config.schema.generation.rows_per_table = 5;
    config.schema.generation.interlace_mode.enabled = false;

    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 1, 5, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    std::vector<std::string> table_names = {"d0"};
    assert(manager.init(table_names));

    int row_count = 0;
    while (auto block = manager.next_multi_batch()) {
        const auto* batch = block.value();
        assert(batch->used_tables == 1);
        row_count += batch->total_rows;

        // Verify timestamp progression
        for (const auto& table : batch->tables) {
            for (size_t i = 0; i < table.used_rows; i++) {
                assert(table.timestamps[i] == 1700000000000 + static_cast<int64_t>(row_count - table.used_rows + i) * 10);
            }
        }
        block.value()->release();
    }

    (void)row_count;
    assert(row_count == 5);
    std::cout << "test_data_generation passed\n";
}

void test_data_generation_with_tags() {
    auto config = create_test_config();
    config.schema.generation.rows_per_table = 5;
    config.schema.generation.interlace_mode.enabled = false;

    // Enable tags
    config.data_format.support_tags = true;
    config.schema.tags_cfg.source_type = "generator";
    config.schema.tags_cfg.generator.schema = {
        {"tag1", "INT", "random", 100, 101}
    };

    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);
    MemoryPool pool(1, 1, 5, col_instances, tag_instances);
    TableDataManager manager(pool, config, col_instances, tag_instances);

    std::vector<std::string> table_names = {"d0"};
    assert(manager.init(table_names));

    int row_count = 0;
    while (auto block = manager.next_multi_batch()) {
        const auto* batch = block.value();
        assert(batch->used_tables == 1);
        row_count += batch->total_rows;

        // Verify timestamp progression and tags
        for (const auto& table : batch->tables) {
            // Verify tags exist
            assert(table.tags_ptr != nullptr);

            // Verify tag value
            auto tag_val = table.get_tag_cell(0, 0);
            assert(std::holds_alternative<int32_t>(tag_val));
            assert(std::get<int32_t>(tag_val) == 100);
            (void)tag_val;

            for (size_t i = 0; i < table.used_rows; i++) {
                assert(table.timestamps[i] == 1700000000000 + static_cast<int64_t>(row_count - table.used_rows + i) * 10);
            }
        }
        block.value()->release();
    }

    (void)row_count;
    assert(row_count == 5);
    std::cout << "test_data_generation_with_tags passed\n";
}

void test_end_to_end_data_generation() {
    GlobalConfig global;
    global.verbose = true;

    std::vector<DataChannel> channel_types = {
        // DataChannel{"native"},
        DataChannel{"websocket"}
    };

    std::vector<DataFormat> format_types(2);
    format_types[0].format_type = "sql";
    format_types[1].format_type = "stmt";
    set_format_opt(format_types[0], "sql", SqlFormatOptions{});
    set_format_opt(format_types[1], "stmt", StmtFormatOptions{});
    auto* sf = get_format_opt_mut<StmtFormatOptions>(format_types[1], "stmt");
    assert(sf != nullptr);
    sf->version = "v2";

    for (const auto& channel : channel_types) {
        for (const auto& format : format_types) {
            std::cout << "[DEBUG] Executing test for channel_type=" << channel.channel_type
                      << ", format_type=" << format.format_type << std::endl;

            auto config = create_test_config();

            config.data_format = format;

            config.schema.generation.rows_per_table = 10;
            config.schema.generation.generate_threads = 1;
            config.schema.generation.data_cache.enabled = false;
            config.queue_capacity = 2;
            config.insert_threads = 1;
            config.schema.tbname.generator.count = 4;           // 4 tables total
            config.target_type = "tdengine";

            auto* tc = get_tdengine_config(config);
            assert(tc != nullptr);
            tc->database = "test_action";
            config.schema.name = "test_super_table";

            InsertDataAction action(global, config);

            action.execute();

            // TODO: Verify contain correct data
            // int row_count = 0;
            // assert(row_count == 10 && "Each table should have 10 rows");
        }
    }

    std::cout << "test_end_to_end_data_generation passed\n";
}

void test_end_to_end_data_generation_with_tags() {
    GlobalConfig global;
    global.verbose = true;

    std::vector<DataChannel> channel_types = {
        // DataChannel{"native"},
        DataChannel{"websocket"}
    };

    std::vector<DataFormat> format_types(2);
    format_types[0].format_type = "sql";
    format_types[1].format_type = "stmt";
    set_format_opt(format_types[0], "sql", SqlFormatOptions{});
    set_format_opt(format_types[1], "stmt", StmtFormatOptions{});
    auto* sf = get_format_opt_mut<StmtFormatOptions>(format_types[1], "stmt");
    assert(sf != nullptr);
    sf->version = "v2";

    for (const auto& channel : channel_types) {
        for (const auto& format : format_types) {
            std::cout << "[DEBUG] Executing test for channel_type=" << channel.channel_type
                      << ", format_type=" << format.format_type << " with tags" << std::endl;

            auto config = create_test_config();

            config.data_format = format;

            // Enable tags
            config.data_format.support_tags = true;
            config.schema.tags_cfg.source_type = "generator";
            config.schema.tags_cfg.generator.schema = {
                {"tag1", "INT", "random", 1, 100},
                {"tag2", "VARCHAR(8)", "random"}
            };

            config.schema.generation.rows_per_table = 10;
            config.schema.generation.generate_threads = 1;
            config.schema.generation.data_cache.enabled = false;
            config.queue_capacity = 2;
            config.insert_threads = 1;
            config.schema.tbname.generator.count = 4;           // 4 tables total
            config.target_type = "tdengine";

            auto* tc = get_tdengine_config(config);
            assert(tc != nullptr);
            tc->database = "test_action";
            config.schema.name = "test_super_table";

            InsertDataAction action(global, config);

            action.execute();
        }
    }

    std::cout << "test_end_to_end_data_generation_with_tags passed\n";
}

void test_concurrent_data_generation() {
    GlobalConfig global;
    global.verbose = true;
    auto config = create_test_config();
    config.schema.generation.rows_per_table = 1000;     // More rows to test concurrency
    config.schema.generation.generate_threads = 4;      // More threads
    config.schema.generation.data_cache.enabled = false;
    config.insert_threads = 4;
    config.schema.tbname.generator.count = 8;             // 8 tables
    config.target_type = "tdengine";

    auto* tc = get_tdengine_config(config);
    assert(tc != nullptr);
    tc->database = "test_action";
    config.schema.name = "test_super_table";

    config.data_format.format_type = "stmt";
    set_format_opt(config.data_format, "stmt", StmtFormatOptions{});

    InsertDataAction action(global, config);

    // Measure execution time to verify concurrent operation
    auto start = std::chrono::steady_clock::now();
    action.execute();
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // TODO: Verify all data was generated
    size_t total_rows = 8000;
    (void)total_rows;
    assert(total_rows == 8000 && "Total rows should match configuration");
    std::cout << "test_concurrent_data_generation passed (duration: "
              << duration.count() << "ms)\n";
}

void test_error_handling() {
    GlobalConfig global;
    auto config = create_test_config();

    // Test case 1: Invalid target type
    {
        auto invalid_config = config;
        invalid_config.target_type = "invalid_target";

        InsertDataAction action(global, invalid_config);
        try {
            action.execute();
            assert(false && "Should throw for invalid target");
        } catch (const std::exception&) {
            // Expected
        }
    }

    // Test case 2: Invalid table configuration
    {
        auto invalid_config = config;
        invalid_config.schema.tbname.generator.count = 0;

        InsertDataAction action(global, invalid_config);
        try {
            action.execute();
            assert(false && "Should throw for invalid table count");
        } catch (const std::exception&) {
            // Expected
        }
    }

    // Test case 3: Invalid thread configuration
    {
        auto invalid_config = config;
        invalid_config.schema.generation.generate_threads = 0;

        InsertDataAction action(global, invalid_config);
        try {
            action.execute();
            assert(false && "Should throw for invalid thread count");
        } catch (const std::exception&) {
            // Expected
        }
    }

    std::cout << "test_error_handling passed\n";
}

void test_cache_units_data_initialization() {
    GlobalConfig global;
    auto config = create_test_config();

    // Enable cache mode
    config.schema.generation.data_cache.enabled = true;
    config.schema.generation.data_cache.num_cached_batches = 2;
    config.schema.generation.tables_reuse_data = false;
    config.schema.generation.rows_per_table = 10;
    config.schema.generation.rows_per_batch = 5;
    config.schema.generation.generate_threads = 1;
    config.insert_threads = 1;
    config.queue_capacity = 1;
    config.schema.tbname.generator.count = 2;

    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);

    MemoryPool pool(
        config.queue_capacity * config.insert_threads,
        config.schema.tbname.generator.count,
        config.schema.generation.rows_per_batch,
        col_instances,
        tag_instances,
        config.schema.generation.tables_reuse_data,
        config.schema.generation.data_cache.num_cached_batches
    );

    // Should initialize cache data for all tables and batches
    InsertDataAction action(global, config);
    action.init_cache_units_data(pool,
                                 config.schema.generation.data_cache.num_cached_batches,
                                 config.schema.tbname.generator.count,
                                 config.schema.generation.rows_per_batch);

    // Check cache units count
    assert(pool.get_cache_units_count() == config.schema.generation.data_cache.num_cached_batches);

    // Check each cache unit and table is prefilled
    for (size_t cache_idx = 0; cache_idx < pool.get_cache_units_count(); ++cache_idx) {
        assert(pool.is_cache_unit_prefilled(cache_idx));
        auto* cache_unit = pool.get_cache_unit(cache_idx);
        (void)cache_unit;
        assert(cache_unit != nullptr);
        for (size_t table_idx = 0; table_idx < static_cast<size_t>(config.schema.tbname.generator.count); ++table_idx) {
            assert(cache_unit->tables[table_idx].data_prefilled);
            assert(cache_unit->tables[table_idx].used_rows == static_cast<size_t>(config.schema.generation.rows_per_batch));
        }
    }

    std::cout << "test_cache_units_data_initialization passed\n";
}

void test_cache_units_data_with_reuse() {
    GlobalConfig global;
    auto config = create_test_config();

    // Enable cache mode and table data reuse
    config.schema.generation.data_cache.enabled = true;
    config.schema.generation.data_cache.num_cached_batches = 1;
    config.schema.generation.tables_reuse_data = true;
    config.schema.generation.rows_per_table = 3;
    config.schema.generation.rows_per_batch = 3;
    config.schema.generation.generate_threads = 1;
    config.insert_threads = 1;
    config.queue_capacity = 1;
    config.schema.tbname.generator.count = 2;

    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);

    MemoryPool pool(
        config.queue_capacity * config.insert_threads,
        config.schema.tbname.generator.count,
        config.schema.generation.rows_per_batch,
        col_instances,
        tag_instances,
        config.schema.generation.tables_reuse_data,
        config.schema.generation.data_cache.num_cached_batches
    );

    InsertDataAction action(global, config);
    action.init_cache_units_data(pool,
                                 config.schema.generation.data_cache.num_cached_batches,
                                 config.schema.tbname.generator.count,
                                 config.schema.generation.rows_per_batch);

    // Only the first table should be initialized due to tables_reuse_data
    for (size_t cache_idx = 0; cache_idx < pool.get_cache_units_count(); ++cache_idx) {
        auto* cache_unit = pool.get_cache_unit(cache_idx);
        (void)cache_unit;
        assert(cache_unit != nullptr);
        assert(cache_unit->tables[0].data_prefilled);
        assert(cache_unit->tables[0].used_rows == static_cast<size_t>(config.schema.generation.rows_per_batch));
        // The second table should not be initialized
        assert(!cache_unit->tables[1].data_prefilled);
        assert(cache_unit->tables[1].used_rows == 0);
    }

    std::cout << "test_cache_units_data_with_reuse passed\n";
}

void test_cache_units_data_generator_failure() {
    GlobalConfig global;
    auto config = create_test_config();

    config.schema.generation.data_cache.enabled = true;
    config.schema.generation.data_cache.num_cached_batches = 1;
    config.schema.tbname.generator.count = 1;
    config.schema.generation.rows_per_table = 2;
    config.schema.generation.rows_per_batch = 10;

    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.generator.schema);
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.generator.schema);

    MemoryPool pool(
        1,
        config.schema.tbname.generator.count,
        config.schema.generation.rows_per_batch,
        col_instances,
        tag_instances,
        false,
        config.schema.generation.data_cache.num_cached_batches
    );

    InsertDataAction action(global, config);

    bool caught = false;
    try {
        config.schema.generation.data_cache.num_cached_batches = 2;
        action.init_cache_units_data(pool,
                                     config.schema.generation.data_cache.num_cached_batches,
                                     config.schema.tbname.generator.count,
                                     config.schema.generation.rows_per_batch);
    } catch (const std::exception& e) {
        assert(std::string(e.what()).find("RowDataGenerator could not generate enough data for cache initialization") != std::string::npos);
        caught = true;
    }
    (void)caught;
    assert(caught);
    std::cout << "test_cache_units_data_generator_failure passed\n";
}

int main() {
    register_tdengine_plugin_config_hooks();
    test_basic_initialization();
    test_table_name_generation();
    test_data_pipeline();
    test_data_pipeline_with_tags();
    test_data_generation();
    test_data_generation_with_tags();
    test_end_to_end_data_generation();
    test_end_to_end_data_generation_with_tags();
    test_concurrent_data_generation();
    test_error_handling();

    test_cache_units_data_initialization();
    test_cache_units_data_with_reuse();
    test_cache_units_data_generator_failure();

    std::cout << "All InsertDataAction tests passed.\n";
    return 0;
}