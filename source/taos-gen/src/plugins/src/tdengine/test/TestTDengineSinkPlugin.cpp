#include "TDengineSinkPlugin.hpp"
#include "TDengineRegistrar.hpp"
#include "StmtContext.hpp"
#include <cassert>
#include <iostream>
#include <stdexcept>
#include <optional>

TDengineConfig* get_tdengine_config(InsertDataConfig& config) {
    return get_plugin_config_mut<TDengineConfig>(config.extensions, "tdengine");
}

InsertDataConfig create_test_config(const std::string& format_type = "sql") {
    InsertDataConfig config;

    // Source config
    config.schema.tbname.source_type = "generator";
    config.schema.columns = {
        ColumnConfig{"col1", "INT"},
        ColumnConfig{"col2", "DOUBLE"}
    };

    // Target config
    set_plugin_config(config.extensions, "tdengine", TDengineConfig{});
    auto* tc = get_tdengine_config(config);
    assert(tc != nullptr);
    tc->dsn = "taos://root:taosdata@localhost:6030/test_action";
    tc->init();
    config.target_type = "tdengine";
    config.timestamp_precision = "ms";
    config.schema.name = "test_super_table";
    config.schema.apply();

    // Control config
    config.data_format.format_type = format_type;

    if (format_type == "sql") {
        set_format_opt(config.data_format, "sql", SqlFormatOptions{});
    }
    else if (format_type == "stmt") {
        set_format_opt(config.data_format, "stmt", StmtFormatOptions{});
    }

    // Insert control for retry
    config.failure_handling.max_retries = 1;
    config.failure_handling.retry_interval_ms = 1;
    config.failure_handling.on_failure = "exit";

    return config;
}

void test_create_tdengine_sink() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());

    auto plugin = SinkPluginFactory::create_sink_plugin(config, col_instances, tag_instances);
    assert(plugin != nullptr);

    // Verify the created type
    auto* td_sink = dynamic_cast<TDengineSinkPlugin*>(plugin.get());
    assert(td_sink != nullptr);
    (void)td_sink;

    std::cout << "test_create_tdengine_sink passed." << std::endl;
}

void test_constructor() {
    // Test valid timestamp precision
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());

    // Test valid SQL format
    {
        auto plugin = std::make_unique<TDengineSinkPlugin>(config, col_instances, tag_instances, 0);
        assert(plugin != nullptr);
    }

    // Test valid STMT format
    {
        auto stmt_config = create_test_config("stmt");
        auto plugin = std::make_unique<TDengineSinkPlugin>(stmt_config, col_instances, tag_instances, 0);
        assert(plugin != nullptr);
    }

    // Test invalid format type
    {
        config.data_format.format_type = "invalid";
        try {
            TDengineSinkPlugin plugin(config, col_instances, tag_instances, 0);
            assert(false); // Should not reach here
        } catch (const std::invalid_argument& e) {
            assert(std::string(e.what()).find("Unsupported format type") != std::string::npos);
        }
    }

    // Test empty timestamp precision (should default)
    {
        config.data_format.format_type = "sql";
        config.timestamp_precision = "";
        auto plugin = std::make_unique<TDengineSinkPlugin>(config, col_instances, tag_instances, 0);
        assert(plugin != nullptr);
    }

    std::cout << "test_constructor passed." << std::endl;
}

void test_is_connected() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());

    TDengineSinkPlugin plugin(config, col_instances, tag_instances, 0);

    bool connected = plugin.is_connected();
    (void)connected;
    assert(!connected);

    std::cout << "test_is_connected passed." << std::endl;
}

void test_connection() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());

    auto* tc = get_tdengine_config(config);
    assert(tc != nullptr);
    std::optional<ConnectorSource> conn_src(*tc);
    TDengineSinkPlugin plugin(config, col_instances, tag_instances);

    // Test successful connection
    bool connected = plugin.connect_with_source(conn_src);
    (void)connected;
    assert(connected);

    // Test reconnection (should return true if already connected)
    connected = plugin.connect();
    assert(connected);

    // Test connection with invalid config
    tc->host = "invalid_host";
    TDengineSinkPlugin invalid_writer(config, col_instances, tag_instances);
    connected = invalid_writer.connect();
    assert(!connected);

    std::cout << "test_connection passed." << std::endl;
}

void test_format_sql() {
    auto config = create_test_config("sql");
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());

    TDengineSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Create test data
    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {42, 3.14}});
    rows.push_back({1500000000001, {43, 2.71}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    FormatResult result = plugin.format(block, false);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    (void)ptr;
    assert(ptr != nullptr);
    assert(ptr->total_rows == 2);
    assert(ptr->start_time == 1500000000000);
    assert(ptr->end_time == 1500000000001);

    std::cout << "test_format_sql passed." << std::endl;
}

void test_format_stmt() {
    auto config = create_test_config("stmt");
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());

    TDengineSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Create test data
    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {42, 3.14}});
    rows.push_back({1500000000001, {43, 2.71}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    FormatResult result = plugin.format(block, false);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    (void)ptr;
    assert(ptr != nullptr);
    assert(ptr->total_rows == 2);
    assert(ptr->start_time == 1500000000000);
    assert(ptr->end_time == 1500000000001);

    std::cout << "test_format_stmt passed." << std::endl;
}

void test_prepare() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());

    TDengineSinkPlugin plugin(config, col_instances, tag_instances);

    // Not connected, should throw
    {
        try {
            plugin.prepare();
            assert(false);
        } catch (const std::runtime_error& e) {
            assert(std::string(e.what()) == "TDengineSinkPlugin is not connected");
        }
    }

    // Connect and test prepare
    {
        std::optional<ConnectorSource> conn_src;
        auto connected = plugin.connect_with_source(conn_src);
        (void)connected;
        assert(connected);

        auto prep_ok = plugin.prepare();
        (void)prep_ok;
        assert(prep_ok);
    }

    std::cout << "test_prepare passed." << std::endl;
}

void test_write_operations() {
    // Test SQL write
    {
        auto config = create_test_config("sql");
        auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
        auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());

        TDengineSinkPlugin plugin(config, col_instances, tag_instances);
        auto connected = plugin.connect();
        (void)connected;
        assert(connected);

        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000010000, {10000, 108.00}});
        rows.push_back({1500000010001, {10001, 108.01}});
        batch.table_batches.emplace_back("d0", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 2, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));

        auto result = plugin.format(block);
        assert(std::holds_alternative<InsertFormatResult>(result));
        const auto& ptr = std::get<InsertFormatResult>(result);

        plugin.write(*ptr);
    }

    // Test STMT write
    {
        auto config = create_test_config("stmt");
        auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
        auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());

        TDengineSinkPlugin plugin(config, col_instances, tag_instances);
        auto connected = plugin.connect();
        (void)connected;
        assert(connected);

        auto prepared = plugin.prepare();
        (void)prepared;
        assert(prepared);

        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000010000, {10000, 108.00}});
        rows.push_back({1500000010001, {10001, 108.01}});
        batch.table_batches.emplace_back("d1", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 2, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));

        auto result = plugin.format(block);
        assert(std::holds_alternative<InsertFormatResult>(result));
        const auto& ptr = std::get<InsertFormatResult>(result);

        plugin.write(*ptr);
    }

    // Test unsupported data type
    {
        auto config = create_test_config();
        auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
        auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());

        TDengineSinkPlugin plugin(config, col_instances, tag_instances);
        auto connected = plugin.connect();
        (void)connected;
        assert(connected);

        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000010000, {10000, 108.00}});
        rows.push_back({1500000010001, {10001, 108.01}});
        batch.table_batches.emplace_back("d2", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 2, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));

        BaseInsertData invalid_data(typeid(void), block, col_instances, tag_instances);

        try {
            plugin.write(invalid_data);
            assert(false); // Should not reach here
        } catch (const std::runtime_error& e) {
            assert(std::string(e.what()).find("Unsupported data type") != std::string::npos);
        }
    }

    std::cout << "test_write_operations passed." << std::endl;
}

void test_write_without_connection() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());
    TDengineSinkPlugin plugin(config, col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {42, 3.14}});
    rows.push_back({1500000000001, {43, 2.71}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    auto result = plugin.format(block);
    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    try {
        plugin.write(*ptr);
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "TDengineSinkPlugin is not connected");
    }

    std::cout << "test_write_without_connection passed." << std::endl;
}

void test_retry_mechanism() {
    auto config = create_test_config();
    config.failure_handling.max_retries = 2;
    config.failure_handling.retry_interval_ms = 1;
    config.failure_handling.on_failure = "exit";
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());
    TDengineSinkPlugin plugin(config, col_instances, tag_instances);

    auto connected = plugin.connect();
    (void)connected;
    assert(connected);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {42, 3.14}});
    rows.push_back({1500000000001, {43, 2.71}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    auto result = plugin.format(block);
    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    plugin.write(*ptr);

    std::cout << "test_retry_mechanism skipped (not implemented)." << std::endl;
}

void test_metrics_and_time() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());
    TDengineSinkPlugin plugin(config, col_instances, tag_instances);

    assert(plugin.get_timestamp_precision() == "ms");
    const ActionMetrics& play = plugin.get_play_metrics();
    const ActionMetrics& write = plugin.get_write_metrics();
    (void)play;
    (void)write;
    auto start = plugin.start_write_time();
    auto end = plugin.end_write_time();
    (void)start;
    (void)end;
    assert(end >= start);

    std::cout << "test_metrics_and_time passed." << std::endl;
}

int main() {
    register_tdengine_plugin_config_hooks();
    test_create_tdengine_sink();
    test_constructor();
    test_is_connected();
    test_connection();
    test_format_sql();
    test_format_stmt();
    test_prepare();
    test_write_operations();
    test_write_without_connection();
    test_retry_mechanism();
    test_metrics_and_time();

    std::cout << "All TDengineSinkPlugin tests passed." << std::endl;
    return 0;
}