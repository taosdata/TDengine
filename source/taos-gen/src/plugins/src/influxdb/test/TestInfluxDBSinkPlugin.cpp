#include "InfluxDBSinkPlugin.hpp"
#include "InfluxDBInsertData.hpp"
#include "InfluxDBClient.hpp"
#include "FormatterRegistrar.hpp"
#include "InfluxDBInsertDataFormatter.hpp"

#include <cassert>
#include <iostream>
#include <optional>
#include <memory>
#include <stdexcept>

// Mock implementation of IInfluxDBClient for testing
class MockInfluxDBClient : public IInfluxDBClient {
public:
    bool connected = false;
    size_t execute_count = 0;
    size_t total_rows_executed = 0;
    bool fail_connect = false;
    int fail_execute_times = 0;

    bool connect() override {
        if (fail_connect) {
            throw std::runtime_error("Simulated connection failure");
        }
        connected = true;
        return true;
    }

    bool is_connected() const override {
        return connected;
    }

    void close() override {
        connected = false;
    }

    bool execute(const InfluxDBInsertData& data) override {
        execute_count++;

        if (fail_execute_times > 0) {
            fail_execute_times--;
            return false;
        }

        total_rows_executed += data.total_rows;
        return true;
    }
};

InfluxDBConfig* get_influxdb_config(InsertDataConfig& config) {
    return get_plugin_config_mut<InfluxDBConfig>(config.extensions, "influxdb");
}

InfluxDBFormatOptions* get_influxdb_format_options(InsertDataConfig& config) {
    return get_format_opt_mut<InfluxDBFormatOptions>(config.data_format, "influxdb");
}

InsertDataConfig create_test_config() {
    InsertDataConfig config;

    set_plugin_config(config.extensions, "influxdb", InfluxDBConfig{});
    auto* ic = get_influxdb_config(config);
    assert(ic != nullptr);
    ic->url = "http://localhost:8086";
    ic->token = "test-token";
    ic->org = "default";
    ic->bucket = "default";

    config.data_format.format_type = "influxdb";
    config.data_format.support_tags = true;
    set_format_opt(config.data_format, "influxdb", InfluxDBFormatOptions{});
    auto* fo = get_influxdb_format_options(config);
    assert(fo != nullptr);
    fo->precision = "ns";
    fo->batch_size = 5000;
    fo->gzip = false;

    config.failure_handling.max_retries = 1;
    config.failure_handling.retry_interval_ms = 1;
    config.failure_handling.on_failure = "exit";
    return config;
}

ColumnConfigInstanceVector create_col_instances() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"usage_idle", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"usage_system", "FLOAT"});
    return col_instances;
}

ColumnConfigInstanceVector create_tag_instances() {
    ColumnConfigInstanceVector tag_instances;
    return tag_instances;
}

void test_create_influxdb_sink() {
    InsertDataConfig config;
    config.target_type = "influxdb";
    set_plugin_config(config.extensions, "influxdb", InfluxDBConfig{});

    config.data_format.format_type = "influxdb";
    config.data_format.support_tags = true;
    set_format_opt(config.data_format, "influxdb", InfluxDBFormatOptions{});

    auto col_instances = create_col_instances();
    auto tag_instances = create_tag_instances();

    auto plugin = SinkPluginFactory::create_sink_plugin(config, col_instances, tag_instances);
    assert(plugin != nullptr);

    auto* influxdb_sink = dynamic_cast<InfluxDBSinkPlugin*>(plugin.get());
    assert(influxdb_sink != nullptr);
    (void)influxdb_sink;

    std::cout << "test_create_influxdb_sink PASSED\n";
}

void test_constructor() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = create_tag_instances();

    try {
        InfluxDBSinkPlugin plugin(config, col_instances, tag_instances, 0);
        std::cout << "test_constructor PASSED\n";
    } catch (const std::exception& e) {
        std::cerr << "test_constructor failed: " << e.what() << std::endl;
        assert(false);
    }
}

void test_is_connected() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = create_tag_instances();

    InfluxDBSinkPlugin plugin(config, col_instances, tag_instances, 0);

    bool connected = plugin.is_connected();
    (void)connected;
    assert(!connected);

    std::cout << "test_is_connected PASSED\n";
}

void test_connection() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = create_tag_instances();

    InfluxDBSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto mock = std::make_unique<MockInfluxDBClient>();
    auto* mock_ptr = mock.get();
    auto* ic = get_influxdb_config(config);
    assert(ic != nullptr);
    auto* fo = get_influxdb_format_options(config);
    assert(fo != nullptr);

    auto influxdb_client = std::make_unique<InfluxDBClient>(*ic, *fo);
    influxdb_client->set_client(std::move(mock));
    plugin.set_client(std::move(influxdb_client));
    assert(plugin.get_client() != nullptr);

    assert(plugin.connect());
    assert(mock_ptr->is_connected());
    (void)mock_ptr;

    // Connect again if already connected
    assert(plugin.connect());
    assert(mock_ptr->is_connected());

    // Disconnect
    plugin.close();

    std::cout << "test_connection PASSED\n";
}

void test_connection_failure() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = create_tag_instances();

    InfluxDBSinkPlugin plugin(config, col_instances, tag_instances, 0);

    auto mock = std::make_unique<MockInfluxDBClient>();
    mock->fail_connect = true;
    auto* mock_ptr = mock.get();
    auto* ic = get_influxdb_config(config);
    assert(ic != nullptr);
    auto* fo = get_influxdb_format_options(config);
    assert(fo != nullptr);

    auto influxdb_client = std::make_unique<InfluxDBClient>(*ic, *fo);
    influxdb_client->set_client(std::move(mock));
    plugin.set_client(std::move(influxdb_client));

    assert(!plugin.connect());
    assert(!mock_ptr->is_connected());
    (void)mock_ptr;

    std::cout << "test_connection_failure PASSED\n";
}

void test_format_basic() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = create_tag_instances();

    InfluxDBSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Create test data
    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1000000000, {50.5f, 10.2f}});
    rows.push_back({2000000000, {60.3f, 15.1f}});
    batch.table_batches.emplace_back("host_0", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    FormatResult result = plugin.format(block, false);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    (void)ptr;
    assert(ptr != nullptr);
    assert(ptr->total_rows == 2);
    assert(ptr->start_time == 1000000000);
    assert(ptr->end_time == 2000000000);

    std::cout << "test_format_basic PASSED\n";
}

void test_format_with_payload() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = create_tag_instances();

    InfluxDBSinkPlugin plugin(config, col_instances, tag_instances, 0);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1000000000, {50.5f, 10.2f}});
    batch.table_batches.emplace_back("host_0", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    FormatResult result = plugin.format(block, false);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    auto* base_ptr = ptr.get();
    assert(base_ptr != nullptr);

    const auto* payload = base_ptr->payload_as<InfluxDBInsertData>();
    (void)payload;
    assert(payload != nullptr);
    assert(!payload->lines.empty());
    assert(payload->total_rows == 1);

    std::cout << "test_format_with_payload PASSED\n";
}

void test_write_operations() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = create_tag_instances();

    InfluxDBSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto mock = std::make_unique<MockInfluxDBClient>();
    auto* mock_ptr = mock.get();
    auto* ic = get_influxdb_config(config);
    assert(ic != nullptr);
    auto* fo = get_influxdb_format_options(config);
    assert(fo != nullptr);

    auto influxdb_client = std::make_unique<InfluxDBClient>(*ic, *fo);
    influxdb_client->set_client(std::move(mock));
    plugin.set_client(std::move(influxdb_client));

    auto prepared = plugin.prepare();
    (void)prepared;
    assert(prepared);

    std::optional<ConnectorSource> conn_src = std::nullopt;
    auto connected = plugin.connect_with_source(conn_src);
    (void)connected;
    assert(connected);

    // Write InfluxDB data
    {
        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1000000000, {50.5f, 10.2f}});
        batch.table_batches.emplace_back("host_0", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 1, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));

        InfluxDBInsertData payload("cpu,host=server01 usage_idle=50.5,usage_system=10.2 1000000000", 1);

        auto base_data = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(payload));
        assert(base_data != nullptr);

        plugin.write(*base_data);
        (void)mock_ptr;
        assert(mock_ptr->execute_count == 1);
        assert(mock_ptr->total_rows_executed == 1);
    }

    // Unsupported data type
    {
        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1000000000, {50.5f, 10.2f}});
        batch.table_batches.emplace_back("host_1", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 1, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));
        BaseInsertData invalid_data(typeid(void), block, col_instances, tag_instances);

        try {
            plugin.write(invalid_data);
            assert(false);
        } catch (const std::runtime_error& e) {
            assert(std::string(e.what()).find("Unsupported data type") != std::string::npos);
        }
    }

    std::cout << "test_write_operations PASSED\n";
}

void test_write_with_retry() {
    auto config = create_test_config();
    config.failure_handling.max_retries = 1;
    auto col_instances = create_col_instances();
    auto tag_instances = create_tag_instances();

    InfluxDBSinkPlugin plugin(config, col_instances, tag_instances, 0);

    auto mock = std::make_unique<MockInfluxDBClient>();
    mock->fail_execute_times = 1;
    auto* mock_ptr = mock.get();
    auto* ic = get_influxdb_config(config);
    assert(ic != nullptr);
    auto* fo = get_influxdb_format_options(config);
    assert(fo != nullptr);

    auto influxdb_client = std::make_unique<InfluxDBClient>(*ic, *fo);
    influxdb_client->set_client(std::move(mock));
    plugin.set_client(std::move(influxdb_client));

    auto connected = plugin.connect();
    (void)connected;
    assert(connected);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1000000000, {50.5f, 10.2f}});
    batch.table_batches.emplace_back("host_0", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    InfluxDBInsertData payload("cpu,host=server01 usage_idle=50.5,usage_system=10.2 1000000000", 1);

    auto base_data = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(payload));
    assert(base_data != nullptr);

    assert(plugin.write(*base_data));
    assert(mock_ptr->execute_count == 2);           // Called twice: 1 fail + 1 success
    assert(mock_ptr->total_rows_executed == 1);     // Only succeeded once

    (void)mock_ptr;

    std::cout << "test_write_with_retry PASSED\n";
}

void test_write_without_connection() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = create_tag_instances();

    InfluxDBSinkPlugin plugin(config, col_instances, tag_instances, 0);

    auto* ic = get_influxdb_config(config);
    assert(ic != nullptr);
    auto* fo = get_influxdb_format_options(config);
    assert(fo != nullptr);

    auto influxdb_client = std::make_unique<InfluxDBClient>(*ic, *fo);
    influxdb_client->set_client(std::make_unique<MockInfluxDBClient>());
    plugin.set_client(std::move(influxdb_client));

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1000000000, {50.5f, 10.2f}});
    rows.push_back({2000000000, {60.3f, 15.1f}});
    batch.table_batches.emplace_back("host_0", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    auto base_data = BaseInsertData::make_with_payload<InfluxDBInsertData>(block, col_instances, tag_instances, {"", 0});
    assert(base_data != nullptr);

    try {
        plugin.write(*base_data);
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "InfluxDBSinkPlugin is not connected");
    }

    std::cout << "test_write_without_connection PASSED\n";
}

int main() {
    test_create_influxdb_sink();
    test_constructor();
    test_is_connected();
    test_connection();
    test_connection_failure();
    test_format_basic();
    test_format_with_payload();
    test_write_operations();
    test_write_with_retry();
    test_write_without_connection();
    std::cout << "\nAll InfluxDBSinkPlugin tests PASSED\n";
    return 0;
}
