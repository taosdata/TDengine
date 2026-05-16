#include "MqttSinkPlugin.hpp"
#include "MqttInsertData.hpp"
#include "MqttClient.hpp"

#include <cassert>
#include <iostream>
#include <optional>
#include <memory>
#include <stdexcept>

class MockMqttClient : public IMqttClient {
public:
    bool connected = false;
    size_t publish_count = 0;
    size_t total_rows_published = 0;
    bool fail_connect = false;
    int fail_publish_times = 0;

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

    void disconnect() override {
        connected = false;
    }

    bool publish(const MqttInsertData& data) override {
        publish_count++;

        if (fail_publish_times > 0) {
            fail_publish_times--;
            return false;
        }

        total_rows_published += data.size();
        return true;
    }

    void close() override {
        connected = false;
    }
};

MqttConfig* get_mqtt_config(InsertDataConfig& config) {
    return get_plugin_config_mut<MqttConfig>(config.extensions, "mqtt");
}

MqttFormatOptions* get_mqtt_format_options(InsertDataConfig& config) {
    return get_format_opt_mut<MqttFormatOptions>(config.data_format, "mqtt");
}

InsertDataConfig create_test_config() {
    InsertDataConfig config;

    // Connector Config
    set_plugin_config(config.extensions, "mqtt", MqttConfig{});
    auto* mc = get_mqtt_config(config);
    assert(mc != nullptr);

    auto& conn_conf = *mc;
    conn_conf.uri = "tcp://localhost:1883";
    conn_conf.user = "user";
    conn_conf.password = "pass";
    conn_conf.client_id = "test_client";
    conn_conf.keep_alive = 60;
    conn_conf.clean_session = true;

    // Format Config
    set_format_opt(config.data_format, "mqtt", MqttFormatOptions{});
    auto* mf = get_mqtt_format_options(config);
    assert(mf != nullptr);

    auto& format_conf = *mf;
    format_conf.topic = "factory/{factory_id}/device-{device_id}/data";
    format_conf.qos = 1;
    format_conf.retain = false;
    format_conf.compression = "none";
    format_conf.encoding = "utf8";
    format_conf.content_type = "json";
    format_conf.records_per_message = 10;

    // Failure Handling
    config.failure_handling.max_retries = 1;
    config.failure_handling.retry_interval_ms = 1;
    config.failure_handling.on_failure = "exit";
    return config;
}

ColumnConfigInstanceVector create_col_instances() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});
    return col_instances;
}

void test_create_mqtt_sink() {
    InsertDataConfig config;
    config.target_type = "mqtt";
    set_plugin_config(config.extensions, "mqtt", MqttConfig{});

    config.data_format.format_type = "mqtt";
    set_format_opt(config.data_format, "mqtt", MqttFormatOptions{});

    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    auto plugin = SinkPluginFactory::create_sink_plugin(config, col_instances, tag_instances);
    assert(plugin != nullptr);

    // Verify the created type
    auto* mqtt_sink = dynamic_cast<MqttSinkPlugin*>(plugin.get());
    assert(mqtt_sink != nullptr);
    (void)mqtt_sink;

    std::cout << "test_create_mqtt_sink passed." << std::endl;
}

void test_constructor() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    try {
        MqttSinkPlugin plugin(config, col_instances, tag_instances, 0);
        std::cout << "test_constructor passed." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "test_constructor failed: " << e.what() << std::endl;
        assert(false);
    }
}

void test_is_connected() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    MqttSinkPlugin plugin(config, col_instances, tag_instances, 0);

    bool connected = plugin.is_connected();
    (void)connected;
    assert(!connected);

    std::cout << "test_is_connected passed." << std::endl;
}

void test_connection() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    MqttSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto mock = std::make_unique<MockMqttClient>();
    auto* mock_ptr = mock.get();
    auto* mc = get_mqtt_config(config);
    assert(mc != nullptr);

    auto* mf = get_mqtt_format_options(config);
    assert(mf != nullptr);

    auto mqtt_client = std::make_unique<MqttClient>(*mc, *mf);
    mqtt_client->set_client(std::move(mock));
    plugin.set_client(std::move(mqtt_client));
    assert(plugin.get_client() != nullptr);

    assert(plugin.connect());
    assert(mock_ptr->is_connected());
    (void)mock_ptr;

    // Connect again if already connected
    assert(plugin.connect());
    assert(mock_ptr->is_connected());

    // Disconnect
    plugin.close();

    std::cout << "test_connection passed." << std::endl;
}

void test_connection_failure() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    MqttSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto mock = std::make_unique<MockMqttClient>();
    mock->fail_connect = true;
    auto* mock_ptr = mock.get();
    auto* mc = get_mqtt_config(config);
    assert(mc != nullptr);

    auto* mf = get_mqtt_format_options(config);
    assert(mf != nullptr);

    auto mqtt_client = std::make_unique<MqttClient>(*mc, *mf);
    mqtt_client->set_client(std::move(mock));
    plugin.set_client(std::move(mqtt_client));

    assert(!plugin.connect());
    assert(!mock_ptr->is_connected());
    (void)mock_ptr;

    std::cout << "test_connection_failure passed." << std::endl;
}

void test_format_basic() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    MqttSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Create test data
    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
    rows.push_back({1500000000001, {std::string("f02"), std::string("d02")}});
    batch.table_batches.emplace_back("tb1", std::move(rows));
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

    std::cout << "test_format_basic passed." << std::endl;
}

void test_format_with_payload() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    MqttSinkPlugin plugin(config, col_instances, tag_instances, 0);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
    batch.table_batches.emplace_back("tb1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    FormatResult result = plugin.format(block, false);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    auto* base_ptr = ptr.get();
    assert(base_ptr != nullptr);

    const auto* payload = base_ptr->payload_as<MqttInsertData>();
    (void)payload;
    assert(payload != nullptr);
    assert(payload->size() == 1);

    std::cout << "test_format_with_payload passed." << std::endl;
}

void test_write_operations() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    MqttSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto mock = std::make_unique<MockMqttClient>();
    auto* mock_ptr = mock.get();
    auto* mc = get_mqtt_config(config);
    assert(mc != nullptr);
    auto* mf = get_mqtt_format_options(config);
    assert(mf != nullptr);

    auto mqtt_client = std::make_unique<MqttClient>(*mc, *mf);
    mqtt_client->set_client(std::move(mock));
    plugin.set_client(std::move(mqtt_client));

    auto connected = plugin.connect();
    (void)connected;
    assert(connected);

    // Construct mqtt data
    {
        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
        batch.table_batches.emplace_back("tb1", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 1, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));

        // Create a simple message batch for the test
        MqttMessageBatch msg_batch;
        msg_batch.emplace_back("test/topic", "{\"factory_id\":\"f01\", \"device_id\":\"d01\"}");

        auto base_data = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(msg_batch));
        assert(base_data != nullptr);

        plugin.write(*base_data);
        (void)mock_ptr;
        assert(mock_ptr->publish_count == 1);
        assert(mock_ptr->total_rows_published == 1);
    }

    // Unsupported data type
    {
        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
        batch.table_batches.emplace_back("tb2", std::move(rows));
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

    std::cout << "test_write_operations passed." << std::endl;
}

void test_write_with_retry() {
    auto config = create_test_config();
    config.failure_handling.max_retries = 1;
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    MqttSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto mock = std::make_unique<MockMqttClient>();
    mock->fail_publish_times = 1; // Fail once
    auto* mock_ptr = mock.get();
    auto* mc = get_mqtt_config(config);
    assert(mc != nullptr);

    auto* mf = get_mqtt_format_options(config);
    assert(mf != nullptr);

    auto mqtt_client = std::make_unique<MqttClient>(*mc, *mf);
    mqtt_client->set_client(std::move(mock));
    plugin.set_client(std::move(mqtt_client));

    auto connected = plugin.connect();
    (void)connected;
    assert(connected);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
    batch.table_batches.emplace_back("tb1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    MqttMessageBatch msg_batch;
    msg_batch.emplace_back("test/topic", "{\"factory_id\":\"f01\", \"device_id\":\"d01\"}");

    auto base_data = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(msg_batch));
    assert(base_data != nullptr);

    assert(plugin.write(*base_data));
    assert(mock_ptr->publish_count == 2);           // Called twice: 1 fail + 1 success
    assert(mock_ptr->total_rows_published == 1);    // Only succeeded once

    (void)mock_ptr;

    std::cout << "test_write_with_retry passed." << std::endl;
}

void test_write_without_connection() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    MqttSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto* mc = get_mqtt_config(config);
    assert(mc != nullptr);
    auto* mf = get_mqtt_format_options(config);
    assert(mf != nullptr);

    auto mqtt_client = std::make_unique<MqttClient>(*mc, *mf);
    mqtt_client->set_client(std::make_unique<MockMqttClient>());
    plugin.set_client(std::move(mqtt_client));

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000010000, {std::string("f0"), std::string("d0")}});
    rows.push_back({1500000010001, {std::string("f1"), std::string("d1")}});
    batch.table_batches.emplace_back("d2", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    auto base_data = BaseInsertData::make_with_payload<MqttInsertData>(block, col_instances, tag_instances, {});
    assert(base_data != nullptr);

    try {
        plugin.write(*base_data);
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "MqttSinkPlugin is not connected");
    }

    std::cout << "test_write_without_connection passed." << std::endl;
}

int main() {
    test_create_mqtt_sink();
    test_constructor();
    test_is_connected();
    test_connection();
    test_connection_failure();
    test_format_basic();
    test_format_with_payload();
    test_write_operations();
    test_write_with_retry();
    test_write_without_connection();
    std::cout << "All MqttSinkPlugin tests passed." << std::endl;
    return 0;
}