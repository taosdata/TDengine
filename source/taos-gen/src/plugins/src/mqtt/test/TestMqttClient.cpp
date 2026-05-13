#include "MqttClient.hpp"
#include "MqttInsertData.hpp"
#include "MqttInsertDataFormatter.hpp"
#include "MqttConfig.hpp"

#include <vector>
#include <string>
#include <cassert>
#include <iostream>
#include <memory>

// Mock implementation of the IMqttClient interface for testing purposes.
class MockMqttClient : public IMqttClient {
public:
    bool connected = false;
    std::vector<std::string> published_topics;
    std::vector<std::string> published_payloads;
    bool fail_connect = false;
    bool fail_publish = false;

    bool connect() override {
        if (fail_connect) {
            return false;
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
        if (fail_publish || !connected) {
            return false;
        }
        for (const auto& [topic, payload] : data) {
            published_topics.push_back(topic);
            published_payloads.push_back(payload);
        }
        return true;
    }

    void close() override {
        connected = false;
    }
};

// Creates a test configuration for the MQTT connector.
MqttConfig create_test_connector_config() {
    MqttConfig config;
    config.uri = "tcp://localhost:1883";
    config.user = "user";
    config.password = "pass";
    config.client_id = "test_client";
    config.keep_alive = 60;
    config.clean_session = true;
    config.max_buffered_messages = 1000;
    return config;
}

// Creates a test configuration for the MQTT data format.
MqttFormatOptions create_test_format_config() {
    MqttFormatOptions format;
    format.topic = "factory/{factory_id}/device-{device_id}/data";
    format.qos = 1;
    format.retain = false;
    format.compression = "none";
    format.encoding = "utf8";
    format.content_type = "json";
    return format;
}

MqttConfig* get_mqtt_config(InsertDataConfig& config) {
    return get_plugin_config_mut<MqttConfig>(config.extensions, "mqtt");
}

MqttFormatOptions* get_mqtt_format_options(InsertDataConfig& config) {
    return get_format_opt_mut<MqttFormatOptions>(config.data_format, "mqtt");
}

InsertDataConfig create_test_insert_data_config() {
    InsertDataConfig config;
    set_plugin_config(config.extensions, "mqtt", MqttConfig{});
    auto* mc = get_mqtt_config(config);
    assert(mc != nullptr);

    *mc = create_test_connector_config();
    config.data_format.format_type = "mqtt";
    set_format_opt(config.data_format, "mqtt", MqttFormatOptions{});
    auto* mf = get_mqtt_format_options(config);
    assert(mf != nullptr);
    *mf = create_test_format_config();
    return config;
}

MqttInsertData create_test_mqtt_data(MemoryPool& pool,
                                     const InsertDataConfig& config,
                                     const ColumnConfigInstanceVector& col_instances,
                                     const ColumnConfigInstanceVector& tag_instances) {
    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
    batch.table_batches.emplace_back("tb1", std::move(rows));
    batch.update_metadata();

    auto* block = pool.convert_to_memory_block(std::move(batch));

    auto formatter = MqttInsertDataFormatter(config.data_format);
    formatter.init(config, col_instances, tag_instances);
    auto result = formatter.format(block);
    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    auto* base_ptr = ptr.get();

    if (!base_ptr) {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    assert(base_ptr->start_time == 1500000000000);
    assert(base_ptr->end_time == 1500000000000);
    assert(base_ptr->total_rows == 1);

    const auto* payload = base_ptr->payload_as<MqttInsertData>();
    assert(payload != nullptr);
    assert(payload->size() == 1);

    return *payload;
}

void test_connect_and_close() {
    auto connector_config = create_test_connector_config();
    auto format_config = create_test_format_config();

    MqttClient client(connector_config, format_config);
    client.set_client(std::make_unique<MockMqttClient>());

    assert(client.connect());
    assert(client.is_connected());
    client.close();
    assert(!client.is_connected());

    std::cout << "test_connect_and_close passed." << std::endl;
}

void test_connect_failure() {
    auto connector_config = create_test_connector_config();
    auto format_config = create_test_format_config();

    MqttClient client(connector_config, format_config);
    auto mock = std::make_unique<MockMqttClient>();
    mock->fail_connect = true;
    client.set_client(std::move(mock));

    assert(!client.connect());
    assert(!client.is_connected());

    std::cout << "test_connect_failure passed." << std::endl;
}

void test_execute_and_publish() {
    InsertDataConfig config = create_test_insert_data_config();
    auto* mc = get_mqtt_config(config);
    assert(mc != nullptr);

    auto* mf = get_mqtt_format_options(config);
    assert(mf != nullptr);

    MqttClient client(*mc, *mf);
    auto mock = std::make_unique<MockMqttClient>();
    auto* mock_ptr = mock.get();
    client.set_client(std::move(mock));

    // Prepare mock data
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    MqttInsertData data = create_test_mqtt_data(pool, config, col_instances, tag_instances);

    auto connected = client.connect();
    assert(connected);
    auto success = client.execute(data);
    assert(success);

    // Check published topic and payload
    assert(mock_ptr->published_topics.size() == 1);
    assert(mock_ptr->published_topics[0] == "factory/f01/device-d01/data");
    assert(mock_ptr->published_payloads[0].find("\"factory_id\":\"f01\"") != std::string::npos);
    assert(mock_ptr->published_payloads[0].find("\"device_id\":\"d01\"") != std::string::npos);

    (void)connected;
    (void)success;
    (void)mock_ptr;

    std::cout << "test_execute_and_publish passed." << std::endl;
}

void test_execute_not_connected() {
    InsertDataConfig config = create_test_insert_data_config();
    auto* mc = get_mqtt_config(config);
    assert(mc != nullptr);

    auto* mf = get_mqtt_format_options(config);
    assert(mf != nullptr);

    MqttClient client(*mc, *mf);
    client.set_client(std::make_unique<MockMqttClient>());

    // Prepare mock data
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    MqttInsertData data = create_test_mqtt_data(pool, config, col_instances, tag_instances);

    // Not connected
    assert(!client.is_connected());
    assert(!client.execute(data));

    std::cout << "test_execute_not_connected passed." << std::endl;
}

void test_publish_failure() {
    InsertDataConfig config = create_test_insert_data_config();
    auto* mc = get_mqtt_config(config);
    assert(mc != nullptr);

    auto* mf = get_mqtt_format_options(config);
    assert(mf != nullptr);

    MqttClient client(*mc, *mf);

    auto mock = std::make_unique<MockMqttClient>();
    mock->fail_publish = true;
    client.set_client(std::move(mock));

    // Prepare mock data
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    MqttInsertData data = create_test_mqtt_data(pool, config, col_instances, tag_instances);

    auto connected = client.connect();
    assert(connected);
    auto success = client.execute(data);
    assert(!success);

    (void)connected;
    (void)success;

    std::cout << "test_publish_failure passed." << std::endl;
}

int main() {
    test_connect_and_close();
    test_connect_failure();
    test_execute_and_publish();
    test_execute_not_connected();
    test_publish_failure();

    std::cout << "All MqttClient tests passed." << std::endl;
    return 0;
}