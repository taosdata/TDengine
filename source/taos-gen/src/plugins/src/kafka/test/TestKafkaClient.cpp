#include "KafkaClient.hpp"
#include "KafkaInsertData.hpp"
#include "KafkaInsertDataFormatter.hpp"
#include "KafkaConfig.hpp"
#include "InsertDataConfig.hpp"
#include "MemoryPool.hpp"

#include <vector>
#include <string>
#include <cassert>
#include <iostream>
#include <memory>

// Mock implementation of the IKafkaClient interface for testing purposes.
class MockKafkaClient : public IKafkaClient {
public:
    bool connected = false;
    std::vector<std::string> produced_keys;
    std::vector<std::string> produced_payloads;
    bool fail_connect = false;
    bool fail_produce = false;

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

    void close() override {
        connected = false;
    }

    bool produce(const KafkaInsertData& data) override {
        if (fail_produce || !connected) {
            return false;
        }
        for (const auto& [key, payload] : data) {
            produced_keys.push_back(key);
            produced_payloads.push_back(payload);
        }
        return true;
    }
};

// Creates a test configuration for the Kafka connector.
KafkaConfig create_test_connector_config() {
    KafkaConfig config;
    config.bootstrap_servers = "localhost:9092";
    config.topic = "test_topic";
    config.client_id = "test_client";
    return config;
}

// Creates a test configuration for the Kafka data format.
KafkaFormatOptions create_test_format_config() {
    KafkaFormatOptions format;
    format.key_pattern = "{device_id}";
    format.key_serializer = "string-utf8";
    format.value_serializer = "json";
    format.tbname_key = "table";
    format.acks = "1";
    format.compression = "none";
    return format;
}

KafkaConfig* get_kafka_config(InsertDataConfig& config) {
    return get_plugin_config_mut<KafkaConfig>(config.extensions, "kafka");
}

KafkaFormatOptions* get_kafka_format_options(InsertDataConfig& config) {
    return get_format_opt_mut<KafkaFormatOptions>(config.data_format, "kafka");
}

// Creates a full InsertDataConfig for testing.
InsertDataConfig create_test_insert_data_config() {
    InsertDataConfig config;
    set_plugin_config(config.extensions, "kafka", KafkaConfig{});
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    *kc = create_test_connector_config();
    config.data_format.format_type = "kafka";

    set_format_opt(config.data_format, "kafka", KafkaFormatOptions{});
    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);
    *kf = create_test_format_config();
    return config;
}

// Creates KafkaInsertData using the formatter, similar to the MQTT tests.
KafkaInsertData create_test_kafka_data(MemoryPool& pool,
                                       const InsertDataConfig& config,
                                       const ColumnConfigInstanceVector& col_instances,
                                       const ColumnConfigInstanceVector& tag_instances) {
    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
    batch.table_batches.emplace_back("tb1", std::move(rows));
    batch.update_metadata();

    auto* block = pool.convert_to_memory_block(std::move(batch));

    auto formatter = KafkaInsertDataFormatter(config.data_format);
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

    const auto* payload = base_ptr->payload_as<KafkaInsertData>();
    assert(payload != nullptr);
    assert(payload->size() == 1);

    return *payload;
}

void test_connect_and_close() {
    auto connector_config = create_test_connector_config();
    auto format_config = create_test_format_config();

    KafkaClient client(connector_config, format_config);
    client.set_client(std::make_unique<MockKafkaClient>());

    assert(client.connect());
    assert(client.is_connected());
    client.close();
    assert(!client.is_connected());

    std::cout << "test_connect_and_close passed." << std::endl;
}

void test_connect_failure() {
    auto connector_config = create_test_connector_config();
    auto format_config = create_test_format_config();

    KafkaClient client(connector_config, format_config);
    auto mock = std::make_unique<MockKafkaClient>();
    mock->fail_connect = true;
    client.set_client(std::move(mock));

    assert(!client.connect());
    assert(!client.is_connected());

    std::cout << "test_connect_failure passed." << std::endl;
}

void test_execute_and_produce() {
    InsertDataConfig config = create_test_insert_data_config();
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    KafkaClient client(*kc, *kf);
    auto mock = std::make_unique<MockKafkaClient>();
    auto* mock_ptr = mock.get();
    client.set_client(std::move(mock));

    // Prepare mock data
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    KafkaInsertData data = create_test_kafka_data(pool, config, col_instances, tag_instances);

    auto connected = client.connect();
    assert(connected);
    auto success = client.execute(data);
    assert(success);

    // Check produced key and payload
    assert(mock_ptr->produced_keys.size() == 1);
    assert(mock_ptr->produced_payloads.size() == 1);
    assert(mock_ptr->produced_keys[0] == "d01"); // Based on format.key_field
    assert(mock_ptr->produced_payloads[0].find("\"factory_id\":\"f01\"") != std::string::npos);
    assert(mock_ptr->produced_payloads[0].find("\"device_id\":\"d01\"") != std::string::npos);

    (void)connected;
    (void)success;
    (void)mock_ptr;

    std::cout << "test_execute_and_produce passed." << std::endl;
}

void test_execute_not_connected() {
    InsertDataConfig config = create_test_insert_data_config();
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    KafkaClient client(*kc, *kf);
    client.set_client(std::make_unique<MockKafkaClient>());

    // Prepare mock data
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    KafkaInsertData data = create_test_kafka_data(pool, config, col_instances, tag_instances);

    // Not connected
    assert(!client.is_connected());
    assert(!client.execute(data));

    std::cout << "test_execute_not_connected passed." << std::endl;
}

void test_produce_failure() {
    InsertDataConfig config = create_test_insert_data_config();
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    KafkaClient client(*kc, *kf);

    auto mock = std::make_unique<MockKafkaClient>();
    mock->fail_produce = true;
    client.set_client(std::move(mock));

    // Prepare mock data
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    KafkaInsertData data = create_test_kafka_data(pool, config, col_instances, tag_instances);

    auto connected = client.connect();
    assert(connected);
    auto success = client.execute(data);
    assert(!success);

    (void)connected;
    (void)success;

    std::cout << "test_produce_failure passed." << std::endl;
}

int main() {
    test_connect_and_close();
    test_connect_failure();
    test_execute_and_produce();
    test_execute_not_connected();
    test_produce_failure();

    std::cout << "All KafkaClient tests passed." << std::endl;
    return 0;
}