#include "KafkaSinkPlugin.hpp"
#include "KafkaInsertData.hpp"
#include "KafkaClient.hpp"
#include "FormatterRegistrar.hpp"
#include "KafkaInsertDataFormatter.hpp"

#include <cassert>
#include <iostream>
#include <optional>
#include <memory>
#include <stdexcept>

// Mock implementation of the IKafkaClient interface for testing purposes.
class MockKafkaClient : public IKafkaClient {
public:
    bool connected = false;
    size_t produce_count = 0;
    size_t total_rows_produced = 0;
    bool fail_connect = false;
    int fail_produce_times = 0;

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

    bool produce(const KafkaInsertData& data) override {
        produce_count++;

        if (fail_produce_times > 0) {
            fail_produce_times--;
            return false;
        }

        total_rows_produced += data.size();
        return true;
    }
};

KafkaConfig* get_kafka_config(InsertDataConfig& config) {
    return get_plugin_config_mut<KafkaConfig>(config.extensions, "kafka");
}

KafkaFormatOptions* get_kafka_format_options(InsertDataConfig& config) {
    return get_format_opt_mut<KafkaFormatOptions>(config.data_format, "kafka");
}

// Creates a test configuration for the Kafka plugin.
InsertDataConfig create_test_config() {
    InsertDataConfig config;

    // Connector Config
    set_plugin_config(config.extensions, "kafka", KafkaConfig{});
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto& conn_conf = *kc;
    conn_conf.bootstrap_servers = "localhost:9092";
    conn_conf.topic = "test_topic";
    conn_conf.client_id = "test_client";

    // Format Config
    set_format_opt(config.data_format, "kafka", KafkaFormatOptions{});
    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto& format_conf = *kf;
    format_conf.key_pattern = "{device_id}";
    format_conf.key_serializer = "string-utf8";
    format_conf.value_serializer = "json";
    format_conf.tbname_key = "table";
    format_conf.acks = "1";
    format_conf.compression = "none";
    format_conf.records_per_message = 1;

    // Failure Handling
    config.failure_handling.max_retries = 1;
    config.failure_handling.retry_interval_ms = 1;
    config.failure_handling.on_failure = "exit";
    return config;
}

// Creates a vector of column config instances for testing.
ColumnConfigInstanceVector create_col_instances() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});
    return col_instances;
}

void test_create_kafka_sink() {
    InsertDataConfig config;
    config.target_type = "kafka";
    set_plugin_config(config.extensions, "kafka", KafkaConfig{});

    config.data_format.format_type = "kafka";
    set_format_opt(config.data_format, "kafka", KafkaFormatOptions{});

    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    auto plugin = SinkPluginFactory::create_sink_plugin(config, col_instances, tag_instances);
    assert(plugin != nullptr);

    // Verify the created type
    auto* kafka_sink = dynamic_cast<KafkaSinkPlugin*>(plugin.get());
    assert(kafka_sink != nullptr);
    (void)kafka_sink;

    std::cout << "test_create_kafka_sink passed." << std::endl;
}

void test_constructor() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    try {
        KafkaSinkPlugin plugin(config, col_instances, tag_instances, 0);
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

    KafkaSinkPlugin plugin(config, col_instances, tag_instances, 0);

    bool connected = plugin.is_connected();
    (void)connected;
    assert(!connected);

    std::cout << "test_is_connected passed." << std::endl;
}

void test_connection() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    KafkaSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto mock = std::make_unique<MockKafkaClient>();
    auto* mock_ptr = mock.get();
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto kafka_client = std::make_unique<KafkaClient>(*kc, *kf);
    kafka_client->set_client(std::move(mock));
    plugin.set_client(std::move(kafka_client));
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

    KafkaSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto mock = std::make_unique<MockKafkaClient>();
    mock->fail_connect = true;
    auto* mock_ptr = mock.get();
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto kafka_client = std::make_unique<KafkaClient>(*kc, *kf);
    kafka_client->set_client(std::move(mock));
    plugin.set_client(std::move(kafka_client));

    assert(!plugin.connect());
    assert(!mock_ptr->is_connected());
    (void)mock_ptr;

    std::cout << "test_connection_failure passed." << std::endl;
}

void test_format_basic() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    KafkaSinkPlugin plugin(config, col_instances, tag_instances, 0);

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

    KafkaSinkPlugin plugin(config, col_instances, tag_instances, 0);

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

    const auto* payload = base_ptr->payload_as<KafkaInsertData>();
    (void)payload;
    assert(payload != nullptr);
    assert(payload->size() == 1);

    std::cout << "test_format_with_payload passed." << std::endl;
}

void test_write_operations() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    KafkaSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto mock = std::make_unique<MockKafkaClient>();
    auto* mock_ptr = mock.get();
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto kafka_client = std::make_unique<KafkaClient>(*kc, *kf);
    kafka_client->set_client(std::move(mock));
    plugin.set_client(std::move(kafka_client));

    auto prepared = plugin.prepare();
    (void)prepared;
    assert(prepared);

    std::optional<ConnectorSource> conn_src = std::nullopt;
    auto connected = plugin.connect_with_source(conn_src);
    (void)connected;
    assert(connected);

    // Construct kafka data
    {
        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
        batch.table_batches.emplace_back("tb1", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 1, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));

        KafkaMessageBatch msg_batch;
        msg_batch.emplace_back("d01", R"({"device_id":"d01","factory_id":"f01"})");

        auto base_data = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(msg_batch));
        assert(base_data != nullptr);

        plugin.write(*base_data);
        (void)mock_ptr;
        assert(mock_ptr->produce_count == 1);
        assert(mock_ptr->total_rows_produced == 1);
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

    KafkaSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto mock = std::make_unique<MockKafkaClient>();
    mock->fail_produce_times = 1; // Fail once
    auto* mock_ptr = mock.get();
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto kafka_client = std::make_unique<KafkaClient>(*kc, *kf);
    kafka_client->set_client(std::move(mock));
    plugin.set_client(std::move(kafka_client));

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

    KafkaMessageBatch msg_batch;
    msg_batch.emplace_back("d01", R"({"device_id":"d01","factory_id":"f01"})");

    auto base_data = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(msg_batch));
    assert(base_data != nullptr);

    assert(plugin.write(*base_data));
    assert(mock_ptr->produce_count == 2);           // Called twice: 1 fail + 1 success
    assert(mock_ptr->total_rows_produced == 1);     // Only succeeded once

    (void)mock_ptr;

    std::cout << "test_write_with_retry passed." << std::endl;
}

void test_write_without_connection() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};

    KafkaSinkPlugin plugin(config, col_instances, tag_instances, 0);

    // Replace with mock
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto kafka_client = std::make_unique<KafkaClient>(*kc, *kf);
    kafka_client->set_client(std::make_unique<MockKafkaClient>());
    plugin.set_client(std::move(kafka_client));

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000010000, {std::string("f0"), std::string("d0")}});
    rows.push_back({1500000010001, {std::string("f1"), std::string("d1")}});
    batch.table_batches.emplace_back("d2", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    auto base_data = BaseInsertData::make_with_payload<KafkaInsertData>(block, col_instances, tag_instances, {});
    assert(base_data != nullptr);

    try {
        plugin.write(*base_data);
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "KafkaSinkPlugin is not connected");
    }

    std::cout << "test_write_without_connection passed." << std::endl;
}

int main() {
    test_create_kafka_sink();
    test_constructor();
    test_is_connected();
    test_connection();
    test_connection_failure();
    test_format_basic();
    test_format_with_payload();
    test_write_operations();
    test_write_with_retry();
    test_write_without_connection();
    std::cout << "All KafkaSinkPlugin tests passed." << std::endl;
    return 0;
}