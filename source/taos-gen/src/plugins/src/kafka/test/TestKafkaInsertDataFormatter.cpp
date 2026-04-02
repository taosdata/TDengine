#include "FormatterRegistrar.hpp"
#include "KafkaInsertDataFormatter.hpp"
#include <iostream>
#include <cassert>

KafkaFormatOptions* get_kafka_format_options(InsertDataConfig& config) {
    return get_format_opt_mut<KafkaFormatOptions>(config.data_format, "kafka");
}

// Helper to create a base config for tests
InsertDataConfig create_base_kafka_config() {
    InsertDataConfig config;
    config.data_format.format_type = "kafka";

    set_format_opt(config.data_format, "kafka", KafkaFormatOptions{});
    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto& kafka_format = *kf;
    kafka_format.records_per_message = 1;
    kafka_format.value_serializer = "json";
    kafka_format.key_pattern = "{table}-{ts}";
    kafka_format.key_serializer = "string-utf8";
    kafka_format.tbname_key = "table_name";
    return config;
}

void test_kafka_format_json_single_record() {
    auto config = create_base_kafka_config();

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f, 42}});
    rows.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    KafkaInsertDataFormatter formatter(config.data_format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    auto* base_ptr = ptr.get();

    if (!base_ptr) {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    assert(base_ptr->start_time == 1500000000000);
    assert(base_ptr->end_time == 1500000000001);
    assert(base_ptr->total_rows == 2);

    const auto* payload = base_ptr->payload_as<KafkaInsertData>();
    assert(payload != nullptr);
    (void)payload;
    assert(payload->size() == 2);

    // Message 1
    assert((*payload)[0].first == "table1-1500000000000");
    nlohmann::json payload1 = nlohmann::json::parse((*payload)[0].second);
    assert(payload1["ts"] == 1500000000000);
    assert(payload1["f1"] == 3.14f);
    assert(payload1["i1"] == 42);
    assert(payload1["table_name"] == "table1");

    // Message 2
    assert((*payload)[1].first == "table1-1500000000001");
    nlohmann::json payload2 = nlohmann::json::parse((*payload)[1].second);
    assert(payload2["ts"] == 1500000000001);
    assert(payload2["f1"] == 2.71f);
    assert(payload2["i1"] == 43);

    std::cout << "test_kafka_format_json_single_record passed!" << std::endl;
}

void test_kafka_format_json_multiple_records() {
    auto config = create_base_kafka_config();

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);
    kf->records_per_message = 3;

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {1.1f}});
    rows.push_back({1500000000001, {2.2f}});
    rows.push_back({1500000000002, {3.3f}});
    rows.push_back({1500000000003, {4.4f}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 4, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    KafkaInsertDataFormatter formatter(config.data_format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    auto* base_ptr = ptr.get();

    if (!base_ptr) {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    assert(base_ptr->start_time == 1500000000000);
    assert(base_ptr->end_time == 1500000000003);
    assert(base_ptr->total_rows == 4);

    const auto* payload = base_ptr->payload_as<KafkaInsertData>();
    assert(payload != nullptr);
    (void)payload;
    assert(payload->size() == 2);

    // Message 1 (full)
    assert((*payload)[0].first == "table1-1500000000000");
    nlohmann::json payload1 = nlohmann::json::parse((*payload)[0].second);
    assert(payload1.is_array() && payload1.size() == 3);
    assert(payload1[0]["f1"] == 1.1f);
    assert(payload1[1]["f1"] == 2.2f);
    assert(payload1[2]["f1"] == 3.3f);

    // Message 2 (partial)
    assert((*payload)[1].first == "table1-1500000000003");
    nlohmann::json payload2 = nlohmann::json::parse((*payload)[1].second);
    assert(payload2.is_array() && payload2.size() == 1);
    assert(payload2[0]["f1"] == 4.4f);

    std::cout << "test_kafka_format_json_multiple_records passed!" << std::endl;
}


void test_kafka_format_influx_single_record() {
    auto config = create_base_kafka_config();

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);
    kf->value_serializer = "influx";
    kf->records_per_message = 1;

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"device", "BINARY(10)"});
    col_instances.emplace_back(ColumnConfig{"status", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1609459200000, {25.5f, std::string("dev1"), 100}});
    rows.push_back({1609459201000, {26.1f, std::string("dev2"), 101}});
    batch.table_batches.emplace_back("weather", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    KafkaInsertDataFormatter formatter(config.data_format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    auto* base_ptr = ptr.get();

    if (!base_ptr) {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    assert(base_ptr->start_time == 1609459200000);
    assert(base_ptr->end_time == 1609459201000);
    assert(base_ptr->total_rows == 2);

    const auto* payload = base_ptr->payload_as<KafkaInsertData>();
    assert(payload != nullptr);
    (void)payload;
    assert(payload->size() == 2);

    // Message 1
    assert((*payload)[0].first == "weather-1609459200000");
    assert((*payload)[0].second == "weather temp=25.5,device=\"dev1\",status=100i 1609459200000");
    // Message 2
    assert((*payload)[1].first == "weather-1609459201000");
    assert((*payload)[1].second == "weather temp=26.1,device=\"dev2\",status=101i 1609459201000");

    std::cout << "test_kafka_format_influx_single_record passed!" << std::endl;
}

void test_kafka_format_influx_multiple_records() {
    auto config = create_base_kafka_config();

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);
    kf->value_serializer = "influx";
    kf->records_per_message = 2;

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"device", "BINARY(10)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1609459200000, {25.5f, std::string("dev1")}});
    rows.push_back({1609459201000, {26.1f, std::string("dev2")}});
    batch.table_batches.emplace_back("weather", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    KafkaInsertDataFormatter formatter(config.data_format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    auto* base_ptr = ptr.get();

    if (!base_ptr) {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    assert(base_ptr->start_time == 1609459200000);
    assert(base_ptr->end_time == 1609459201000);
    assert(base_ptr->total_rows == 2);

    const auto* payload = base_ptr->payload_as<KafkaInsertData>();
    assert(payload != nullptr);
    assert(payload->size() == 1);

    assert((*payload)[0].first == "weather-1609459200000");
    std::string expected_payload = "weather temp=25.5,device=\"dev1\" 1609459200000\n"
                                   "weather temp=26.1,device=\"dev2\" 1609459201000";
    assert((*payload)[0].second == expected_payload);

    (void)payload;
    (void)expected_payload;

    std::cout << "test_kafka_format_influx_multiple_records passed!" << std::endl;
}

void test_kafka_format_empty_batch() {
    auto config = create_base_kafka_config();
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    (void)block;
    assert(block == nullptr);

    KafkaInsertDataFormatter formatter(config.data_format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result).empty());

    std::cout << "test_kafka_format_empty_batch passed!" << std::endl;
}

void test_kafka_format_invalid_serializer() {
    auto config = create_base_kafka_config();

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);
    kf->value_serializer = "unsupported_format";

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    KafkaInsertDataFormatter formatter(config.data_format);
    formatter.init(config, col_instances, tag_instances);

    try {
        formatter.format(block);
        assert(false && "Should have thrown an exception");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "Unsupported Kafka value_serializer: unsupported_format");
        std::cout << "test_kafka_format_invalid_serializer passed!" << std::endl;
    }
}

void test_kafka_format_factory_creation() {
    DataFormat format;
    format.format_type = "kafka";
    set_format_opt(format, "kafka", KafkaFormatOptions{});

    auto formatter = FormatterFactory::create_formatter<InsertDataConfig>(format);
    assert(formatter != nullptr);

    auto* kafka_formatter = dynamic_cast<KafkaInsertDataFormatter*>(formatter.get());
    assert(kafka_formatter != nullptr);
    (void)kafka_formatter;

    std::cout << "test_kafka_format_factory_creation passed!" << std::endl;
}

void test_kafka_format_with_tags() {
    auto config = create_base_kafka_config();

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);
    kf->key_pattern = "{region}-{table}"; // Use tag in key

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    tag_instances.emplace_back(ColumnConfig{"region", "VARCHAR(10)"});
    tag_instances.emplace_back(ColumnConfig{"sensor_id", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    // Register tags
    std::vector<ColumnType> tag_values = {std::string("us-west"), int32_t(1001)};
    block->tables[0].tags_ptr = pool.register_table_tags("table1", tag_values);

    KafkaInsertDataFormatter formatter(config.data_format);
    formatter.init(config, col_instances, tag_instances);

    // Test JSON format with tags
    {
        FormatResult result = formatter.format(block);
        assert(std::holds_alternative<InsertFormatResult>(result));
        const auto& ptr = std::get<InsertFormatResult>(result);
        auto* base_ptr = ptr.get();

        if (!base_ptr) {
            throw std::runtime_error("Unexpected null BaseInsertData pointer");
        }

        const auto* payload = base_ptr->payload_as<KafkaInsertData>();
        assert(payload != nullptr);
        (void)payload;
        assert(payload->size() == 1);
        assert((*payload)[0].first == "us-west-table1");

        nlohmann::json payload1 = nlohmann::json::parse((*payload)[0].second);
        assert(payload1["ts"] == 1500000000000);
        assert(payload1["f1"] == 3.14f);
        assert(payload1["region"] == "us-west");
        assert(payload1["sensor_id"] == 1001);
        assert(payload1["table_name"] == "table1");
    }

    // Test Influx format with tags
    {
        auto* kf = get_kafka_format_options(config);
        assert(kf != nullptr);
        kf->value_serializer = "influx";
        FormatResult result = formatter.format(block);
        assert(std::holds_alternative<InsertFormatResult>(result));
        const auto& ptr = std::get<InsertFormatResult>(result);
        auto* base_ptr = ptr.get();

        if (!base_ptr) {
            throw std::runtime_error("Unexpected null BaseInsertData pointer");
        }

        const auto* payload = base_ptr->payload_as<KafkaInsertData>();
        assert(payload != nullptr);
        (void)payload;
        assert(payload->size() == 1);
        assert((*payload)[0].first == "us-west-table1");

        std::string expected_payload = "table1,region=us-west,sensor_id=1001 f1=3.14 1500000000000";
        assert((*payload)[0].second == expected_payload);
        (void)payload;
        (void)expected_payload;
    }

    std::cout << "test_kafka_format_with_tags passed!" << std::endl;
}

int main() {
    test_kafka_format_json_single_record();
    test_kafka_format_json_multiple_records();
    test_kafka_format_influx_single_record();
    test_kafka_format_influx_multiple_records();
    test_kafka_format_empty_batch();
    test_kafka_format_invalid_serializer();
    test_kafka_format_factory_creation();
    test_kafka_format_with_tags();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}