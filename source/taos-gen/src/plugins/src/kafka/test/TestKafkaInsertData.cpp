#include "FormatResult.hpp"
#include "KafkaInsertData.hpp"
#include <cassert>
#include <cstring>
#include <iostream>

void test_kafka_insert_data_basic() {
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

    // Prepare message batches
    KafkaMessageBatch msg_batch;
    msg_batch.emplace_back("key1", "value1");
    msg_batch.emplace_back("key2", "value2");

    auto base_data = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(msg_batch));
    assert(base_data != nullptr);
    assert(base_data->type == typeid(KafkaInsertData));
    assert(base_data->start_time == 1500000000000);
    assert(base_data->end_time == 1500000000000);
    assert(base_data->total_rows == 1);

    const auto* payload = base_data->payload_as<KafkaInsertData>();
    assert(payload != nullptr);
    (void)payload;
    assert(payload->size() == 2);
    assert((*payload)[0].first == "key1");
    assert((*payload)[0].second == "value1");
    assert((*payload)[1].first == "key2");
    assert((*payload)[1].second == "value2");

    std::cout << "test_kafka_insert_data_basic passed!" << std::endl;
}

void test_kafka_insert_data_move_ctor() {
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

    KafkaMessageBatch msg_batch;
    msg_batch.emplace_back("key", "value");

    auto original = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(msg_batch));
    assert(original != nullptr);
    assert(original->type == typeid(KafkaInsertData));

    BaseInsertData moved(std::move(*original));
    assert(moved.type == typeid(KafkaInsertData));

    const auto* payload = moved.payload_as<KafkaInsertData>();
    assert(payload != nullptr);
    (void)payload;
    assert(payload->size() == 1);
    assert((*payload)[0].first == "key");
    assert((*payload)[0].second == "value");

    std::cout << "test_kafka_insert_data_move_ctor passed!" << std::endl;
}

void test_format_result_with_kafka_insert_data() {
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

    KafkaMessageBatch msg_batch;
    msg_batch.emplace_back("key", "value");

    auto base_data = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(msg_batch));
    assert(base_data != nullptr);
    assert(base_data->type == typeid(KafkaInsertData));

    FormatResult result = std::move(base_data);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<KafkaInsertData>();
        assert(payload != nullptr);
        (void)payload;
        assert(payload->size() == 1);
        assert((*payload)[0].first == "key");
        assert((*payload)[0].second == "value");
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    std::cout << "test_format_result_with_kafka_insert_data passed!" << std::endl;
}

int main() {
    test_kafka_insert_data_basic();
    test_kafka_insert_data_move_ctor();
    test_format_result_with_kafka_insert_data();

    std::cout << "All tests passed!" << std::endl;
    return 0;
}