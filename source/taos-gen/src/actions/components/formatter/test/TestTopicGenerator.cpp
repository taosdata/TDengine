#include "TopicGenerator.hpp"
#include <cassert>
#include <iostream>
#include <vector>
#include <stdexcept>

void test_basic_pattern() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "INT"});
    TopicGenerator tg("factory/{factory_id}/device-{device_id}/data", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {std::string("f01"), 101}});
    rows.push_back({1500000000001, {std::string("f02"), 102}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    std::string topic0 = tg.generate(tb, 0);
    std::string topic1 = tg.generate(tb, 1);
    assert(topic0 == "factory/f01/device-101/data");
    assert(topic1 == "factory/f02/device-102/data");
    std::cout << "test_basic_pattern passed." << std::endl;
}

void test_special_placeholders() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    TopicGenerator tg("table/{table}/ts/{ts}", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {}});
    batch.table_batches.emplace_back("my_table", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    auto& tb = block->tables[0];

    std::string topic = tg.generate(tb, 0);
    assert(topic == "table/my_table/ts/1500000000000");

    tb.table_name = nullptr;
    topic = tg.generate(tb, 0);
    assert(topic.find("UNKNOWN_TABLE") != std::string::npos);

    tb.timestamps = nullptr;
    topic = tg.generate(tb, 0);
    assert(topic.find("INVALID_TS") != std::string::npos);

    std::cout << "test_special_placeholders passed." << std::endl;
}

void test_col_not_found() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"a", "VARCHAR(16)"});
    TopicGenerator tg("prefix/{b}/suffix", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {std::string("x")}});
    batch.table_batches.emplace_back("my_table", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    std::string topic = tg.generate(tb, 0);
    assert(topic == "prefix/{COL_NOT_FOUND:b}/suffix");
    std::cout << "test_col_not_found passed." << std::endl;
}

void test_pattern_edge_cases() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"x", "VARCHAR(16)"});

    TopicGenerator tg1("{x}{x}", col_instances, tag_instances);
    TopicGenerator tg2("plain/text", col_instances, tag_instances);
    TopicGenerator tg3("{x}tail", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {std::string("A")}});
    batch.table_batches.emplace_back("my_table", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    auto tp1 = tg1.generate(tb, 0);
    auto tp2 = tg2.generate(tb, 0);
    auto tp3 = tg3.generate(tb, 0);
    (void)tp1;
    (void)tp2;
    (void)tp3;
    assert(tp1 == "AA");
    assert(tp2 == "plain/text");
    assert(tp3 == "Atail");

    std::cout << "test_pattern_edge_cases passed." << std::endl;
}

void test_topic_generation_with_tags() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    // Define columns
    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});

    // Define tags
    tag_instances.emplace_back(ColumnConfig{"region", "VARCHAR(10)"});
    tag_instances.emplace_back(ColumnConfig{"sensor_id", "INT"});

    TopicGenerator tg("region/{region}/sensor/{sensor_id}/data", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {25.5f}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    // Register and assign tags to the table block
    std::vector<ColumnType> tag_values = {std::string("us-west"), int32_t(1001)};
    block->tables[0].tags_ptr = pool.register_table_tags("t1", tag_values);

    const auto& tb = block->tables[0];

    std::string topic = tg.generate(tb, 0);
    assert(topic == "region/us-west/sensor/1001/data");

    std::cout << "test_topic_generation_with_tags passed." << std::endl;
}

int main() {
    test_basic_pattern();
    test_special_placeholders();
    test_col_not_found();
    test_pattern_edge_cases();
    test_topic_generation_with_tags();
    std::cout << "All TopicGenerator tests passed." << std::endl;
    return 0;
}