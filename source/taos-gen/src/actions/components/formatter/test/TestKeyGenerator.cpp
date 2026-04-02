#include "KeyGenerator.hpp"

#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>
#include <stdexcept>
#include <algorithm>

// Helper to create big-endian byte string for verification
template<typename T>
std::string to_big_endian(T value) {
    std::string bytes(sizeof(T), '\0');
    std::memcpy(&bytes[0], &value, sizeof(T));
    uint16_t num = 0x0001;
    bool is_little_endian = *(reinterpret_cast<uint8_t*>(&num)) == 0x01;
    if (is_little_endian) {
        std::reverse(bytes.begin(), bytes.end());
    }
    return bytes;
}

void test_string_serializer() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"device_id", "INT"});
    KeyGenerator kg("prefix/{table}/dev-{device_id}/ts/{ts}", "string-utf8", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1609459200000, {123}});
    batch.table_batches.emplace_back("metrics", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    std::string key = kg.generate(tb, 0);
    assert(key == "prefix/metrics/dev-123/ts/1609459200000");
    std::cout << "test_string_serializer passed." << std::endl;
}

void test_integer_serializer() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"id", "BIGINT"});
    KeyGenerator kg("{id}", "int64", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({0, {static_cast<int64_t>(123456789012345)}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    std::string key = kg.generate(tb, 0);
    std::string expected_key = to_big_endian(static_cast<int64_t>(123456789012345));
    assert(key == expected_key);
    std::cout << "test_integer_serializer passed." << std::endl;
}

void test_invalid_pattern_for_integer_serializer() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"id", "INT"});
    try {
        KeyGenerator kg("prefix-{id}", "int32", col_instances, tag_instances);
        assert(false && "Should have thrown an exception");
    } catch (const std::invalid_argument& e) {
        std::string msg = e.what();
        assert(msg.find("must be a single placeholder") != std::string::npos);
        std::cout << "test_invalid_pattern_for_integer_serializer passed." << std::endl;
    }
}

void test_unsupported_serializer() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    try {
        KeyGenerator kg("{id}", "float32", col_instances, tag_instances);
        assert(false && "Should have thrown an exception");
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()) == "Unsupported key_serializer: float32");
        std::cout << "test_unsupported_serializer passed." << std::endl;
    }
}

void test_integer_parse_error() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"id", "VARCHAR(10)"});
    KeyGenerator kg("{id}", "int32", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({0, {std::string("not-a-number")}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    try {
        kg.generate(tb, 0);
        assert(false && "Should have thrown an exception");
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        assert(msg.find("Failed to parse key value") != std::string::npos);
        std::cout << "test_integer_parse_error passed." << std::endl;
    }
}

void test_key_generation_with_tags() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    // Define columns
    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});

    // Define tags
    tag_instances.emplace_back(ColumnConfig{"region", "VARCHAR(10)"});
    tag_instances.emplace_back(ColumnConfig{"sensor_id", "INT"});

    // Test string serializer with tags
    {
        KeyGenerator kg("region/{region}/sensor/{sensor_id}", "string-utf8", col_instances, tag_instances);

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

        std::string key = kg.generate(tb, 0);
        assert(key == "region/us-west/sensor/1001");
    }

    // Test integer serializer with tag
    {
        KeyGenerator kg("{sensor_id}", "int32", col_instances, tag_instances);

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

        std::string key = kg.generate(tb, 0);
        std::string expected_key = to_big_endian(static_cast<int32_t>(1001));
        assert(key == expected_key);
    }

    std::cout << "test_key_generation_with_tags passed." << std::endl;
}

int main() {
    test_string_serializer();
    test_integer_serializer();
    test_invalid_pattern_for_integer_serializer();
    test_unsupported_serializer();
    test_integer_parse_error();
    test_key_generation_with_tags();
    std::cout << "All KeyGenerator tests passed." << std::endl;
    return 0;
}