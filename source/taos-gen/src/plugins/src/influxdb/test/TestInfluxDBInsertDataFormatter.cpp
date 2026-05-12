#include "FormatterRegistrar.hpp"
#include "InfluxDBInsertDataFormatter.hpp"
#include <iostream>
#include <cassert>

InsertDataConfig create_base_influxdb_config(const std::string& schema_name = "test_measurement") {
    InsertDataConfig config;
    config.schema.name = schema_name;
    config.data_format.format_type = "influxdb";
    set_format_opt(config.data_format, "influxdb", InfluxDBFormatOptions{});
    return config;
}

void test_influxdb_format_basic() {
    auto config = create_base_influxdb_config();

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"temperature", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"humidity", "INT"});
    tag_instances.emplace_back(ColumnConfig{"location", "BINARY(24)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f, 42}});
    rows.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.push_back({"sensor_01", std::move(rows)});
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    // Register tags
    std::vector<ColumnType> tag_values;
    tag_values.emplace_back(std::string("Beijing"));
    block->tables[0].tags_ptr = pool.register_table_tags("sensor_01", tag_values);

    InfluxDBInsertDataFormatter formatter(config.data_format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    auto* base_ptr = ptr.get();
    assert(base_ptr != nullptr);
    assert(base_ptr->total_rows == 2);

    const auto* payload = base_ptr->payload_as<InfluxDBInsertData>();
    assert(payload != nullptr);
    assert(payload->total_rows == 2);
    assert(!payload->lines.empty());

    // Verify line protocol format - measurement should be schema name, not table name
    assert(payload->lines.find("test_measurement,") != std::string::npos);
    assert(payload->lines.find("sensor_01") == std::string::npos);
    assert(payload->lines.find("location=Beijing") != std::string::npos);
    assert(payload->lines.find("temperature=") != std::string::npos);
    assert(payload->lines.find("humidity=42i") != std::string::npos);
    assert(payload->lines.find("1500000000000") != std::string::npos);

    std::cout << "Generated line protocol:\n" << payload->lines << std::endl;
    std::cout << "test_influxdb_format_basic PASSED\n";
}

void test_influxdb_format_multiple_tables() {
    auto config = create_base_influxdb_config();

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"value", "FLOAT"});
    tag_instances.emplace_back(ColumnConfig{"host", "BINARY(32)"});

    MultiBatch batch;
    {
        std::vector<RowData> rows;
        rows.push_back({1000000, {1.0f}});
        batch.table_batches.push_back({"server1", std::move(rows)});
    }
    {
        std::vector<RowData> rows;
        rows.push_back({2000000, {2.0f}});
        batch.table_batches.push_back({"server2", std::move(rows)});
    }
    batch.update_metadata();

    MemoryPool pool(2, 2, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    std::vector<ColumnType> tag_a = {std::string("host-a")};
    std::vector<ColumnType> tag_b = {std::string("host-b")};
    block->tables[0].tags_ptr = pool.register_table_tags("server1", tag_a);
    block->tables[1].tags_ptr = pool.register_table_tags("server2", tag_b);

    InfluxDBInsertDataFormatter formatter(config.data_format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);
    const auto* payload = ptr->payload_as<InfluxDBInsertData>();
    (void)payload;
    assert(payload != nullptr);
    assert(payload->total_rows == 2);
    // Measurement should be schema name for all tables
    assert(payload->lines.find("test_measurement,") != std::string::npos);
    assert(payload->lines.find("host=host-a") != std::string::npos);
    assert(payload->lines.find("host=host-b") != std::string::npos);
    // Table names should NOT appear as measurement names
    assert(payload->lines.find("server1") == std::string::npos);
    assert(payload->lines.find("server2") == std::string::npos);

    std::cout << "test_influxdb_format_multiple_tables PASSED\n";
}

void test_influxdb_format_empty_batch() {
    auto config = create_base_influxdb_config();

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"v", "FLOAT"});

    InfluxDBInsertDataFormatter formatter(config.data_format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(nullptr);

    assert(std::holds_alternative<std::string>(result));
    std::cout << "test_influxdb_format_empty_batch PASSED\n";
}

void test_influxdb_formatter_factory() {
    auto config = create_base_influxdb_config();

    auto formatter = FormatterFactory::create_formatter<InsertDataConfig>(config.data_format);
    assert(formatter != nullptr);
    std::cout << "test_influxdb_formatter_factory PASSED\n";
}

int main() {
    test_influxdb_format_basic();
    test_influxdb_format_multiple_tables();
    test_influxdb_format_empty_batch();
    test_influxdb_formatter_factory();
    std::cout << "\nAll InfluxDB formatter tests PASSED\n";
    return 0;
}
