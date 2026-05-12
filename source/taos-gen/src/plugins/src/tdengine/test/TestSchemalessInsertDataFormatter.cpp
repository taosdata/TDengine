#include "FormatterRegistrar.hpp"
#include "SchemalessInsertDataFormatter.hpp"
#include "PluginExtensions.hpp"
#include <iostream>
#include <cassert>

void set_tdengine_database_sml(InsertDataConfig& config, const std::string& db_name) {
    set_plugin_config(config.extensions, "tdengine", TDengineConfig{});
    auto* tc = get_plugin_config_mut<TDengineConfig>(config.extensions, "tdengine");
    if (tc) {
        tc->database = db_name;
    }
}

SchemalessFormatOptions* get_schemaless_format_options(DataFormat& format) {
    set_format_opt(format, "schemaless", SchemalessFormatOptions{});
    return get_format_opt_mut<SchemalessFormatOptions>(format, "schemaless");
}

void test_schemaless_format_single_table() {
    DataFormat format;
    format.format_type = "schemaless";
    auto* sf = get_schemaless_format_options(format);
    (void)sf;
    assert(sf != nullptr);

    InsertDataConfig config;
    config.schema.name = "meters";
    config.timestamp_precision = "ms";
    set_tdengine_database_sml(config, "test_db");

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"current", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"voltage", "INT"});
    tag_instances.emplace_back(ColumnConfig{"location", "VARCHAR(64)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f, 220}});
    rows.push_back({1500000000001, {2.71f, 221}});
    batch.table_batches.push_back({"d1001", rows});
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    assert(block != nullptr);

    std::vector<ColumnType> tag_values;
    tag_values.emplace_back(std::string("Beijing"));
    block->tables[0].tags_ptr = pool.register_table_tags("d1001", tag_values);

    SchemalessInsertDataFormatter formatter(format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<SchemalessInsertData>();
        assert(payload != nullptr);
        assert(payload->total_rows == 2);
        assert(payload->protocol == TSDB_SML_LINE_PROTOCOL);
        assert(payload->precision == TSDB_SML_TIMESTAMP_MILLI_SECONDS);

        // Verify line protocol format
        const std::string& lines = payload->lines;
        // Should contain measurement name "meters"
        assert(lines.find("meters,") == 0);
        // Should contain child table id tag
        assert(lines.find("id=d1001") != std::string::npos);
        // Should contain location tag
        assert(lines.find("location=Beijing") != std::string::npos);
        // Should contain field values
        assert(lines.find("current=") != std::string::npos);
        assert(lines.find("voltage=") != std::string::npos);
        // Should contain timestamps
        assert(lines.find("1500000000000") != std::string::npos);
        assert(lines.find("1500000000001") != std::string::npos);
        // Should have newline separator between rows
        assert(lines.find('\n') != std::string::npos);

        std::cout << "Generated lines:\n" << lines << std::endl;
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    std::cout << "test_schemaless_format_single_table passed!" << std::endl;
}

void test_schemaless_format_multiple_tables() {
    DataFormat format;
    format.format_type = "schemaless";
    auto* sf = get_schemaless_format_options(format);
    (void)sf;
    assert(sf != nullptr);

    InsertDataConfig config;
    config.schema.name = "sensors";
    config.timestamp_precision = "us";
    set_tdengine_database_sml(config, "test_db");

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});
    tag_instances.emplace_back(ColumnConfig{"region", "VARCHAR(32)"});

    MultiBatch batch;
    std::vector<RowData> rows1;
    rows1.push_back({1600000000000000, {36.5f}});
    batch.table_batches.push_back({"t1", rows1});

    std::vector<RowData> rows2;
    rows2.push_back({1600000000000001, {37.2f}});
    batch.table_batches.push_back({"t2", rows2});
    batch.update_metadata();

    MemoryPool pool(1, 2, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    assert(block != nullptr);

    std::vector<ColumnType> tags1;
    tags1.emplace_back(std::string("east"));
    block->tables[0].tags_ptr = pool.register_table_tags("t1", tags1);

    std::vector<ColumnType> tags2;
    tags2.emplace_back(std::string("west"));
    block->tables[1].tags_ptr = pool.register_table_tags("t2", tags2);

    SchemalessInsertDataFormatter formatter(format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<SchemalessInsertData>();
        assert(payload != nullptr);
        assert(payload->total_rows == 2);
        assert(payload->precision == TSDB_SML_TIMESTAMP_MICRO_SECONDS);

        const std::string& lines = payload->lines;
        // Both lines should use "sensors" as measurement
        size_t first_line_end = lines.find('\n');
        assert(first_line_end != std::string::npos);
        std::string line1 = lines.substr(0, first_line_end);
        std::string line2 = lines.substr(first_line_end + 1);

        assert(line1.find("sensors,") == 0);
        assert(line2.find("sensors,") == 0);
        assert(line1.find("id=t1") != std::string::npos);
        assert(line2.find("id=t2") != std::string::npos);

        std::cout << "Generated lines:\n" << lines << std::endl;
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    std::cout << "test_schemaless_format_multiple_tables passed!" << std::endl;
}

void test_schemaless_format_empty_rows() {
    DataFormat format;
    format.format_type = "schemaless";
    auto* sf = get_schemaless_format_options(format);
    (void)sf;
    assert(sf != nullptr);

    InsertDataConfig config;
    config.schema.name = "meters";
    set_tdengine_database_sml(config, "test_db");

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    batch.table_batches.push_back({"table1", {}});
    batch.total_rows = 0;

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    SchemalessInsertDataFormatter formatter(format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == "");
    std::cout << "test_schemaless_format_empty_rows passed!" << std::endl;
}

void test_schemaless_format_via_factory() {
    DataFormat format;
    format.format_type = "schemaless";
    auto* sf = get_schemaless_format_options(format);
    (void)sf;
    assert(sf != nullptr);

    auto formatter = FormatterFactory::create_formatter<InsertDataConfig>(format);
    assert(formatter != nullptr);

    std::cout << "test_schemaless_format_via_factory passed!" << std::endl;
}

int main() {
    test_schemaless_format_single_table();
    test_schemaless_format_multiple_tables();
    test_schemaless_format_empty_rows();
    test_schemaless_format_via_factory();
    std::cout << "All schemaless tests passed!" << std::endl;
    return 0;
}
