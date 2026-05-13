#include "FormatterRegistrar.hpp"
#include "SqlInsertDataFormatter.hpp"
#include "PluginExtensions.hpp"
#include <iostream>
#include <cassert>

void set_tdengine_database(InsertDataConfig& config, const std::string& db_name) {
    set_plugin_config(config.extensions, "tdengine", TDengineConfig{});
    auto* tc = get_plugin_config_mut<TDengineConfig>(config.extensions, "tdengine");
    if (tc) {
        tc->database = db_name;
    }
}

SqlFormatOptions* get_sql_format_options(DataFormat& format) {
    set_format_opt(format, "sql", SqlFormatOptions{});
    return get_format_opt_mut<SqlFormatOptions>(format, "sql");
}

void test_format_insert_data_single_table() {
    DataFormat format;
    format.format_type = "sql";
    auto* sf = get_sql_format_options(format);
    (void)sf;
    assert(sf != nullptr);

    InsertDataConfig config;
    set_tdengine_database(config, "test_db");

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});
    col_instances.emplace_back(ColumnConfig{"s1", "VARCHAR(20)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f, 42, std::string("value1")}});
    rows.push_back({1500000000001, {2.71f, 43, std::string("value2")}});
    batch.table_batches.push_back({"table1", rows});
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    if (block) {
        assert(block->used_tables == 1);
        auto& table_block = block->tables[0];

        // Verify timestamps
        assert(table_block.timestamps[0] == 1500000000000);
        assert(table_block.timestamps[1] == 1500000000001);

        // Verify float column
        float* f1_data = static_cast<float*>(table_block.columns[0].fixed_data);
        (void)f1_data;
        assert(f1_data[0] == 3.14f);
        assert(f1_data[1] == 2.71f);

        // Verify integer column
        int32_t* i1_data = static_cast<int32_t*>(table_block.columns[1].fixed_data);
        (void)i1_data;
        assert(i1_data[0] == 42);
        assert(i1_data[1] == 43);

        // Verify string column
        auto& s1_col = table_block.columns[2];
        assert(s1_col.lengths[0] == 6);
        assert(s1_col.lengths[1] == 6);
        assert(s1_col.var_offsets[0] == 0);
        assert(s1_col.var_offsets[1] == 6);
        assert(s1_col.current_offset == 12);

        std::string value1(s1_col.var_data, 6);
        std::string value2(s1_col.var_data + 6, 6);
        assert(value1 == "value1");
        assert(value2 == "value2");
    }

    SqlInsertDataFormatter formatter(format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<SqlInsertData>();
        (void)payload;
        assert(payload != nullptr);
        assert(payload->str() ==
           "INSERT INTO `table1` VALUES "
           "(1500000000000,3.14,42,'value1')"
           "(1500000000001,2.71,43,'value2');");
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    std::cout << "test_format_insert_data_single_table passed!" << std::endl;
}

void test_format_insert_data_multiple_tables() {
    DataFormat format;
    format.format_type = "sql";
    auto* sf = get_sql_format_options(format);
    (void)sf;
    assert(sf != nullptr);

    InsertDataConfig config;
    set_tdengine_database(config, "test_db");

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    // First table
    std::vector<RowData> rows1;
    rows1.push_back({1500000000000, {3.14f, 42}});
    rows1.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.push_back({"table1", rows1});

    // Second table
    std::vector<RowData> rows2;
    rows2.push_back({1500000000002, {1.23f, 44}});
    rows2.push_back({1500000000003, {4.56f, 45}});
    batch.table_batches.push_back({"table2", rows2});

    batch.update_metadata();

    MemoryPool pool(1, 2, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    (void)block;

    auto formatter = FormatterFactory::create_formatter<InsertDataConfig>(format);
    formatter->init(config, col_instances, tag_instances);
    FormatResult result = formatter->format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<SqlInsertData>();
        (void)payload;
        assert(payload != nullptr);
        assert(payload->str() ==
           "INSERT INTO `table1` VALUES "
           "(1500000000000,3.14,42)"
           "(1500000000001,2.71,43) "
           "`table2` VALUES "
           "(1500000000002,1.23,44)"
           "(1500000000003,4.56,45);");
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }
    std::cout << "test_format_insert_data_multiple_tables passed!" << std::endl;
}

void test_format_insert_data_empty_rows() {
    DataFormat format;
    format.format_type = "sql";
    auto* sf = get_sql_format_options(format);
    (void)sf;
    assert(sf != nullptr);

    InsertDataConfig config;
    set_tdengine_database(config, "test_db");

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    batch.table_batches.push_back({"table1", {}});
    batch.total_rows = 0;

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    SqlInsertDataFormatter formatter(format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == "");
    std::cout << "test_format_insert_data_empty_rows passed!" << std::endl;
}

void test_format_insert_data_different_types() {
    DataFormat format;
    format.format_type = "sql";
    auto* sf = get_sql_format_options(format);
    (void)sf;
    assert(sf != nullptr);

    InsertDataConfig config;
    set_tdengine_database(config, "test_db");

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"b1", "BOOL"});
    col_instances.emplace_back(ColumnConfig{"s1", "NCHAR(20)"});
    col_instances.emplace_back(ColumnConfig{"j1", "JSON"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f, true, std::u16string(u"测试"), JsonValue{ std::string("{\"key\":\"value\"}") }}});
    batch.table_batches.push_back({"table1", rows});
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    SqlInsertDataFormatter formatter(format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<SqlInsertData>();
        (void)payload;
        assert(payload != nullptr);
        assert(payload->str() ==
           "INSERT INTO `table1` VALUES "
           "(1500000000000,3.14,true,'测试','{\"key\":\"value\"}');");
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    std::cout << "test_format_insert_data_different_types passed!" << std::endl;
}

void test_format_insert_data_auto_create_table() {
    DataFormat format;
    format.format_type = "sql";

    auto* sf = get_sql_format_options(format);
    assert(sf != nullptr);
    sf->auto_create_table = true;

    InsertDataConfig config;
    set_tdengine_database(config, "test_db");
    config.schema.name = "meters";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"current", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"voltage", "INT"});

    ColumnConfigInstanceVector tag_instances;
    tag_instances.emplace_back(ColumnConfig{"groupid", "INT"});
    tag_instances.emplace_back(ColumnConfig{"location", "NCHAR(20)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1600000000000, {10.5f, 220}});
    rows.push_back({1600000001000, {11.2f, 221}});
    batch.table_batches.push_back({"d1001", rows});
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    assert(block != nullptr);

    std::vector<ColumnType> tag_values;
    tag_values.emplace_back(1); // groupid
    tag_values.emplace_back(std::u16string(u"北京")); // location
    block->tables[0].tags_ptr = pool.register_table_tags("d1001", tag_values);

    SqlInsertDataFormatter formatter(format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<SqlInsertData>();
        assert(payload != nullptr);

        std::string sql = payload->str();
        std::string expected_prefix = "INSERT INTO `d1001` USING `meters` TAGS (1,'北京') VALUES ";
        std::string expected_values = "(1600000000000,10.5,220)(1600000001000,11.2,221);";
        (void)expected_prefix;
        (void)expected_values;
        assert(sql.find(expected_prefix) != std::string::npos);
        assert(sql.find(expected_values) != std::string::npos);
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    std::cout << "test_format_insert_data_auto_create_table passed!" << std::endl;
}

void test_format_insert_data_multiple_tables_with_tags() {
    DataFormat format;
    format.format_type = "sql";

    auto* sf = get_sql_format_options(format);
    assert(sf != nullptr);
    sf->auto_create_table = true;

    InsertDataConfig config;
    set_tdengine_database(config, "test_db");
    config.schema.name = "sensors";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});

    ColumnConfigInstanceVector tag_instances;
    tag_instances.emplace_back(ColumnConfig{"id", "INT"});

    MultiBatch batch;

    // Table 1
    std::vector<RowData> rows1;
    rows1.push_back({1600000000000, {36.5f}});
    batch.table_batches.push_back({"t1", rows1});

    // Table 2
    std::vector<RowData> rows2;
    rows2.push_back({1600000000000, {37.2f}});
    batch.table_batches.push_back({"t2", rows2});

    batch.update_metadata();

    MemoryPool pool(1, 2, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    assert(block != nullptr);

    std::vector<ColumnType> tags1;
    tags1.emplace_back(101);
    block->tables[0].tags_ptr = pool.register_table_tags("t1", tags1);

    std::vector<ColumnType> tags2;
    tags2.emplace_back(102);
    block->tables[1].tags_ptr = pool.register_table_tags("t2", tags2);

    SqlInsertDataFormatter formatter(format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<SqlInsertData>();
        assert(payload != nullptr);

        std::string sql = payload->str();
        std::string part1 = "`t1` USING `sensors` TAGS (101) VALUES (1600000000000,36.5)";
        std::string part2 = "`t2` USING `sensors` TAGS (102) VALUES (1600000000000,37.2)";
        (void)part1;
        (void)part2;
        (void)sql;
        assert(sql.find("INSERT INTO") == 0);
        assert(sql.find(part1) != std::string::npos);
        assert(sql.find(part2) != std::string::npos);
        assert(sql.back() == ';');
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    std::cout << "test_format_insert_data_multiple_tables_with_tags passed!" << std::endl;
}

int main() {
    test_format_insert_data_single_table();
    test_format_insert_data_multiple_tables();
    test_format_insert_data_empty_rows();
    test_format_insert_data_different_types();
    test_format_insert_data_auto_create_table();
    test_format_insert_data_multiple_tables_with_tags();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}