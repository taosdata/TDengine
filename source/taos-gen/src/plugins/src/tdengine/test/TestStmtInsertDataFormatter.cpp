#include "DataFormat.hpp"
#include "FormatterRegistrar.hpp"
#include "StmtInsertDataFormatter.hpp"
#include "StmtContext.hpp"
#include <iostream>
#include <cassert>

TDengineConfig* get_tdengine_config(InsertDataConfig& config) {
    set_plugin_config(config.extensions, "tdengine", TDengineConfig{});
    return get_plugin_config_mut<TDengineConfig>(config.extensions, "tdengine");
}

StmtFormatOptions* get_stmt_format_options(DataFormat& format) {
    set_format_opt(format, "stmt", StmtFormatOptions{});
    return get_format_opt_mut<StmtFormatOptions>(format, "stmt");
}

void test_stmt_prepare_subtable() {
    DataFormat format;
    format.format_type = "stmt";

    auto* sf = get_stmt_format_options(format);
    assert(sf != nullptr);

    sf->version = "v2";
    sf->auto_create_table = false;

    InsertDataConfig config;
    auto* tc = get_tdengine_config(config);
    assert(tc != nullptr);
    tc->database = "test_db";
    tc->protocol_type = TDengineConfig::ProtocolType::Native;

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    ColumnConfigInstanceVector tag_instances;

    StmtInsertDataFormatter formatter(format);
    auto ctx = formatter.init(config, col_instances, tag_instances);
    const auto* stmt = dynamic_cast<const StmtContext*>(ctx.get());
    (void)stmt;
    assert(stmt != nullptr);

    std::string expected = "INSERT INTO ? VALUES(?,?,?)";
    assert(stmt->sql == expected);
    std::cout << "test_stmt_prepare_subtable passed!" << std::endl;
}

void test_stmt_prepare_supertable_websocket() {
    DataFormat format;
    format.format_type = "stmt";

    auto* sf = get_stmt_format_options(format);
    assert(sf != nullptr);
    sf->version = "v2";
    sf->auto_create_table = false;

    InsertDataConfig config;
    auto* tc = get_tdengine_config(config);
    assert(tc != nullptr);
    tc->database = "test_db";
    tc->protocol_type = TDengineConfig::ProtocolType::WebSocket;
    config.schema.name = "test_stb";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    ColumnConfigInstanceVector tag_instances;

    StmtInsertDataFormatter formatter(format);
    auto ctx = formatter.init(config, col_instances, tag_instances);
    const auto* stmt = dynamic_cast<const StmtContext*>(ctx.get());
    (void)stmt;
    assert(stmt != nullptr);

    std::string expected = "INSERT INTO `test_stb`(tbname,ts,`f1`,`i1`) VALUES(?,?,?,?)";
    assert(stmt->sql == expected);
    std::cout << "test_stmt_prepare_supertable_websocket passed!" << std::endl;
}

void test_stmt_prepare_auto_create_table() {
    DataFormat format;
    format.format_type = "stmt";

    auto* sf = get_stmt_format_options(format);
    assert(sf != nullptr);
    sf->version = "v2";
    sf->auto_create_table = true;

    InsertDataConfig config;
    auto* tc = get_tdengine_config(config);
    assert(tc != nullptr);
    tc->database = "test_db";
    config.schema.name = "test_stb";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"c1", "INT"});

    ColumnConfigInstanceVector tag_instances;
    tag_instances.emplace_back(ColumnConfig{"t1", "INT"});
    tag_instances.emplace_back(ColumnConfig{"t2", "VARCHAR(10)"});

    StmtInsertDataFormatter formatter(format);
    auto ctx = formatter.init(config, col_instances, tag_instances);
    const auto* stmt = dynamic_cast<const StmtContext*>(ctx.get());
    (void)stmt;
    assert(stmt != nullptr);

    std::string expected = "INSERT INTO ? USING `test_stb` TAGS (?,?) VALUES(?,?)";
    assert(stmt->sql == expected);
    std::cout << "test_stmt_prepare_auto_create_table passed!" << std::endl;
}

void test_stmt_format_insert_data_single_table() {
    DataFormat format;
    format.format_type = "stmt";

    auto* sf = get_stmt_format_options(format);
    assert(sf != nullptr);
    sf->version = "v2";

    InsertDataConfig config;
    auto* tc = get_tdengine_config(config);
    assert(tc != nullptr);
    tc->database = "test_db";

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

    StmtInsertDataFormatter formatter(format);
    formatter.init(config, col_instances, tag_instances);
    FormatResult result = formatter.format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<StmtV2InsertData>();
        assert(payload != nullptr);
        (void)payload;

        assert(base_ptr->start_time == 1500000000000);
        assert(base_ptr->end_time == 1500000000001);
        assert(base_ptr->total_rows == 2);
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    std::cout << "test_stmt_format_insert_data_single_table passed!" << std::endl;
}

void test_stmt_format_insert_data_multiple_tables() {
    DataFormat format;
    format.format_type = "stmt";

    auto* sf = get_stmt_format_options(format);
    assert(sf != nullptr);
    sf->version = "v2";

    InsertDataConfig config;
    auto* tc = get_tdengine_config(config);
    assert(tc != nullptr);
    tc->database = "test_db";

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    // First table
    std::vector<RowData> rows1;
    rows1.push_back({1500000000000, {3.14f, 42}});
    rows1.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.emplace_back("table1", std::move(rows1));

    // Second table
    std::vector<RowData> rows2;
    rows2.push_back({1500000000002, {1.23f, 44}});
    rows2.push_back({1500000000003, {4.56f, 45}});
    batch.table_batches.emplace_back("table2", std::move(rows2));

    batch.update_metadata();

    MemoryPool pool(1, 2, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    auto formatter = FormatterFactory::create_formatter<InsertDataConfig>(format);
    formatter->init(config, col_instances, tag_instances);
    FormatResult result = formatter->format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<StmtV2InsertData>();
        assert(payload != nullptr);
        (void)payload;

        assert(base_ptr->start_time == 1500000000000);
        assert(base_ptr->end_time == 1500000000003);
        assert(base_ptr->total_rows == 4);
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    std::cout << "test_stmt_format_insert_data_multiple_tables passed!" << std::endl;
}

void test_stmt_format_insert_data_empty_batch() {
    DataFormat format;
    format.format_type = "stmt";

    auto* sf = get_stmt_format_options(format);
    assert(sf != nullptr);
    sf->version = "v2";

    InsertDataConfig config;
    auto* tc = get_tdengine_config(config);
    assert(tc != nullptr);
    tc->database = "test_db";

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    (void)block;
    assert(block == nullptr);

    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    block = pool.convert_to_memory_block(std::move(batch));
    assert(block != nullptr);

    // block->tables.clear();
    // StmtInsertDataFormatter formatter(format);
    // FormatResult result = formatter.format(config, col_instances, tag_instances, block);

    // assert(std::holds_alternative<std::string>(result));
    // assert(std::get<std::string>(result).empty());
    std::cout << "test_stmt_format_insert_data_empty_batch passed!" << std::endl;
}

void test_stmt_format_insert_data_invalid_version() {
    DataFormat format;
    format.format_type = "stmt";

    auto* sf = get_stmt_format_options(format);
    assert(sf != nullptr);
    sf->version = "v1";  // Unsupported version

    InsertDataConfig config;
    auto* tc = get_tdengine_config(config);
    assert(tc != nullptr);
    tc->database = "test_db";

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

    StmtInsertDataFormatter formatter(format);
    formatter.init(config, col_instances, tag_instances);

    try {
        formatter.format(block);
        assert(false && "Should throw exception for invalid version");
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()) == "Unsupported stmt version: v1");
        std::cout << "test_stmt_format_insert_data_invalid_version passed!" << std::endl;
    }
}

void test_stmt_format_insert_data_with_empty_rows() {
    DataFormat format;
    format.format_type = "stmt";

    auto* sf = get_stmt_format_options(format);
    assert(sf != nullptr);
    sf->version = "v2";

    InsertDataConfig config;
    auto* tc = get_tdengine_config(config);
    assert(tc != nullptr);
    tc->database = "test_db";

    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    // Table with data
    std::vector<RowData> rows1;
    rows1.push_back({1500000000000, {3.14f, 42}});
    rows1.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.emplace_back("table1", std::move(rows1));

    // Table without data
    std::vector<RowData> empty_rows;
    batch.table_batches.emplace_back("table2", std::move(empty_rows));

    // Another table with data
    std::vector<RowData> rows3;
    rows3.push_back({1500000000002, {1.23f, 44}});
    batch.table_batches.emplace_back("table3", std::move(rows3));

    batch.update_metadata();

    MemoryPool pool(1, 3, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    auto formatter = FormatterFactory::create_formatter<InsertDataConfig>(format);
    formatter->init(config, col_instances, tag_instances);
    FormatResult result = formatter->format(block);

    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& ptr = std::get<InsertFormatResult>(result);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<StmtV2InsertData>();
        assert(payload != nullptr);
        (void)payload;

        // Verify the timing information excludes empty table
        assert(base_ptr->start_time == 1500000000000);
        assert(base_ptr->end_time == 1500000000002);

        // Verify total rows only counts non-empty tables
        assert(base_ptr->total_rows == 3);  // 2 rows from table1 + 1 row from table3
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    std::cout << "test_stmt_format_insert_data_with_empty_rows passed!" << std::endl;
}

int main() {
    test_stmt_prepare_subtable();
    test_stmt_prepare_supertable_websocket();
    test_stmt_prepare_auto_create_table();

    test_stmt_format_insert_data_single_table();
    test_stmt_format_insert_data_multiple_tables();
    test_stmt_format_insert_data_empty_batch();
    test_stmt_format_insert_data_invalid_version();
    test_stmt_format_insert_data_with_empty_rows();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}