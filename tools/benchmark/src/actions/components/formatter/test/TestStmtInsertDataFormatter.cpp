#include <iostream>
#include <cassert>
#include "FormatterRegistrar.h"
#include "StmtInsertDataFormatter.h"

void test_stmt_format_insert_data_single_table() {
    DataFormat format;
    format.format_type = "stmt";
    format.stmt_config.version = "v2";

    InsertDataConfig config;
    config.target.tdengine.database_info.name = "test_db";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f, 42}});
    rows.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.start_time = 1500000000000;
    batch.end_time = 1500000000001;
    batch.total_rows = 2;

    StmtInsertDataFormatter formatter(format);
    FormatResult result = formatter.format(config, col_instances, std::move(batch));

    assert(std::holds_alternative<StmtV2InsertData>(result));
    const auto& stmt_data = std::get<StmtV2InsertData>(result);
    assert(stmt_data.start_time == 1500000000000);
    assert(stmt_data.end_time == 1500000000001);
    assert(stmt_data.total_rows == 2);
    std::cout << "test_stmt_format_insert_data_single_table passed!" << std::endl;
}

void test_stmt_format_insert_data_multiple_tables() {
    DataFormat format;
    format.format_type = "stmt";
    format.stmt_config.version = "v2";

    InsertDataConfig config;
    config.target.tdengine.database_info.name = "test_db";

    ColumnConfigInstanceVector col_instances;
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

    batch.start_time = 1500000000000;
    batch.end_time = 1500000000003;
    batch.total_rows = 4;

    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(format);
    FormatResult result = formatter->format(config, col_instances, std::move(batch));

    assert(std::holds_alternative<StmtV2InsertData>(result));
    const auto& stmt_data = std::get<StmtV2InsertData>(result);
    assert(stmt_data.start_time == 1500000000000);
    assert(stmt_data.end_time == 1500000000003);
    assert(stmt_data.total_rows == 4);
    std::cout << "test_stmt_format_insert_data_multiple_tables passed!" << std::endl;
}

void test_stmt_format_insert_data_empty_batch() {
    DataFormat format;
    format.format_type = "stmt";
    format.stmt_config.version = "v2";

    InsertDataConfig config;
    config.target.tdengine.database_info.name = "test_db";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;

    StmtInsertDataFormatter formatter(format);
    FormatResult result = formatter.format(config, col_instances, std::move(batch));

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result).empty());
    std::cout << "test_stmt_format_insert_data_empty_batch passed!" << std::endl;
}

void test_stmt_format_insert_data_invalid_version() {
    DataFormat format;
    format.format_type = "stmt";
    format.stmt_config.version = "v1";  // Unsupported version

    InsertDataConfig config;
    config.target.tdengine.database_info.name = "test_db";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f, 42}});
    rows.push_back({1500000000001, {2.71f, 43}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.start_time = 1500000000000;
    batch.end_time = 1500000000001;
    batch.total_rows = 2;

    StmtInsertDataFormatter formatter(format);
    try {
        formatter.format(config, col_instances, std::move(batch));
        assert(false && "Should throw exception for invalid version");
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()) == "Unsupported stmt version: v1");
        std::cout << "test_stmt_format_insert_data_invalid_version passed!" << std::endl;
    }
}

void test_stmt_format_insert_data_with_empty_rows() {
    DataFormat format;
    format.format_type = "stmt";
    format.stmt_config.version = "v2";

    InsertDataConfig config;
    config.target.tdengine.database_info.name = "test_db";

    ColumnConfigInstanceVector col_instances;
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

    batch.start_time = 1500000000000;
    batch.end_time = 1500000000002;
    batch.total_rows = 3;

    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(format);
    FormatResult result = formatter->format(config, col_instances, std::move(batch));

    assert(std::holds_alternative<StmtV2InsertData>(result));
    const auto& stmt_data = std::get<StmtV2InsertData>(result);
    
    // Verify the timing information excludes empty table
    assert(stmt_data.start_time == 1500000000000);
    assert(stmt_data.end_time == 1500000000002);
    
    // Verify total rows only counts non-empty tables
    assert(stmt_data.total_rows == 3);  // 2 rows from table1 + 1 row from table3
    
    std::cout << "test_stmt_format_insert_data_with_empty_rows passed!" << std::endl;
}

int main() {
    test_stmt_format_insert_data_single_table();
    test_stmt_format_insert_data_multiple_tables();
    test_stmt_format_insert_data_empty_batch();
    test_stmt_format_insert_data_invalid_version();
    test_stmt_format_insert_data_with_empty_rows();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}