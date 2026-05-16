#include "FormatResult.hpp"
#include "SqlInsertData.hpp"
#include "StmtV2InsertData.hpp"
#include "MemoryPool.hpp"
#include <cassert>
#include <cstring>
#include <iostream>

void test_sql_data() {
    // Test construction and move semantics
    std::string test_sql = "SELECT * FROM test_table;";
    const size_t sql_length = test_sql.length();
    SqlData sql_data(std::move(test_sql));

    // Test size
    (void)sql_length;
    assert(sql_data.size() == sql_length);

    // Test string content
    assert(sql_data.str() == "SELECT * FROM test_table;");

    // Test C-string
    assert(strcmp(sql_data.c_str(), "SELECT * FROM test_table;") == 0);

    // Test move constructor
    SqlData moved_data(std::move(sql_data));
    assert(moved_data.size() == sql_length);
    assert(moved_data.str() == "SELECT * FROM test_table;");

    std::cout << "test_sql_data passed!" << std::endl;
}

void test_base_insert_data() {
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

    BaseInsertData data(block, col_instances, tag_instances);
    (void)data;
    assert(data.type == typeid(BaseInsertData));
    assert(data.start_time == 1500000000000);
    assert(data.end_time == 1500000000001);
    assert(data.total_rows == 2);

    std::cout << "test_base_insert_data passed!" << std::endl;
}

void test_sql_insert_data() {
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

    SqlInsertData data("INSERT INTO test_table VALUES(1,2,3);");

    // Test base class members
    auto base_data = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(data));
    assert(base_data != nullptr);
    assert(base_data->start_time == 1500000000000);
    assert(base_data->end_time == 1500000000001);
    assert(base_data->total_rows == 2);

    // Test SqlInsertData member
    const auto* payload = base_data->payload_as<SqlInsertData>();
    (void)payload;
    assert(payload != nullptr);
    assert(payload->str() == "INSERT INTO test_table VALUES(1,2,3);");

    std::cout << "test_sql_insert_data passed!" << std::endl;
}

void test_stmt_v2_insert_data() {
    // Create test data
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f}});
    batch.table_batches.emplace_back("test_table", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    // Create StmtV2InsertData
    StmtV2InsertData data(block);

    // Test base class members
    auto base_data = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(data));
    assert(base_data != nullptr);
    assert(base_data->start_time == 1500000000000);
    assert(base_data->end_time == 1500000000000);
    assert(base_data->total_rows == 1);
    assert(base_data->row_count() == 1);
    assert(base_data->column_count() == 1);

    // Test StmtV2Data members
    const auto* payload = base_data->payload_as<StmtV2InsertData>();
    (void)payload;
    assert(payload != nullptr);
    assert(payload->block_ == block);

    std::cout << "test_stmt_v2_insert_data passed!" << std::endl;
}

void test_format_result_variant() {
    // Test string variant
    FormatResult result1 = std::string("Test string");
    assert(std::holds_alternative<std::string>(result1));
    assert(std::get<std::string>(result1) == "Test string");

    // Test SqlInsertData variant
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

    SqlInsertData data("INSERT DATA");
    auto base_data = BaseInsertData::make_with_payload(block, col_instances, tag_instances, std::move(data));
    assert(base_data != nullptr);
    FormatResult result2 = std::move(base_data);

    assert(std::holds_alternative<InsertFormatResult>(result2));
    const auto& ptr = std::get<InsertFormatResult>(result2);

    if (auto* base_ptr = ptr.get()) {
        const auto* payload = base_ptr->payload_as<SqlInsertData>();
        assert(payload != nullptr);
        (void)payload;
        assert(ptr->start_time == 1500000000000);
        assert(ptr->end_time == 1500000000001);
        assert(ptr->total_rows == 2);
        assert(payload->str() == "INSERT DATA");
    } else {
        throw std::runtime_error("Unexpected null BaseInsertData pointer");
    }

    // Test variant type checking
    assert(!std::holds_alternative<InsertFormatResult>(result1));
    assert(!std::holds_alternative<std::string>(result2));

    std::cout << "test_format_result_variant passed!" << std::endl;
}

int main() {
    test_sql_data();
    test_base_insert_data();
    test_sql_insert_data();
    test_stmt_v2_insert_data();
    test_format_result_variant();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}