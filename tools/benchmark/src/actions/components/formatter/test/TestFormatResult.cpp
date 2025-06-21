#include <cassert>
#include <iostream>
#include "FormatResult.h"

void test_sql_data() {
    // Test construction and move semantics
    std::string test_sql = "SELECT * FROM test_table;";
    const size_t sql_length = test_sql.length();
    SqlData sql_data(std::move(test_sql));
    
    // Test size
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
    BaseInsertData data{
        .start_time = 1500000000000,
        .end_time = 1500000001000,
        .total_rows = 100
    };
    
    assert(data.start_time == 1500000000000);
    assert(data.end_time == 1500000001000);
    assert(data.total_rows == 100);
    
    std::cout << "test_base_insert_data passed!" << std::endl;
}

void test_sql_insert_data() {
    SqlInsertData data{
        BaseInsertData{
            .start_time = 1500000000000,
            .end_time = 1500000001000,
            .total_rows = 100
        },
        .data = SqlData(std::string("INSERT INTO test_table VALUES(1,2,3);"))
    };
    
    // Test base class members
    assert(data.start_time == 1500000000000);
    assert(data.end_time == 1500000001000);
    assert(data.total_rows == 100);
    
    // Test SqlData member
    assert(data.data.str() == "INSERT INTO test_table VALUES(1,2,3);");
    
    std::cout << "test_sql_insert_data passed!" << std::endl;
}

void test_format_result_variant() {
    // Test string variant
    FormatResult result1 = std::string("Test string");
    assert(std::holds_alternative<std::string>(result1));
    assert(std::get<std::string>(result1) == "Test string");
    
    // Test SqlInsertData variant
    FormatResult result2 = SqlInsertData{
        BaseInsertData{1500000000000, 1500000001000, 100},
        SqlData(std::string("INSERT DATA"))
    };
    assert(std::holds_alternative<SqlInsertData>(result2));
    const auto& sql_data = std::get<SqlInsertData>(result2);
    assert(sql_data.start_time == 1500000000000);
    assert(sql_data.data.str() == "INSERT DATA");
    
    // Test variant type checking
    assert(!std::holds_alternative<StmtV2InsertData>(result1));
    assert(!std::holds_alternative<std::string>(result2));
    
    std::cout << "test_format_result_variant passed!" << std::endl;
}

void test_stmt_v2_insert_data() {
    // Create test data
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    
    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14f}});
    batch.table_batches.emplace_back("test_table", std::move(rows));
    
    // Create StmtV2InsertData
    StmtV2InsertData data{
        BaseInsertData{
            .start_time = 1500000000000,
            .end_time = 1500000001000,
            .total_rows = 1
        },
        .data = StmtV2Data(col_instances, std::move(batch))
    };
    
    // Test base class members
    assert(data.start_time == 1500000000000);
    assert(data.end_time == 1500000001000);
    assert(data.total_rows == 1);
    
    // Test StmtV2Data members
    assert(data.data.row_count() == 1);
    assert(data.data.column_count() == 1);
    
    std::cout << "test_stmt_v2_insert_data passed!" << std::endl;
}

int main() {
    test_sql_data();
    test_base_insert_data();
    test_sql_insert_data();
    test_format_result_variant();
    test_stmt_v2_insert_data();
    
    std::cout << "All tests passed!" << std::endl;
    return 0;
}