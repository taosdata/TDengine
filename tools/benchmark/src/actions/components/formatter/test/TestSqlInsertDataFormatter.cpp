#include <iostream>
#include <cassert>
#include "FormatterRegistrar.h"
#include "SqlInsertDataFormatter.h"

void test_format_insert_data_single_table() {
    DataFormat format;
    format.format_type = "sql";

    InsertDataConfig config;
    config.target.tdengine.database_info.name = "test_db";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});
    col_instances.emplace_back(ColumnConfig{"s1", "VARCHAR"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14, 42, std::string("value1")}});
    rows.push_back({1500000000001, {2.71, 43, std::string("value2")}});
    batch.table_batches.push_back({"table1", rows});

    SqlInsertDataFormatter formatter(format);
    FormatResult result = formatter.format(config, col_instances, batch);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == 
           "INSERT INTO `test_db`.`table1` VALUES "
           "(1500000000000,3.14,42,'value1')"
           "(1500000000001,2.71,43,'value2');");
    std::cout << "test_format_insert_data_single_table passed!" << std::endl;
}

void test_format_insert_data_multiple_tables() {
    DataFormat format;
    format.format_type = "sql";

    InsertDataConfig config;
    config.target.tdengine.database_info.name = "test_db";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"i1", "INT"});

    MultiBatch batch;
    // First table
    std::vector<RowData> rows1;
    rows1.push_back({1500000000000, {3.14, 42}});
    rows1.push_back({1500000000001, {2.71, 43}});
    batch.table_batches.push_back({"table1", rows1});

    // Second table
    std::vector<RowData> rows2;
    rows2.push_back({1500000000002, {1.23, 44}});
    rows2.push_back({1500000000003, {4.56, 45}});
    batch.table_batches.push_back({"table2", rows2});

    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(format);
    FormatResult result = formatter->format(config, col_instances, batch);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == 
           "INSERT INTO `test_db`.`table1` VALUES "
           "(1500000000000,3.14,42)"
           "(1500000000001,2.71,43) "
           "`test_db`.`table2` VALUES "
           "(1500000000002,1.23,44)"
           "(1500000000003,4.56,45);");
    std::cout << "test_format_insert_data_multiple_tables passed!" << std::endl;
}

void test_format_insert_data_empty_rows() {
    DataFormat format;
    format.format_type = "sql";

    InsertDataConfig config;
    config.target.tdengine.database_info.name = "test_db";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});

    MultiBatch batch;
    batch.table_batches.push_back({"table1", {}});

    SqlInsertDataFormatter formatter(format);
    FormatResult result = formatter.format(config, col_instances, batch);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == "");
    std::cout << "test_format_insert_data_empty_rows passed!" << std::endl;
}

void test_format_insert_data_different_types() {
    DataFormat format;
    format.format_type = "sql";

    InsertDataConfig config;
    config.target.tdengine.database_info.name = "test_db";

    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"f1", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"b1", "BOOL"});
    col_instances.emplace_back(ColumnConfig{"s1", "NCHAR"});
    col_instances.emplace_back(ColumnConfig{"j1", "JSON"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {3.14, true, std::u16string(u"测试"), std::string("{\"key\":\"value\"}")}});
    batch.table_batches.push_back({"table1", rows});

    SqlInsertDataFormatter formatter(format);
    FormatResult result = formatter.format(config, col_instances, batch);

    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == 
           "INSERT INTO `test_db`.`table1` VALUES "
           "(1500000000000,3.14,true,'测试','{\"key\":\"value\"}');");
    std::cout << "test_format_insert_data_different_types passed!" << std::endl;
}

int main() {
    test_format_insert_data_single_table();
    test_format_insert_data_multiple_tables();
    test_format_insert_data_empty_rows();
    test_format_insert_data_different_types();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}