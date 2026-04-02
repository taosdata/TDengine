#include "ColumnType.hpp"
#include "StringUtils.hpp"
#include <iostream>
#include <cassert>
#include <sstream>

void test_column_type_ostream() {
    std::stringstream ss;

    // Test bool
    ss.str(""); ss << ColumnType{true};
    assert(ss.str() == "true");

    // Test integer types
    ss.str(""); ss << ColumnType{static_cast<int32_t>(123)};
    assert(ss.str() == "123");
    ss.str(""); ss << ColumnType{static_cast<int64_t>(-456)};
    assert(ss.str() == "-456");

    // Test floating point types
    ss.str(""); ss << ColumnType{3.14f};
    assert(ss.str() == "3.14");

    // Test string type
    ss.str(""); ss << ColumnType{std::string("hello world")};
    assert(ss.str() == "hello world");

    // Test Decimal
    ss.str(""); ss << ColumnType{Decimal{"123.456"}};
    assert(ss.str() == "Decimal(123.456)");

    // Test JsonValue
    ss.str(""); ss << ColumnType{JsonValue{R"({"a":1})"}} ;
    assert(ss.str() == R"({"a":1})");

    // Test Geometry
    ss.str(""); ss << ColumnType{Geometry{"POINT(10 20)"}};
    assert(ss.str() == "Geometry(POINT(10 20))");

    // Test NCHAR (u16string)
    ss.str(""); ss << ColumnType{std::u16string{u"nchar_测试"}};
    assert(ss.str() == "nchar_测试");

    // Test VarBinary
    ss.str(""); ss << ColumnType{std::vector<uint8_t>{0xDE, 0xAD, 0xBE, 0xEF}};
    assert(ss.str() == "VarBinary(222,173,190,239)");

    std::cout << "test_column_type_ostream passed!" << std::endl;
}

void test_row_type_ostream() {
    RowType row;
    row.push_back(true);
    row.push_back(static_cast<int32_t>(42));
    row.push_back(std::string("test"));

    std::stringstream ss;
    ss << row;
    assert(ss.str() == "[true, 42, test]");

    std::cout << "test_row_type_ostream passed!" << std::endl;
}

void test_variant_index() {
    static_assert(variant_index<bool, ColumnType>::value == 0);
    static_assert(variant_index<int8_t, ColumnType>::value == 1);
    static_assert(variant_index<int32_t, ColumnType>::value == 5);
    static_assert(variant_index<double, ColumnType>::value == 10);
    static_assert(variant_index<std::string, ColumnType>::value == 13);
    static_assert(variant_index<Geometry, ColumnType>::value == 16);

    std::cout << "test_variant_index passed!" << std::endl;
}

void test_column_type_traits() {
    // Test is_numeric
    assert(ColumnTypeTraits::is_numeric(ColumnTypeTag::INT));
    assert(ColumnTypeTraits::is_numeric(ColumnTypeTag::DOUBLE));
    assert(!ColumnTypeTraits::is_numeric(ColumnTypeTag::VARCHAR));
    assert(!ColumnTypeTraits::is_numeric(ColumnTypeTag::BOOL));

    // Test is_integer
    assert(ColumnTypeTraits::is_integer(ColumnTypeTag::BIGINT));
    assert(ColumnTypeTraits::is_integer(ColumnTypeTag::SMALLINT_UNSIGNED));
    assert(!ColumnTypeTraits::is_integer(ColumnTypeTag::FLOAT));
    assert(!ColumnTypeTraits::is_integer(ColumnTypeTag::NCHAR));

    // Test needs_quotes
    assert(ColumnTypeTraits::needs_quotes(ColumnTypeTag::VARCHAR));
    assert(ColumnTypeTraits::needs_quotes(ColumnTypeTag::JSON));
    assert(!ColumnTypeTraits::needs_quotes(ColumnTypeTag::INT));
    assert(!ColumnTypeTraits::needs_quotes(ColumnTypeTag::BOOL));

    std::cout << "test_column_type_traits passed!" << std::endl;
}

int main() {
    test_column_type_ostream();
    test_row_type_ostream();
    test_variant_index();
    test_column_type_traits();

    std::cout << "All tests passed!" << std::endl;
    return 0;
}