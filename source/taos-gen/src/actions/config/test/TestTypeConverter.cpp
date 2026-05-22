#include "TypeConverter.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

using namespace TypeConverter;

void test_convert_bool() {
    assert(convert_value<bool>("true") == true);
    assert(convert_value<bool>("1") == true);
    assert(convert_value<bool>("t") == true);
    assert(convert_value<bool>("false") == false);
    assert(convert_value<bool>("0") == false);
    assert(convert_value<bool>("f") == false);
    std::cout << "test_convert_bool passed" << std::endl;
}

void test_convert_bool_case_insensitive() {
    // Uppercase
    assert(convert_value<bool>("TRUE") == true);
    assert(convert_value<bool>("FALSE") == false);
    assert(convert_value<bool>("T") == true);
    assert(convert_value<bool>("F") == false);

    // Mixed case
    assert(convert_value<bool>("True") == true);
    assert(convert_value<bool>("False") == false);
    assert(convert_value<bool>("tRuE") == true);
    assert(convert_value<bool>("fAlSe") == false);

    std::cout << "test_convert_bool_case_insensitive passed" << std::endl;
}

void test_convert_int() {
    assert(convert_value<int32_t>("123") == 123);
    assert(convert_value<int8_t>("-42") == -42);
    assert(convert_value<uint16_t>("65535") == 65535);

    // Leading '+' sign for unsigned types (from_chars compatibility fix)
    assert(convert_value<uint8_t>("+1") == 1);
    assert(convert_value<uint16_t>("+100") == 100);
    assert(convert_value<uint32_t>("+4294967295") == 4294967295u);
    assert(convert_value<uint64_t>("+18446744073709551615") == UINT64_MAX);

    // Leading '+' sign for signed types (already supported by from_chars)
    assert(convert_value<int32_t>("+42") == 42);
    assert(convert_value<int64_t>("+9999") == 9999);

    std::cout << "test_convert_int passed" << std::endl;
}

void test_convert_float_double() {
    assert(convert_value<float>("3.14") == 3.14f);
    assert(convert_value<double>("2.71828") == 2.71828);
    std::cout << "test_convert_float_double passed" << std::endl;
}

void test_convert_string() {
    assert(convert_value<std::string>("  hello  ") == "hello");
    std::cout << "test_convert_string passed" << std::endl;
}

void test_convert_trim_behavior() {
    assert(convert_value<bool>(" true ") == true);
    assert(convert_value<int32_t>(" 123 ") == 123);
    assert(convert_value<std::string>(" a ") == "a");
    std::cout << "test_convert_trim_behavior passed" << std::endl;
}

void test_convert_u16string() {
    std::u16string expected = u"你好";
    std::string utf8 = "你好";
    assert(convert_value<std::u16string>(utf8) == expected);
    std::cout << "test_convert_u16string passed" << std::endl;
}

void test_convert_vector_uint8() {
    std::vector<uint8_t> expected = {'a', 'b', 'c'};
    assert(convert_value<std::vector<uint8_t>>("abc") == expected);
    std::cout << "test_convert_vector_uint8 passed" << std::endl;
}

void test_convert_to_type() {
    assert(std::get<bool>(convert_to_type("true", ColumnTypeTag::BOOL)) == true);
    assert(std::get<int8_t>(convert_to_type("-128", ColumnTypeTag::TINYINT)) == -128);
    assert(std::get<uint8_t>(convert_to_type("255", ColumnTypeTag::TINYINT_UNSIGNED)) == 255);
    assert(std::get<int16_t>(convert_to_type("-32768", ColumnTypeTag::SMALLINT)) == -32768);
    assert(std::get<uint16_t>(convert_to_type("65535", ColumnTypeTag::SMALLINT_UNSIGNED)) == 65535);
    assert(std::get<int32_t>(convert_to_type("2147483647", ColumnTypeTag::INT)) == 2147483647);
    assert(std::get<uint32_t>(convert_to_type("4294967295", ColumnTypeTag::INT_UNSIGNED)) == 4294967295u);
    assert(std::get<int64_t>(convert_to_type("-9223372036854775808", ColumnTypeTag::BIGINT)) == INT64_MIN);
    assert(std::get<uint64_t>(convert_to_type("18446744073709551615", ColumnTypeTag::BIGINT_UNSIGNED)) == UINT64_MAX);
    assert(std::get<float>(convert_to_type("1.23", ColumnTypeTag::FLOAT)) == 1.23f);
    assert(std::get<double>(convert_to_type("2.71828", ColumnTypeTag::DOUBLE)) == 2.71828);
    std::u16string expected_u16 = u"你好";
    assert(std::get<std::u16string>(convert_to_type("你好", ColumnTypeTag::NCHAR)) == expected_u16);
    assert(std::get<std::string>(convert_to_type("abc", ColumnTypeTag::VARCHAR)) == "abc");
    assert(std::get<std::string>(convert_to_type("bin", ColumnTypeTag::BINARY)) == "bin");
    assert(std::get<std::string>(convert_to_type("{\"a\":1}", ColumnTypeTag::JSON)) == "{\"a\":1}");
    std::vector<uint8_t> expected_vec = {'a', 'b', 'c'};
    assert(std::get<std::vector<uint8_t>>(convert_to_type("abc", ColumnTypeTag::VARBINARY)) == expected_vec);
    std::cout << "test_convert_to_type passed" << std::endl;
}

int main() {
    test_convert_bool();
    test_convert_bool_case_insensitive();
    test_convert_int();
    test_convert_float_double();
    test_convert_string();
    test_convert_u16string();
    test_convert_vector_uint8();
    test_convert_to_type();
    test_convert_trim_behavior();

    std::cout << "All TypeConverter tests passed!" << std::endl;
    return 0;
}