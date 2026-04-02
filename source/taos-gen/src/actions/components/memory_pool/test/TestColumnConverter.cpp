#include "ColumnConverter.hpp"
#include <cassert>
#include <string>
#include <vector>
#include <iostream>
#include <cstring>

void test_fixed_type_handler_int32() {
    ColumnType value = int32_t(12345);
    int32_t dest = 0;
    ColumnConverter::fixed_type_handler<int32_t>(value, &dest, sizeof(int32_t));
    assert(dest == 12345);
    std::cout << "test_fixed_type_handler_int32 passed." << std::endl;
}

void test_fixed_type_handler_float() {
    ColumnType value = 3.14f;
    float dest = 0.0f;
    ColumnConverter::fixed_type_handler<float>(value, &dest, sizeof(float));
    assert(dest == 3.14f);
    std::cout << "test_fixed_type_handler_float passed." << std::endl;
}

void test_fixed_type_handler_double() {
    ColumnType value = 3.14159265758;
    double dest = 0.0;
    ColumnConverter::fixed_type_handler<double>(value, &dest, sizeof(double));
    assert(dest == 3.14159265758);
    std::cout << "test_fixed_type_handler_double passed." << std::endl;
}

void test_fixed_type_handler_decimal() {
    Decimal dec;
    dec.value = "123.456";
    ColumnType value = dec;
    Decimal dest;
    ColumnConverter::fixed_type_handler<Decimal>(value, &dest, sizeof(Decimal));
    assert(dest.value == "123.456");
    std::cout << "test_fixed_type_handler_decimal passed." << std::endl;
}

void test_string_type_handler() {
    ColumnType value = std::string("hello");
    char buf[16] = {};
    size_t len = ColumnConverter::string_type_handler(value, buf, sizeof(buf));
    (void)len;
    assert(len == 5);
    assert(std::string(buf, len) == "hello");
    std::cout << "test_string_type_handler passed." << std::endl;
}

void test_u16string_type_handler() {
    ColumnType value = std::u16string(u"你好世界");
    char buf[32] = {};
    size_t len = ColumnConverter::u16string_type_handler(value, buf, sizeof(buf));
    (void)len;
    assert(len == std::string("你好世界").size()); // 12
    std::string restored(buf, len);
    assert(restored == "你好世界");
    std::cout << "test_u16string_type_handler passed." << std::endl;
}

void test_json_type_handler() {
    JsonValue json;
    json.raw_json = R"({"a":1,"b":2})";
    ColumnType value = json;
    char buf[64] = {};
    size_t len = ColumnConverter::json_type_handler(value, buf, sizeof(buf));
    (void)len;
    assert(len == json.raw_json.size());
    assert(std::string(buf, len) == json.raw_json);
    std::cout << "test_json_type_handler passed." << std::endl;
}

void test_binary_type_handler() {
    ColumnType value = std::vector<uint8_t>{1, 2, 3, 4};
    char buf[8] = {};
    size_t len = ColumnConverter::binary_type_handler(value, buf, sizeof(buf));
    (void)len;
    assert(len == 4);
    assert(static_cast<uint8_t>(buf[0]) == 1);
    assert(static_cast<uint8_t>(buf[1]) == 2);
    assert(static_cast<uint8_t>(buf[2]) == 3);
    assert(static_cast<uint8_t>(buf[3]) == 4);
    std::cout << "test_binary_type_handler passed." << std::endl;
}

void test_fixed_to_column() {
    int64_t src = 123456789;
    ColumnType value = ColumnConverter::fixed_to_column<int64_t>(&src);
    assert(std::get<int64_t>(value) == 123456789);
    std::cout << "test_fixed_to_column passed." << std::endl;
}

void test_string_to_column() {
    const char* src = "abcde";
    ColumnType value = ColumnConverter::string_to_column(src, 5);
    assert(std::get<std::string>(value) == "abcde");
    std::cout << "test_string_to_column passed." << std::endl;
}

void test_u16string_to_column() {
    std::string utf8 = "你好世界";
    ColumnType value = ColumnConverter::u16string_to_column(utf8.data(), utf8.size());
    std::u16string expected(u"你好世界");
    (void)value;
    (void)expected;
    assert(std::get<std::u16string>(value) == expected);
    std::cout << "test_u16string_to_column passed." << std::endl;
}

void test_json_to_column() {
    std::string raw = R"({"x":42})";
    ColumnType value = ColumnConverter::json_to_column(raw.data(), raw.size());
    assert(std::get<JsonValue>(value).raw_json == raw);
    std::cout << "test_json_to_column passed." << std::endl;
}

void test_binary_to_column() {
    uint8_t arr[3] = {9, 8, 7};
    ColumnType value = ColumnConverter::binary_to_column(reinterpret_cast<const char*>(arr), 3);
    auto vec = std::get<std::vector<uint8_t>>(value);
    assert(vec.size() == 3 && vec[0] == 9 && vec[1] == 8 && vec[2] == 7);
    std::cout << "test_binary_to_column passed." << std::endl;
}

void test_column_to_string() {
    ColumnType value = int32_t(42);
    std::string str = ColumnConverter::column_to_string(value);
    assert(str.find("42") != std::string::npos);
    std::cout << "test_column_to_string passed." << std::endl;
}

void test_create_handler_for_column() {
    ColumnConfig config{"col1", "INT"};
    ColumnConfigInstance instance(config);
    auto handler = ColumnConverter::create_handler_for_column(instance);
    (void)handler;
    assert(handler.to_fixed != nullptr);
    assert(handler.to_var == nullptr);
    assert(handler.to_column_fixed != nullptr);
    assert(handler.to_string != nullptr);
    std::cout << "test_create_handler_for_column passed." << std::endl;
}

void test_create_handlers_for_columns() {
    ColumnConfigInstanceVector vec;
    vec.emplace_back(ColumnConfig{"col1", "INT"});
    vec.emplace_back(ColumnConfig{"col2", "VARCHAR(20)"});
    auto handlers = ColumnConverter::create_handlers_for_columns(vec);
    assert(handlers.size() == 2);
    assert(handlers[0].to_fixed != nullptr);
    assert(handlers[1].to_var != nullptr);
    std::cout << "test_create_handlers_for_columns passed." << std::endl;
}

void test_handler_exceptions() {
    bool caught = false;
    try {
        ColumnConfig config{"colX", "UNSUPPORTED"};
        ColumnConfigInstance instance(config);
        ColumnConverter::create_handler_for_column(instance);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Unsupported type") != std::string::npos);
        caught = true;
    }
    (void)caught;
    assert(caught);
    std::cout << "test_handler_exceptions passed." << std::endl;
}

int main() {
    test_fixed_type_handler_int32();
    test_fixed_type_handler_float();
    test_fixed_type_handler_double();
    test_fixed_type_handler_decimal();
    test_string_type_handler();
    test_u16string_type_handler();
    test_json_type_handler();
    test_binary_type_handler();
    test_fixed_to_column();
    test_string_to_column();
    test_u16string_to_column();
    test_json_to_column();
    test_binary_to_column();
    test_column_to_string();
    test_create_handler_for_column();
    test_create_handlers_for_columns();
    test_handler_exceptions();

    std::cout << "All ColumnConverter tests passed." << std::endl;
    return 0;
}