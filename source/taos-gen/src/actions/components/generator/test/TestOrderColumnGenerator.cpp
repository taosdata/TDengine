#include "OrderColumnGenerator.hpp"
#include <iostream>
#include <cassert>

void test_generate_bool_order_column() {
    ColumnConfig config;
    config.type = "bool";
    config.order_min = 0;
    config.order_max = 2;
    config.parse_type();
    ColumnConfigInstance instance(config);

    OrderColumnGenerator generator(instance);

    for (int i = 0; i < 10; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<bool>(value));
        bool b = std::get<bool>(value);
        (void)b;
        assert(b == false || b == true);
    }
    std::cout << "test_generate_bool_order_column passed." << std::endl;
}

void test_generate_tinyint_order_column() {
    ColumnConfig config;
    config.type = "tinyint";
    config.order_min = -5;
    config.order_max = 5;
    config.parse_type();
    ColumnConfigInstance instance(config);

    OrderColumnGenerator generator(instance);

    for (int i = 0; i < 10; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<int8_t>(value));
        int8_t v = std::get<int8_t>(value);
        (void)v;
        assert(v >= -5 && v < 5);
    }
    std::cout << "test_generate_tinyint_order_column passed." << std::endl;
}

void test_generate_tinyint_unsigned_order_column() {
    ColumnConfig config;
    config.type = "tinyint unsigned";
    config.order_min = 0;
    config.order_max = 10;
    config.parse_type();
    ColumnConfigInstance instance(config);

    OrderColumnGenerator generator(instance);

    for (int i = 0; i < 10; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<uint8_t>(value));
        uint8_t v = std::get<uint8_t>(value);
        (void)v;
        assert(v >= 0 && v < 10);
    }
    std::cout << "test_generate_tinyint_unsigned_order_column passed." << std::endl;
}

void test_generate_smallint_order_column() {
    ColumnConfig config;
    config.type = "smallint";
    config.order_min = -100;
    config.order_max = -95;
    config.parse_type();
    ColumnConfigInstance instance(config);

    OrderColumnGenerator generator(instance);

    for (int i = 0; i < 10; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<int16_t>(value));
        int16_t v = std::get<int16_t>(value);
        (void)v;
        assert(v >= -100 && v < -95);
    }
    std::cout << "test_generate_smallint_order_column passed." << std::endl;
}

void test_generate_smallint_unsigned_order_column() {
    ColumnConfig config;
    config.type = "smallint unsigned";
    config.order_min = 100;
    config.order_max = 105;
    config.parse_type();
    ColumnConfigInstance instance(config);

    OrderColumnGenerator generator(instance);

    for (int i = 0; i < 10; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<uint16_t>(value));
        uint16_t v = std::get<uint16_t>(value);
        (void)v;
        assert(v >= 100 && v < 105);
    }
    std::cout << "test_generate_smallint_unsigned_order_column passed." << std::endl;
}

void test_generate_int_order_column() {
    ColumnConfig config;
    config.type = "int";
    config.order_min = 10;
    config.order_max = 15;
    config.parse_type();
    ColumnConfigInstance instance(config);

    OrderColumnGenerator generator(instance);

    for (int i = 0; i < 10; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<int32_t>(value));
        int32_t v = std::get<int32_t>(value);
        (void)v;
        assert(v >= 10 && v < 15);
    }
    std::cout << "test_generate_int_order_column passed." << std::endl;
}

void test_generate_int_unsigned_order_column() {
    ColumnConfig config;
    config.type = "int unsigned";
    config.order_min = 100;
    config.order_max = 105;
    config.parse_type();
    ColumnConfigInstance instance(config);

    OrderColumnGenerator generator(instance);

    for (int i = 0; i < 10; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<uint32_t>(value));
        uint32_t v = std::get<uint32_t>(value);
        (void)v;
        assert(v >= 100 && v < 105);
    }
    std::cout << "test_generate_int_unsigned_order_column passed." << std::endl;
}

void test_generate_bigint_order_column() {
    ColumnConfig config;
    config.type = "bigint";
    config.order_min = 100000;
    config.order_max = 100005;
    config.parse_type();
    ColumnConfigInstance instance(config);

    OrderColumnGenerator generator(instance);

    for (int i = 0; i < 10; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<int64_t>(value));
        int64_t v = std::get<int64_t>(value);
        (void)v;
        assert(v >= 100000 && v < 100005);
    }
    std::cout << "test_generate_bigint_order_column passed." << std::endl;
}

void test_generate_bigint_unsigned_order_column() {
    ColumnConfig config;
    config.type = "bigint unsigned";
    config.order_min = 1000;
    config.order_max = 1005;
    config.parse_type();
    ColumnConfigInstance instance(config);

    OrderColumnGenerator generator(instance);

    for (int i = 0; i < 10; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<uint64_t>(value));
        uint64_t v = std::get<uint64_t>(value);
        (void)v;
        assert(v >= 1000 && v < 1005);
    }
    std::cout << "test_generate_bigint_unsigned_order_column passed." << std::endl;
}

void test_generate_multiple_values() {
    ColumnConfig config;
    config.type = "int";
    config.order_min = 1;
    config.order_max = 4;
    config.parse_type();
    ColumnConfigInstance instance(config);

    OrderColumnGenerator generator(instance);
    auto values = generator.generate(5);

    assert(values.size() == 5);
    for (const auto& value : values) {
        assert(std::holds_alternative<int32_t>(value));
        int32_t v = std::get<int32_t>(value);
        (void)v;
        assert(v >= 1 && v < 4);
    }
    std::cout << "test_generate_multiple_values passed." << std::endl;
}

void test_invalid_order_range_exception() {
    ColumnConfig config;
    config.type = "int";
    config.order_min = 10;
    config.order_max = 5; // max <= min
    config.parse_type();
    ColumnConfigInstance instance(config);

    try {
        OrderColumnGenerator generator(instance);
        assert(false && "Expected exception for invalid order range");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Invalid order range") != std::string::npos);
    }
    std::cout << "test_invalid_order_range_exception passed." << std::endl;
}

void test_unsupported_type_tag_exception() {
    ColumnConfig config;
    config.type = "float";
    config.order_min = 1;
    config.order_max = 10;
    config.parse_type();
    ColumnConfigInstance instance(config);

    try {
        OrderColumnGenerator generator(instance);
        assert(false && "Expected exception for unsupported type_tag");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("unsupported type_tag") != std::string::npos);
    }
    std::cout << "test_unsupported_type_tag_exception passed." << std::endl;
}

int main() {
    test_generate_bool_order_column();
    test_generate_tinyint_order_column();
    test_generate_tinyint_unsigned_order_column();
    test_generate_smallint_order_column();
    test_generate_smallint_unsigned_order_column();
    test_generate_int_order_column();
    test_generate_int_unsigned_order_column();
    test_generate_bigint_order_column();
    test_generate_bigint_unsigned_order_column();
    test_generate_multiple_values();
    test_invalid_order_range_exception();
    test_unsupported_type_tag_exception();

    std::cout << "All OrderColumnGenerator tests passed." << std::endl;
    return 0;
}