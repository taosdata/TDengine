#include <iostream>
#include <cassert>
#include <cmath>
#include "ExprColumnGenerator.hpp"

void test_generate_expr_double_column() {
    ColumnConfig config;
    config.type = "double";
    config.formula = std::string("square(1, 10, 4, 0) * 2 + math.sin(_i/10)");
    ColumnConfigInstance instance(config);

    ExprColumnGenerator generator(instance);

    ColumnType value = generator.generate();
    assert(std::holds_alternative<double>(value));
    double double_value = std::get<double>(value);
    double expected = 20.0 + std::sin(0.0); // _i==0
    (void)double_value;
    (void)expected;
    assert(std::abs(double_value - expected) < 1e-6);

    std::cout << "test_generate_expr_double_column passed.\n";
}

void test_generate_expr_string_column() {
    ColumnConfig config;
    config.type = "varchar(15)";
    config.formula = std::string("rand_ipv4()");
    ColumnConfigInstance instance(config);

    ExprColumnGenerator generator(instance);

    ColumnType value = generator.generate();
    assert(std::holds_alternative<std::string>(value));
    std::string str_value = std::get<std::string>(value);
    // Check IPv4 format: should contain three dots
    size_t dot1 = str_value.find('.');
    size_t dot2 = str_value.find('.', dot1 + 1);
    size_t dot3 = str_value.find('.', dot2 + 1);
    (void)dot3;
    assert(dot1 != std::string::npos);
    assert(dot2 != std::string::npos);
    assert(dot3 != std::string::npos);

    std::cout << "test_generate_expr_string_column passed.\n";
}

void test_generate_expr_int_column() {
    ColumnConfig config;
    config.type = "int";
    config.formula = std::string("3.7 + 2");
    ColumnConfigInstance instance(config);

    ExprColumnGenerator generator(instance);

    ColumnType value = generator.generate();
    assert(std::holds_alternative<int32_t>(value));
    int32_t int_value = std::get<int32_t>(value);
    (void)int_value;
    assert(int_value == 5); // static_cast<int32_t>(5.7) == 5

    std::cout << "test_generate_expr_int_column passed.\n";
}

void test_generate_expr_bool_column() {
    ColumnConfig config;
    config.type = "bool";
    config.formula = std::string("0.0");
    ColumnConfigInstance instance(config);

    ExprColumnGenerator generator(instance);

    ColumnType value = generator.generate();
    assert(std::holds_alternative<bool>(value));
    assert(std::get<bool>(value) == false);

    // Test non-zero
    config.formula = std::string("123.45");
    ColumnConfigInstance instance2(config);
    ExprColumnGenerator generator2(instance2);
    value = generator2.generate();
    assert(std::holds_alternative<bool>(value));
    assert(std::get<bool>(value) == true);

    std::cout << "test_generate_expr_bool_column passed.\n";
}

void test_generate_expr_nchar_column() {
    ColumnConfig config;
    config.type = "nchar(5)";
    config.formula = std::string("'abcde'");
    ColumnConfigInstance instance(config);

    ExprColumnGenerator generator(instance);

    ColumnType value = generator.generate();
    assert(std::holds_alternative<std::u16string>(value));
    std::u16string u16_value = std::get<std::u16string>(value);
    assert(u16_value.size() == 5);
    assert(u16_value[0] == u'a');

    std::cout << "test_generate_expr_nchar_column passed.\n";
}

void test_generate_expr_multiple_values() {
    ColumnConfig config;
    config.type = "double";
    config.formula = std::string("square(1, 10, 4, 0)");
    ColumnConfigInstance instance(config);

    ExprColumnGenerator generator(instance);
    auto values = generator.generate(5);

    assert(values.size() == 5);
    double expected[] = {10, 10, 1, 1, 10};
    for (size_t i = 0; i < values.size(); ++i) {
        assert(std::holds_alternative<double>(values[i]));
        double double_value = std::get<double>(values[i]);
        (void)double_value;
        (void)expected;
        assert(std::abs(double_value - expected[i]) < 1e-6);
    }

    std::cout << "test_generate_expr_multiple_values passed.\n";
}

void test_generate_expr_unsupported_conversion() {
    ColumnConfig config;
    config.type = "geometry(21)";
    config.formula = std::string("1.23");
    ColumnConfigInstance instance(config);

    ExprColumnGenerator generator(instance);
    try {
        generator.generate();
        assert(false && "Should throw for unsupported type conversion");
    } catch (const std::runtime_error&) {
        std::cout << "test_generate_expr_unsupported_conversion passed.\n";
    }
}

int main() {
    test_generate_expr_double_column();
    test_generate_expr_string_column();
    test_generate_expr_int_column();
    test_generate_expr_bool_column();
    test_generate_expr_nchar_column();
    test_generate_expr_multiple_values();
    test_generate_expr_unsupported_conversion();

    std::cout << "All ExprColumnGenerator tests passed.\n";
    return 0;
}