#include <iostream>
#include <cassert>
#include "RandomColumnGenerator.h"

void test_generate_int_column() {
    ColumnConfig config;
    config.type = "int";
    config.min = 10;
    config.max = 20;
    ColumnConfigInstance instance(config);

    RandomColumnGenerator generator(instance);

    for (int i = 0; i < 100; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<int>(value));
        int int_value = std::get<int>(value);
        assert(int_value >= 10 && int_value < 20);
    }

    std::cout << "test_generate_int_column passed.\n";
}

void test_generate_double_column() {
    ColumnConfig config;
    config.type = "double";
    config.min = 1.5;
    config.max = 3.5;
    ColumnConfigInstance instance(config);

    RandomColumnGenerator generator(instance);

    for (int i = 0; i < 100; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<double>(value));
        double double_value = std::get<double>(value);
        assert(double_value >= 1.5 && double_value < 3.5);
    }

    std::cout << "test_generate_double_column passed.\n";
}

void test_generate_bool_column() {
    ColumnConfig config;
    config.type = "bool";
    ColumnConfigInstance instance(config);

    RandomColumnGenerator generator(instance);

    for (int i = 0; i < 100; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<bool>(value));
    }

    std::cout << "test_generate_bool_column passed.\n";
}

void test_generate_string_column_with_corpus() {
    ColumnConfig config;
    config.type = "varchar";
    config.corpus = std::string("abc");
    ColumnConfigInstance instance(config);

    RandomColumnGenerator generator(instance);

    for (int i = 0; i < 100; ++i) {
        ColumnType value = generator.generate();
        assert(std::holds_alternative<std::string>(value));
        std::string str_value = std::get<std::string>(value);
        assert(str_value.size() == 1);
        assert(config.corpus->find(str_value[0]) != std::string::npos);
    }

    std::cout << "test_generate_string_column passed.\n";
}

void test_generate_multiple_values() {
    ColumnConfig config;
    config.type = "int";
    config.min = 1;
    config.max = 10;
    ColumnConfigInstance instance(config);

    RandomColumnGenerator generator(instance);
    auto values = generator.generate(10);

    assert(values.size() == 10);
    for (const auto& value : values) {
        assert(std::holds_alternative<int>(value));
        int int_value = std::get<int>(value);
        assert(int_value >= 1 && int_value < 10);
    }

    std::cout << "test_generate_multiple_values passed.\n";
}

int main() {
    test_generate_int_column();
    test_generate_double_column();
    test_generate_bool_column();
    test_generate_string_column_with_corpus();
    test_generate_multiple_values();

    std::cout << "All tests passed.\n";
    return 0;
}