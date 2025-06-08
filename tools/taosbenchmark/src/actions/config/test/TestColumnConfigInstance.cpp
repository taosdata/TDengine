#include <iostream>
#include <cassert>
#include "ColumnConfigInstance.h"

void test_column_instance_single() {
    std::cout << "Running test_column_instance_single..." << std::endl;

    ColumnConfig config("col", "int", std::nullopt);
    ColumnConfigInstance instance(config);

    assert(instance.name() == "col");
    assert(instance.type() == "int");
    assert(instance.type_tag() == ColumnTypeTag::INT);

    std::cout << "test_column_instance_single passed!" << std::endl;
}

void test_column_instance_multiple() {
    std::cout << "Running test_column_instance_multiple..." << std::endl;

    ColumnConfig config("col", "int", std::nullopt);
    config.count = 3;

    auto instances = ColumnConfigInstanceFactory::create(config);

    assert(instances.size() == 3);
    assert(instances[0].name() == "col1");
    assert(instances[1].name() == "col2");
    assert(instances[2].name() == "col3");

    for (const auto& instance : instances) {
        assert(instance.type() == "int");
        assert(instance.type_tag() == ColumnTypeTag::INT);
    }

    std::cout << "test_column_instance_multiple passed!" << std::endl;
}

void test_column_instance_with_length() {
    std::cout << "Running test_column_instance_with_length..." << std::endl;

    ColumnConfig config("col", "varchar", std::nullopt, 50);
    ColumnConfigInstance instance(config);

    assert(instance.name() == "col");
    assert(instance.type() == "varchar");
    assert(instance.type_tag() == ColumnTypeTag::VARCHAR);

    std::cout << "test_column_instance_with_length passed!" << std::endl;
}

void test_column_instance_with_min_max() {
    std::cout << "Running test_column_instance_with_min_max..." << std::endl;

    ColumnConfig config("col", "float", std::nullopt, 1.0, 10.0);
    ColumnConfigInstance instance(config);

    assert(instance.name() == "col");
    assert(instance.type() == "float");
    assert(instance.type_tag() == ColumnTypeTag::FLOAT);

    std::cout << "test_column_instance_with_min_max passed!" << std::endl;
}

int main() {
    std::cout << "Running ColumnConfigInstance tests..." << std::endl;

    test_column_instance_single();
    test_column_instance_multiple();
    test_column_instance_with_length();
    test_column_instance_with_min_max();

    std::cout << "All ColumnConfigInstance tests passed!" << std::endl;
    return 0;
}