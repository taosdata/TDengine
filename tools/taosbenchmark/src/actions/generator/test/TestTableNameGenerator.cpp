#include <iostream>
#include <cassert>
#include "TableNameGenerator.h"

void test_generate_table_names() {
    TableNameConfig::Generator config;
    config.prefix = "table_";
    config.from = 1;
    config.count = 5;

    TableNameGenerator generator(config);
    std::vector<std::string> names = generator.generate();

    assert(names.size() == 5);
    assert(names[0] == "table_1");
    assert(names[1] == "table_2");
    assert(names[2] == "table_3");
    assert(names[3] == "table_4");
    assert(names[4] == "table_5");

    std::cout << "test_generate_table_names passed.\n";
}

void test_generate_empty_table_names() {
    TableNameConfig::Generator config;
    config.prefix = "table_";
    config.from = 1;
    config.count = 0;

    TableNameGenerator generator(config);
    std::vector<std::string> names = generator.generate();

    assert(names.empty());

    std::cout << "test_generate_empty_table_names passed.\n";
}

int main() {
    test_generate_table_names();
    test_generate_empty_table_names();

    std::cout << "All tests passed.\n";
    return 0;
}