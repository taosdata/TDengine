#include <iostream>
#include <cassert>
#include <fstream>
#include "CreateChildTableAction.hpp"
#include "ActionFactory.hpp"
#include "DatabaseConnector.hpp"
#include "CreateChildTableConfig.hpp"


void test_create_child_table_action_from_generator() {
    GlobalConfig global;
    TDengineConfig tdengine("taos://root:taosdata@localhost:6030/test_action");
    CreateChildTableConfig config;
    config.tdengine = tdengine;
    config.schema.name = "test_super_table";

    // Configure child table name generator
    config.schema.tbname.source_type = "generator";
    config.schema.tbname.generator.prefix = "d";
    config.schema.tbname.generator.count = 10;

    // Configure tag generator
    config.schema.tags = {
        {"tag1", "float", "random", 1.5, 3.5},
        {"tag2", "varchar(20)", "random"}
    };
    config.schema.apply();

    // Create action instance
    std::cout << "Creating action instance for child table from generator..." << std::endl;
    auto action = ActionFactory::instance().create_action(
        global,
        "tdengine/create-child-table",
        config
    );

    // Execute child table creation action
    std::cout << "Executing child table creation..." << std::endl;
    action->execute();

    std::cout << "test_create_child_table_action_from_generator passed\n";
}

void test_create_child_table_action_from_csv() {
    GlobalConfig global;
    TDengineConfig tdengine("taos://root:taosdata@localhost:6030/test_action");

    CreateChildTableConfig config;
    config.tdengine = tdengine;
    config.schema.name = "test_super_table";

    std::ofstream test_file("table_names_and_tags.csv");
    test_file << "tag1,tag2,tag3,table\n";
    test_file << "30.125,New York,not,table1\n";
    test_file << "3.1415926,Los Angeles,me,table2\n";
    test_file.close();

    // Configure child table name CSV file path
    config.schema.tbname.source_type = "csv";
    config.schema.tbname.csv.file_path = "table_names_and_tags.csv";
    config.schema.tbname.csv.has_header = true;
    config.schema.tbname.csv.tbname_index = 3;

    // Configure tag CSV file path
    config.schema.from_csv.enabled = true;
    config.schema.from_csv.tags.enabled = true;
    config.schema.from_csv.tags.file_path = "table_names_and_tags.csv";
    config.schema.from_csv.tags.exclude_indices = {2,3};
    config.schema.tags = {
        {"tag1", "float"},
        {"tag2", "varchar(20)"}
    };
    config.schema.apply();

    // Create action instance
    std::cout << "Creating action instance for child table from CSV..." << std::endl;
    auto action = ActionFactory::instance().create_action(
        global,
        "tdengine/create-child-table",
        config
    );

    // Execute child table creation action from CSV
    std::cout << "Executing child table creation from CSV..." << std::endl;
    action->execute();

    std::cout << "test_create_child_table_action_from_csv passed\n";
}

int main() {
    std::cout << "Running create-child-table-action tests..." << std::endl;

    try {
        test_create_child_table_action_from_generator();
        test_create_child_table_action_from_csv();
    } catch (const std::exception& e) {
        std::cerr << "Test exception: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "All action tests completed." << std::endl;
    return 0;
}