#include <iostream>
#include <cassert>
#include "CreateSuperTableAction.hpp"
#include "ActionFactory.hpp"
#include "DatabaseConnector.hpp"
#include "CreateSuperTableConfig.hpp"


void test_create_super_table_action() {
    GlobalConfig global;
    TDengineConfig conn_info("taos://root:taosdata@localhost:6030/test_action");

    CreateSuperTableConfig config;
    config.tdengine = conn_info;
    config.schema.name = "test_super_table";

    // Add columns
    config.schema.columns = {
        {"col1", "INT", "random"},
        {"col2", "DOUBLE", "random", 0, 100}
    };

    // Add tags
    config.schema.tags = {
        {"tag1", "FLOAT", "random"},
        {"tag2", "VARCHAR(20)", "random"}
    };
    config.schema.apply();

    // Create action instance
    std::cout << "Creating action instance for super table..." << std::endl;
    auto action = ActionFactory::instance().create_action(
        global,
        "tdengine/create-super-table",
        config
    );

    // Execute super table creation action
    std::cout << "Executing super table creation..." << std::endl;
    action->execute();

    // // Verify super table creation
    // std::cout << "Verifying super table creation..." << std::endl;
    // auto connector = ConnectorFactory::create(conn_info);
    // if (!connector->connect()) {
    //     std::cerr << "Verification failed: cannot connect to database" << std::endl;
    //     return;
    // }

    // // Check if super table exists
    // bool table_exists = false;
    // connector->query("SHOW TABLES", [&](const QueryResult& result) {
    //     for (const auto& row : result.rows) {
    //         if (row[0] == config.schema.name) {
    //             table_exists = true;
    //             break;
    //         }
    //     }
    // });

    // connector->close();

    // if (table_exists) {
    //     std::cout << "Verification succeeded: super table '" << config.schema.name
    //               << "' created" << std::endl;
    // } else {
    //     std::cerr << "Verification failed: super table not created" << std::endl;
    // }

    // Clean up test super table
    // std::cout << "Cleaning up test super table..." << std::endl;
    // auto cleanup_connector = ConnectorFactory::create(conn_info);
    // if (cleanup_connector->connect()) {
    //     cleanup_connector->execute("DROP TABLE IF EXISTS `" + config.schema.name + "`");
    //     cleanup_connector->close();
    // }

    std::cout << "=== Test completed ===" << std::endl;
}

int main() {
    std::cout << "Running create-super-table-action tests..." << std::endl;

    try {
        test_create_super_table_action();
    } catch (const std::exception& e) {
        std::cerr << "Test exception: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "All action tests completed." << std::endl;
    return 0;
}