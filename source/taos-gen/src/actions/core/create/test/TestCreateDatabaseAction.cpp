#include <iostream>
#include <cassert>
#include "CreateDatabaseAction.hpp"
#include "ActionFactory.hpp"
#include "DatabaseConnector.hpp"
#include "CreateDatabaseConfig.hpp"


void test_create_database_action() {
    GlobalConfig global;
    TDengineConfig conn_info("taos://root:taosdata@localhost:6030/test_action");
    CreateDatabaseConfig config;
    config.tdengine = conn_info;

    // Create action instance
    std::cout << "Creating action instance..." << std::endl;
    auto action = ActionFactory::instance().create_action(
        global,
        "tdengine/create-database",
        config
    );

    // Execute database creation action
    std::cout << "Executing database creation..." << std::endl;
    action->execute();

    // // Verify result
    // std::cout << "Verifying database creation..." << std::endl;
    // auto connector = ConnectorFactory::create(conn_info);
    // if (!connector->connect()) {
    //     std::cerr << "Verification failed: cannot connect to database" << std::endl;
    //     return;
    // }

    // // Check if database exists
    // bool db_exists = false;
    // connector->query("SHOW DATABASES", [&](const QueryResult& result) {
    //     for (const auto& row : result.rows) {
    //         if (row[0] == config.tdengine.database) {
    //             db_exists = true;
    //             break;
    //         }
    //     }
    // });

    // connector->close();

    // if (db_exists) {
    //     std::cout << "Verification succeeded: database '" << config.tdengine.database
    //               << "' created" << std::endl;
    // } else {
    //     std::cerr << "Verification failed: database not created" << std::endl;
    // }

    // Clean up test database
    // std::cout << "Cleaning up test database..." << std::endl;
    // auto cleanup_connector = ConnectorFactory::create(conn_info);
    // if (cleanup_connector->connect()) {
    //     cleanup_connector->execute("DROP DATABASE IF EXISTS `" + config.tdengine.database + "`");
    //     cleanup_connector->close();
    // }

    std::cout << "=== Test completed ===" << std::endl;
}

int main() {
    std::cout << "Running create-database-action tests..." << std::endl;

    try {
        test_create_database_action();
    } catch (const std::exception& e) {
        std::cerr << "Test exception: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "All action tests completed." << std::endl;
    return 0;
}