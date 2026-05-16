#include "ConnectorFactory.hpp"
#include "WebsocketConnector.hpp"
#include <iostream>

void test_websocket_connector_create_database() {
    // Set connection info
    TDengineConfig conn_info;
    conn_info.database.clear();

    // Create connector instance using ConnectorFactory::create
    auto connector = ConnectorFactory::create(conn_info);

    // Test connection
    if (!connector->connect()) {
        std::cerr << "Failed to connect to TDengine." << std::endl;
        return;
    }
    std::cout << "Connected to TDengine successfully." << std::endl;

    // Test SQL execution
    std::string sql = "DROP DATABASE IF EXISTS `test_connector`";
    if (!connector->execute(sql)) {
        std::cerr << "Failed to execute SQL: " << sql << std::endl;
        connector->close();
        return;
    }
    std::cout << "SQL executed successfully: " << sql << std::endl;

    sql = "CREATE DATABASE IF NOT EXISTS `test_connector`";
    if (!connector->execute(sql)) {
        std::cerr << "Failed to execute SQL: " << sql << std::endl;
        connector->close();
        return;
    }
    std::cout << "SQL executed successfully: " << sql << std::endl;

    sql = "DROP DATABASE IF EXISTS `test_connector`";
    if (!connector->execute(sql)) {
        std::cerr << "Failed to execute SQL: " << sql << std::endl;
        connector->close();
        return;
    }
    std::cout << "SQL executed successfully: " << sql << std::endl;

    // Close connection
    connector->close();
    std::cout << "Connection closed successfully." << std::endl;
}

int main() {
    std::cout << "Running WebsocketConnector tests..." << std::endl;

    // Call test function
    test_websocket_connector_create_database();

    std::cout << "WebsocketConnector tests completed." << std::endl;
    return 0;
}