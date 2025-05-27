#include <iostream>
#include <cassert>
#include "CreateDatabaseAction.h"
#include "ActionFactory.h"
#include "DatabaseConnector.h"
#include "CreateDatabaseConfig.h"


void test_create_database_action() {
    ConnectionInfo conn_info;
    conn_info.host = "localhost";
    conn_info.port = 6030;
    conn_info.user = "root";
    conn_info.password = "taosdata";

    DataChannel channel;
    channel.channel_type = "native";

    DataFormat format;
    format.format_type = "sql";

    CreateDatabaseConfig config;
    config.connection_info = conn_info;
    config.data_format = format;
    config.data_channel = channel;
    config.database_info.name = "test_action_db";
    config.database_info.drop_if_exists = true;

    // 创建动作实例
    std::cout << "Creating action instance..." << std::endl;
    auto action = ActionFactory::instance().create_action(
        "actions/create-database",
        config
    );

    // 执行数据库创建动作
    std::cout << "Executing database creation..." << std::endl;
    action->execute();

    // // 验证结果
    // std::cout << "Verifying database creation..." << std::endl;
    // auto connector = DatabaseConnector::create(channel, conn_info);
    // if (!connector->connect()) {
    //     std::cerr << "验证失败：无法连接数据库" << std::endl;
    //     return;
    // }

    // // 检查数据库是否存在
    // bool db_exists = false;
    // connector->query("SHOW DATABASES", [&](const QueryResult& result) {
    //     for (const auto& row : result.rows) {
    //         if (row[0] == config.database_info.name) {
    //             db_exists = true;
    //             break;
    //         }
    //     }
    // });

    // connector->close();

    // if (db_exists) {
    //     std::cout << "验证成功：数据库 '" << config.database_info.name 
    //               << "' 已创建" << std::endl;
    // } else {
    //     std::cerr << "验证失败：数据库未创建" << std::endl;
    // }

    // 清理测试数据库
    // std::cout << "Cleaning up test database..." << std::endl;
    // auto cleanup_connector = DatabaseConnector::create(channel, conn_info);
    // if (cleanup_connector->connect()) {
    //     cleanup_connector->execute("DROP DATABASE IF EXISTS `" + config.database_info.name + "`");
    //     cleanup_connector->close();
    // }

    std::cout << "=== Test completed ===" << std::endl;
}

int main() {
    std::cout << "Running create-database-action tests..." << std::endl;

    try {
        test_create_database_action();
    } catch (const std::exception& e) {
        std::cerr << "测试异常: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "All action tests completed." << std::endl;
    return 0;
}

