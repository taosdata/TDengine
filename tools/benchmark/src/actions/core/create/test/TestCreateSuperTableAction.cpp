#include <iostream>
#include <cassert>
#include "CreateSuperTableAction.h"
#include "ActionFactory.h"
#include "DatabaseConnector.h"
#include "CreateSuperTableConfig.h"


void test_create_super_table_action() {
    ConnectionInfo conn_info;
    conn_info.host = "localhost";
    conn_info.port = 6030;
    conn_info.user = "root";
    conn_info.password = "taosdata";

    DataChannel channel;
    channel.channel_type = "native";

    DataFormat format;
    format.format_type = "sql";

    CreateSuperTableConfig config;
    config.connection_info = conn_info;
    config.data_format = format;
    config.data_channel = channel;
    config.database_info.name = "test_action_db";
    config.super_table_info.name = "test_super_table";

    // 添加列
    config.super_table_info.columns = {
        {"col1", "INT", "random"},
        {"col2", "BINARY", "random", 10}
    };

    // 添加标签
    config.super_table_info.tags = {
        {"tag1", "FLOAT", "random"},
        {"tag2", "NCHAR", "random", 20}
    };

    // 创建动作实例
    std::cout << "Creating action instance for super table..." << std::endl;
    auto action = ActionFactory::instance().create_action(
        "actions/create-super-table",
        config
    );

    // 执行超级表创建动作
    std::cout << "Executing super table creation..." << std::endl;
    action->execute();

    // // 验证超级表是否创建成功
    // std::cout << "Verifying super table creation..." << std::endl;
    // auto connector = DatabaseConnector::create(channel, conn_info);
    // if (!connector->connect()) {
    //     std::cerr << "验证失败：无法连接数据库" << std::endl;
    //     return;
    // }

    // // 检查超级表是否存在
    // bool table_exists = false;
    // connector->query("SHOW TABLES", [&](const QueryResult& result) {
    //     for (const auto& row : result.rows) {
    //         if (row[0] == config.super_table_info.name) {
    //             table_exists = true;
    //             break;
    //         }
    //     }
    // });

    // connector->close();

    // if (table_exists) {
    //     std::cout << "验证成功：超级表 '" << config.super_table_info.name 
    //               << "' 已创建" << std::endl;
    // } else {
    //     std::cerr << "验证失败：超级表未创建" << std::endl;
    // }

    // 清理测试超级表
    // std::cout << "Cleaning up test super table..." << std::endl;
    // auto cleanup_connector = DatabaseConnector::create(channel, conn_info);
    // if (cleanup_connector->connect()) {
    //     cleanup_connector->execute("DROP TABLE IF EXISTS `" + config.super_table_info.name + "`");
    //     cleanup_connector->close();
    // }

    std::cout << "=== Test completed ===" << std::endl;
}

int main() {
    std::cout << "Running create-super-table-action tests..." << std::endl;

    try {
        test_create_super_table_action();
    } catch (const std::exception& e) {
        std::cerr << "测试异常: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "All action tests completed." << std::endl;
    return 0;
}