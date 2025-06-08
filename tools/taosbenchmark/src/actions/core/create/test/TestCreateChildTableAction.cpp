#include <iostream>
#include <cassert>
#include "CreateChildTableAction.h"
#include "ActionFactory.h"
#include "DatabaseConnector.h"
#include "CreateChildTableConfig.h"


void test_create_child_table_action_from_generator() {
    ConnectionInfo conn_info;
    conn_info.host = "localhost";
    conn_info.port = 6030;
    conn_info.user = "root";
    conn_info.password = "taosdata";

    DataChannel channel;
    channel.channel_type = "native";

    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.connection_info = conn_info;
    config.data_format = format;
    config.data_channel = channel;
    config.database_info.name = "test_action_db";
    config.super_table_info.name = "test_super_table";

    // 配置子表名生成器
    config.child_table_info.table_name.source_type = "generator";
    config.child_table_info.table_name.generator.prefix = "d";
    config.child_table_info.table_name.generator.count = 10;

    // 配置标签生成器
    config.child_table_info.tags.source_type = "generator";
    config.child_table_info.tags.generator.schema = {
        {"tag1", "float", "random", 1.5, 3.5},
        {"tag2", "nchar", "random", 20}
    };

    // 创建动作实例
    std::cout << "Creating action instance for child table from generator..." << std::endl;
    auto action = ActionFactory::instance().create_action(
        "actions/create-child-table",
        config
    );

    // 执行子表创建动作
    std::cout << "Executing child table creation..." << std::endl;
    action->execute();


    std::cout << "=== Test completed ===" << std::endl;
}

void test_create_child_table_action_from_csv() {
    ConnectionInfo conn_info;
    conn_info.host = "localhost";
    conn_info.port = 6030;
    conn_info.user = "root";
    conn_info.password = "taosdata";

    DataChannel channel;
    channel.channel_type = "native";

    DataFormat format;
    format.format_type = "sql";

    CreateChildTableConfig config;
    config.connection_info = conn_info;
    config.data_format = format;
    config.data_channel = channel;
    config.database_info.name = "test_action_db";
    config.super_table_info.name = "test_super_table";

    // 配置子表名 CSV 文件路径
    config.child_table_info.table_name.source_type = "csv";
    config.child_table_info.table_name.csv.file_path = "table_names.csv";

    // 配置标签 CSV 文件路径
    config.child_table_info.tags.source_type = "csv";
    config.child_table_info.tags.csv.file_path = "tags.csv";
    config.child_table_info.tags.csv.schema = {
        {"tag1", "float", "random", 1.5, 3.5},
        {"tag2", "nchar", "random", 20}
    };

    // 创建动作实例
    std::cout << "Creating action instance for child table from CSV..." << std::endl;
    auto action = ActionFactory::instance().create_action(
        "actions/create-child-table",
        config
    );

    // 执行子表创建动作
    std::cout << "Executing child table creation from CSV..." << std::endl;
    action->execute();

    std::cout << "=== Test completed ===" << std::endl;
}


int main() {
    std::cout << "Running create-child-table-action tests..." << std::endl;

    try {
        test_create_child_table_action_from_generator();
        // test_create_child_table_action_from_csv();
    } catch (const std::exception& e) {
        std::cerr << "测试异常: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "All action tests completed." << std::endl;
    return 0;
}