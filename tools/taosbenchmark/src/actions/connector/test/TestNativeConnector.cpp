#include "NativeConnector.h"
#include <iostream>

void test_native_connector_create_database() {
    // 配置连接信息
    ConnectionInfo conn_info;
    conn_info.host = "localhost";
    conn_info.port = 6030;
    conn_info.user = "root";
    conn_info.password = "taosdata";

    // 创建 NativeConnector 实例
    NativeConnector connector(conn_info);

    // 测试连接
    if (!connector.connect()) {
        std::cerr << "Failed to connect to TDengine." << std::endl;
        return;
    }
    std::cout << "Connected to TDengine successfully." << std::endl;

    // 测试执行 SQL
    const std::string sql = "CREATE DATABASE IF NOT EXISTS `test_native_connector`";
    if (!connector.execute(sql)) {
        std::cerr << "Failed to execute SQL: " << sql << std::endl;
        connector.close();
        return;
    }
    std::cout << "SQL executed successfully: " << sql << std::endl;

    // 关闭连接
    connector.close();
    std::cout << "Connection closed successfully." << std::endl;
}

int main() {
    std::cout << "Running NativeConnector tests..." << std::endl;

    // 调用测试函数
    test_native_connector_create_database();

    std::cout << "NativeConnector tests completed." << std::endl;
    return 0;
}