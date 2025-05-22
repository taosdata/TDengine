#include "ConnectionInfo.h"
#include <cassert>
#include <iostream>
#include <stdexcept>

int main() {
    ConnectionInfo conn;

    // 测试 1: 标准带端口和查询参数
    try {
        conn.parse_dsn("http://127.0.0.1:6041?token=ea7f89ae7c8359f247a5");
        assert(conn.host == "127.0.0.1");
        assert(conn.port == 6041);
        assert(conn.user == "token");
        assert(conn.password == "ea7f89ae7c8359f247a5");
    } catch (const std::exception& e) {
        std::cerr << "Test 1 failed: " << e.what() << "\n";
        return 1;
    }

    // 测试 2: 无端口，使用默认值
    try {
        conn.parse_dsn("https://gw.cloud.taosdata.com?token=5445e0a753cd0a522473");
        assert(conn.host == "gw.cloud.taosdata.com");
        assert(conn.port == 6041);  // 保留之前的值，未重置
        assert(conn.user == "token");
        assert(conn.password == "5445e0a753cd0a522473");
    } catch (const std::exception& e) {
        std::cerr << "Test 2 failed: " << e.what() << "\n";
        return 1;
    }

    // 测试 3: 错误格式（缺少 ://）
    try {
        conn.parse_dsn("invalid_dsn");
        std::cerr << "Test 3 failed: Exception not thrown for invalid DSN\n";
        return 1;
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("missing '://'") != std::string::npos);
    }

    // 测试 4: 非法端口号
    try {
        conn.parse_dsn("http://localhost:70000");  // 端口超过 65535
        std::cerr << "Test 4 failed: Exception not thrown for invalid port\n";
        return 1;
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Invalid port number") != std::string::npos);
    }

    std::cout << "All tests passed!\n";
    return 0;
}