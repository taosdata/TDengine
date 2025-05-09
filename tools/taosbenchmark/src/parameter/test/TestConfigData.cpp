#include "ConfigData.h"
#include <cassert>
#include <iostream>
#include <stdexcept>

int main() {
    GlobalConfig config;

    // 测试 1: 标准带端口和查询参数
    try {
        config.parse_dsn("http://127.0.0.1:6041?user=admin&password=Taos123!");
        assert(config.host == "127.0.0.1");
        assert(config.port == 6041);
        assert(config.user == "admin");
        assert(config.password == "Taos123!");
    } catch (const std::exception& e) {
        std::cerr << "Test 1 failed: " << e.what() << "\n";
        return 1;
    }

    // 测试 2: 无端口，使用默认值
    try {
        config.parse_dsn("https://gw.cloud.taosdata.com?token=abcdef123456");
        assert(config.host == "gw.cloud.taosdata.com");
        assert(config.port == 6041);  // 保留之前的值，未重置
        assert(config.password == "abcdef123456");  // token 映射到 password
    } catch (const std::exception& e) {
        std::cerr << "Test 2 failed: " << e.what() << "\n";
        return 1;
    }

    // 测试 3: 错误格式（缺少 ://）
    try {
        config.parse_dsn("invalid_dsn");
        std::cerr << "Test 3 failed: Exception not thrown for invalid DSN\n";
        return 1;
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("missing '://'") != std::string::npos);
    }

    // 测试 4: 非法端口号
    try {
        config.parse_dsn("http://localhost:70000");  // 端口超过 65535
        std::cerr << "Test 4 failed: Exception not thrown for invalid port\n";
        return 1;
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Invalid port number") != std::string::npos);
    }

    std::cout << "All tests passed!\n";
    return 0;
}