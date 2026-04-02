#include "TDengineConfig.hpp"
#include <cassert>
#include <iostream>
#include <stdexcept>

void test_taos_ws_full_dsn() {
    TDengineConfig conn("taos+ws://root:taosdata@localhost:6041/tsbench");
    assert(conn.protocol_type == TDengineConfig::ProtocolType::WebSocket);
    assert(conn.user == "root");
    assert(conn.password == "taosdata");
    assert(conn.host == "localhost");
    assert(conn.port == 6041);
    assert(conn.database == "tsbench");
    std::cout << "test_taos_ws_full_dsn passed.\n";
}

void test_taos_native_with_user() {
    TDengineConfig conn("taos://user@127.0.0.1:6030/dbname");
    assert(conn.protocol_type == TDengineConfig::ProtocolType::Native);
    assert(conn.user == "user");
    assert(conn.password.empty());
    assert(conn.host == "127.0.0.1");
    assert(conn.port == 6030);
    assert(conn.database == "dbname");
    std::cout << "test_taos_native_with_user passed.\n";
}

void test_taos_native_no_user() {
    TDengineConfig conn("taos://localhost/db");
    assert(conn.protocol_type == TDengineConfig::ProtocolType::Native);
    assert(conn.user == "root");            // default user
    assert(conn.password == "taosdata");    // default password
    assert(conn.port == 6030);              // default port
    assert(conn.host == "localhost");
    assert(conn.database == "db");
    std::cout << "test_taos_native_no_user passed.\n";
}

void test_taos_ws_no_db() {
    TDengineConfig conn("taos+ws://root:pwd@host:12345/");
    assert(conn.protocol_type == TDengineConfig::ProtocolType::WebSocket);
    assert(conn.user == "root");
    assert(conn.password == "pwd");
    assert(conn.host == "host");
    assert(conn.port == 12345);
    assert(conn.database.empty());
    std::cout << "test_taos_ws_no_db passed.\n";
}

void test_invalid_protocol() {
    try {
        TDengineConfig conn("mysql://user:pwd@host:3306/db");
        std::cerr << "Test invalid_protocol failed: Exception not thrown for invalid protocol\n";
        std::exit(1);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("unsupported protocol") != std::string::npos);
    }
    std::cout << "test_invalid_protocol passed.\n";
}

void test_missing_protocol_separator() {
    try {
        TDengineConfig conn("taos+ws-root:pwd@host:6041/db");
        std::cerr << "Test missing_protocol_separator failed: Exception not thrown for missing ://\n";
        std::exit(1);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("missing '://'") != std::string::npos);
    }
    std::cout << "test_missing_protocol_separator passed.\n";
}

void test_invalid_port() {
    try {
        TDengineConfig conn("taos://user:pwd@host:70000/db");
        std::cerr << "Test invalid_port failed: Exception not thrown for invalid port\n";
        std::exit(1);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Invalid port number") != std::string::npos);
    }
    std::cout << "test_invalid_port passed.\n";
}

int main() {
    test_taos_ws_full_dsn();
    test_taos_native_with_user();
    test_taos_native_no_user();
    test_taos_ws_no_db();
    test_invalid_protocol();
    test_missing_protocol_separator();
    test_invalid_port();
    std::cout << "All TDengineConfig tests passed!\n";
    return 0;
}