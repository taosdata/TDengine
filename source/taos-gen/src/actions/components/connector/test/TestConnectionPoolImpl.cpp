#include "ConnectionPoolImpl.hpp"
#include <cassert>
#include <iostream>
#include <chrono>

void test_pool_initialization() {
    TDengineConfig info;
    info.database.clear();
    info.pool.enabled = true;
    info.pool.min_size = 2;
    info.pool.max_size = 4;
    info.pool.timeout = 100;

    ConnectionPoolImpl pool(info);

    assert(pool.total_connections() == info.pool.min_size);
    assert(pool.available_connections() == info.pool.min_size);
    assert(pool.active_connections() == 0);

    std::cout << "test_pool_initialization passed\n";
}

void test_get_and_return_connection() {
    TDengineConfig info;
    info.database.clear();
    info.pool.enabled = true;
    info.pool.min_size = 2;
    info.pool.max_size = 4;
    info.pool.timeout = 100;

    ConnectionPoolImpl pool(info);

    auto conn = pool.get_connector();
    assert(conn != nullptr);
    assert(pool.available_connections() == info.pool.min_size - 1);
    assert(pool.active_connections() == 1);

    pool.return_connection(std::move(conn));
    assert(pool.available_connections() == info.pool.min_size);
    assert(pool.active_connections() == 0);

    std::cout << "test_get_and_return_connection passed\n";
}

void test_pool_max_size() {
    TDengineConfig info;
    info.database.clear();
    info.pool.enabled = true;
    info.pool.max_size = 2;
    info.pool.min_size = 1;
    info.pool.timeout = 100;

    ConnectionPoolImpl pool(info);

    auto c1 = pool.get_connector();
    auto c2 = pool.get_connector();
    assert(pool.total_connections() == 2);
    assert(pool.available_connections() == 0);
    assert(pool.active_connections() == 2);

    pool.return_connection(std::move(c1));
    pool.return_connection(std::move(c2));
    assert(pool.available_connections() == 2);

    std::cout << "test_pool_max_size passed\n";
}

void test_connection_timeout() {
    TDengineConfig info;
    info.database.clear();
    info.pool.enabled = true;
    info.pool.min_size = 1;
    info.pool.max_size = 1;
    info.pool.timeout = 100; // 100ms

    ConnectionPoolImpl pool(info);

    // Get the only available connection
    auto conn = pool.get_connector();
    assert(conn != nullptr);

    // Now the pool has no available connections, getting another should timeout and throw
    bool timeout_thrown = false;
    auto start = std::chrono::steady_clock::now();
    try {
        pool.get_connector();
    } catch (const std::runtime_error& e) {
        timeout_thrown = std::string(e.what()).find("Timeout") != std::string::npos;
    }
    auto end = std::chrono::steady_clock::now();
    (void)timeout_thrown;
    assert(timeout_thrown);

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    int expected = info.pool.timeout;
    int tolerance = 30;
    (void)elapsed_ms;
    (void)expected;
    (void)tolerance;
    assert(std::abs(elapsed_ms - expected) < tolerance);

    // After returning the connection, it can be acquired again
    pool.return_connection(std::move(conn));
    auto conn2 = pool.get_connector();
    assert(conn2 != nullptr);

    std::cout << "test_connection_timeout passed\n";
}

int main() {
    test_pool_initialization();
    test_get_and_return_connection();
    test_pool_max_size();
    test_connection_timeout();

    std::cout << "All ConnectionPoolImpl tests passed!" << std::endl;
    return 0;
}