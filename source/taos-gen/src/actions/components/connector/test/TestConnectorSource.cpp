#include "ConnectorSource.hpp"
#include <cassert>
#include <iostream>

void test_connector_source_with_pool() {
    TDengineConfig info;
    info.database.clear();
    info.pool.enabled = true;
    info.pool.min_size = 2;
    info.pool.max_size = 4;
    info.pool.timeout = 100;

    ConnectorSource source(info);

    assert(source.total_connections() == info.pool.min_size);
    assert(source.available_connections() == info.pool.min_size);
    assert(source.active_connections() == 0);

    auto conn = source.get_connector();
    assert(conn != nullptr);
    assert(source.available_connections() == info.pool.min_size - 1);
    assert(source.active_connections() == 1);

    conn->close();

    std::cout << "test_connector_source_with_pool passed\n";
}

void test_connector_source_without_pool() {
    TDengineConfig info;
    info.database.clear();
    info.pool.enabled = false;

    ConnectorSource source(info);

    assert(source.total_connections() == 0);
    assert(source.available_connections() == 0);
    assert(source.active_connections() == 0);

    auto conn = source.get_connector();
    assert(conn != nullptr);

    std::cout << "test_connector_source_without_pool passed\n";
}

int main() {
    test_connector_source_with_pool();
    test_connector_source_without_pool();

    std::cout << "All ConnectorSource tests passed!" << std::endl;
    return 0;
}