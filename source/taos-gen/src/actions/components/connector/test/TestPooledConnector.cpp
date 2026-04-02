#include "PooledConnector.hpp"
#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>

struct DummyConnector : public DatabaseConnector {
    bool valid = true;
    bool connected = true;
    bool connect() override { return true; }
    void close() noexcept override {}
    bool select_db(const std::string&) override { return true; }
    bool prepare(const std::string&) override { return true; }
    bool execute(const std::string&) override { return true; }
    bool execute(const SqlInsertData&) override { return true; }
    bool execute(const StmtV2InsertData&) override { return true; }
    bool is_connected() const override { return connected; }
    bool is_valid() const override { return valid; }
    void reset_state() noexcept override {}
    ~DummyConnector() override = default;
};

namespace ConnectorFactory {
    std::unique_ptr<DatabaseConnector> create(const DataChannel&, const TDengineConfig&) {
        return std::make_unique<DummyConnector>();
    }
}

void test_pooled_connector_constructor_valid() {
    TDengineConfig info;
    ConnectionPoolImpl pool(info);
    auto real_conn = std::make_unique<DummyConnector>();
    PooledConnector pooled(std::move(real_conn), pool);
    // No exception, object constructed
    assert(true);
    std::cout << "test_pooled_connector_constructor_valid passed\n";
}

void test_pooled_connector_constructor_nullptr() {
    TDengineConfig info;
    ConnectionPoolImpl pool(info);
    bool exception_thrown = false;
    try {
        PooledConnector pooled(nullptr, pool);
    } catch (const std::invalid_argument&) {
        exception_thrown = true;
    }
    (void)exception_thrown;
    assert(exception_thrown);
    std::cout << "test_pooled_connector_constructor_nullptr passed\n";
}

int main() {
    test_pooled_connector_constructor_valid();
    test_pooled_connector_constructor_nullptr();
    std::cout << "All PooledConnector constructor tests passed!" << std::endl;
    return 0;
}