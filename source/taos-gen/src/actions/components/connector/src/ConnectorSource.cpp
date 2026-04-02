#include "ConnectorSource.hpp"
#include "ConnectorFactory.hpp"
#include "PooledConnector.hpp"
#include <iostream>
#include <stdexcept>

ConnectorSource::ConnectorSource(const TDengineConfig& conn_info) : conn_info_(conn_info)
{
    if (conn_info_.pool.enabled) {
        pool_impl_ = std::make_unique<ConnectionPoolImpl>(conn_info);
    }
}

ConnectorSource::~ConnectorSource() {
}

std::unique_ptr<DatabaseConnector> ConnectorSource::get_connector() {
    if (conn_info_.pool.enabled && pool_impl_) {
        // Get connection from the connection pool
        return pool_impl_->get_connector();
    }

    // Create a new connection
    auto conn = create_raw_connection();
    if (conn->connect()) {
        return conn;
    }
    throw std::runtime_error("Failed to connect to database");
}

std::unique_ptr<DatabaseConnector> ConnectorSource::create_raw_connection() const {
    return ConnectorFactory::create(conn_info_);
}

size_t ConnectorSource::total_connections() const {
    return pool_impl_ ? pool_impl_->total_connections() : 0;
}

size_t ConnectorSource::available_connections() const {
    return pool_impl_ ? pool_impl_->available_connections() : 0;
}

size_t ConnectorSource::active_connections() const {
    return pool_impl_ ? pool_impl_->active_connections() : 0;
}