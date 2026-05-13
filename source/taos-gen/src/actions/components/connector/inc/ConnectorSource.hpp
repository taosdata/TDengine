#pragma once

#include "DatabaseConnector.hpp"
#include "ConnectionPoolImpl.hpp"
#include <memory>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>

class ConnectorSource {
public:
    ConnectorSource(const TDengineConfig& conn_info);
    ~ConnectorSource();

    std::unique_ptr<DatabaseConnector> get_connector();

    size_t total_connections() const;
    size_t available_connections() const;
    size_t active_connections() const;

private:
    std::unique_ptr<ConnectionPoolImpl> pool_impl_;

    std::unique_ptr<DatabaseConnector> create_raw_connection() const;

    const TDengineConfig& conn_info_;

    ConnectorSource(const ConnectorSource&) = delete;
    ConnectorSource& operator=(const ConnectorSource&) = delete;
};