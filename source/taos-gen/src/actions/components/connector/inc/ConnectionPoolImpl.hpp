#pragma once

#include "DataChannel.hpp"
#include "DatabaseConnector.hpp"
#include <memory>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>


class ConnectionPoolImpl {
public:
    ConnectionPoolImpl(const TDengineConfig& conn_info);
    ~ConnectionPoolImpl();

    std::unique_ptr<DatabaseConnector> get_connector();
    void return_connection(std::unique_ptr<DatabaseConnector> conn);

    size_t total_connections() const;
    size_t available_connections() const;
    size_t active_connections() const;

private:
    void initialize();
    void create_connections_locked(size_t count);
    void create_connections(size_t count);
    void close_all_connections();

    const TDengineConfig& conn_info_;

    std::queue<std::unique_ptr<DatabaseConnector>> available_connections_;
    std::atomic<size_t> total_count_{0};

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    bool is_shutting_down_ = false;
};