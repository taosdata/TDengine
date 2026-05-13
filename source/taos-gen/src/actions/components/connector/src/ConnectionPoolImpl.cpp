#include "ConnectionPoolImpl.hpp"
#include "ConnectorFactory.hpp"
#include "PooledConnector.hpp"
#include "LogUtils.hpp"
#include <iostream>
#include <stdexcept>

ConnectionPoolImpl::ConnectionPoolImpl(const TDengineConfig& conn_info) : conn_info_(conn_info)
{
    initialize();
}

ConnectionPoolImpl::~ConnectionPoolImpl() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        is_shutting_down_ = true;
    }

    condition_.notify_all();
    close_all_connections();
}

void ConnectionPoolImpl::initialize() {
    create_connections(conn_info_.pool.min_size);
}

void ConnectionPoolImpl::create_connections_locked(size_t count) {
    for (size_t i = 0; i < count; ++i) {
        if (total_count_ >= conn_info_.pool.max_size) break;

        try {
            std::unique_ptr<DatabaseConnector> conn = ConnectorFactory::create(conn_info_);

            if (conn->connect()) {
                available_connections_.push(std::move(conn));
                total_count_++;
            } else {
                LogUtils::error("Connection creation failed: connect error");
            }
        } catch (const std::exception& e) {
            LogUtils::error("Connection creation failed: {}", e.what());
        }
    }
}

void ConnectionPoolImpl::create_connections(size_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    create_connections_locked(count);
}

std::unique_ptr<DatabaseConnector> ConnectionPoolImpl::get_connector() {
    std::unique_lock<std::mutex> lock(mutex_);

    // Expand the pool if needed
    if (available_connections_.empty() && total_count_ < conn_info_.pool.max_size) {
        create_connections_locked(1);
    }

    // Wait for an available connection
    if (available_connections_.empty()) {
        auto status = condition_.wait_for(
            lock,
            std::chrono::milliseconds(conn_info_.pool.timeout),
            [this] { return !available_connections_.empty() ||
                is_shutting_down_;
            }
        );

        if (!status) {
            throw std::runtime_error("Timeout waiting for database connection");
        }
    }

    if (is_shutting_down_) {
        throw std::runtime_error("Connection pool is shutting down");
    }

    // Take out a connection
    auto real_conn = std::move(available_connections_.front());
    available_connections_.pop();

    // Check if the connection is valid
    if (!real_conn->is_valid()) {
        lock.unlock();
        try {
            auto new_conn = ConnectorFactory::create(conn_info_);
            if (new_conn->connect()) {
                real_conn = std::move(new_conn);
            } else {
                throw std::runtime_error("Failed to reconnect");
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            total_count_--;
            throw;
        }
        lock.lock();
    }

    return std::make_unique<PooledConnector>(std::move(real_conn), *this);
}

void ConnectionPoolImpl::return_connection(
    std::unique_ptr<DatabaseConnector> conn)
{
    conn->reset_state();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (is_shutting_down_) {
            conn->close();
            total_count_--;
            return;
        }

        available_connections_.push(std::move(conn));
    }
    condition_.notify_one();
}

void ConnectionPoolImpl::close_all_connections() {
    std::queue<std::unique_ptr<DatabaseConnector>> to_close;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::swap(available_connections_, to_close);
        total_count_ = 0;
    }

    while (!to_close.empty()) {
        auto conn = std::move(to_close.front());
        to_close.pop();
        conn->close();
    }
}

size_t ConnectionPoolImpl::total_connections() const {
    return total_count_.load();
}

size_t ConnectionPoolImpl::available_connections() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return available_connections_.size();
}

size_t ConnectionPoolImpl::active_connections() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return total_count_.load() - available_connections_.size();
}