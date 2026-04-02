#include "PooledConnector.hpp"

PooledConnector::PooledConnector(
    std::unique_ptr<DatabaseConnector> real_conn,
    ConnectionPoolImpl& pool)
    : real_conn_(std::move(real_conn)), pool_(pool)
{
    if (!real_conn_) {
        throw std::invalid_argument("Real connection cannot be null");
    }
}

PooledConnector::~PooledConnector() {
    if (!is_closed_) {
        close();
    }
}

bool PooledConnector::connect() {
    return true;
}

void PooledConnector::close() noexcept {
    if (!is_closed_) {
        is_closed_ = true;
        pool_.return_connection(std::move(real_conn_));
    }
}

bool PooledConnector::select_db(const std::string& db_name) {
    return real_conn_->select_db(db_name);
}

bool PooledConnector::prepare(const std::string& sql) {
    return real_conn_->prepare(sql);
}

bool PooledConnector::execute(const std::string& sql) {
    return real_conn_->execute(sql);
}

bool PooledConnector::execute(const SqlInsertData& data) {
    return real_conn_->execute(data);
}

bool PooledConnector::execute(const StmtV2InsertData& data) {
    return real_conn_->execute(data);
}

bool PooledConnector::is_connected() const {
    return real_conn_->is_connected();
}

bool PooledConnector::is_valid() const {
    return real_conn_->is_valid();
}

void PooledConnector::reset_state() noexcept {
    real_conn_->reset_state();
}