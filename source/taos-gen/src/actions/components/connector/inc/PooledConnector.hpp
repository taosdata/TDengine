#pragma once

#include "DatabaseConnector.hpp"
#include "ConnectionPoolImpl.hpp"

class PooledConnector : public DatabaseConnector {
public:
    PooledConnector(std::unique_ptr<DatabaseConnector> real_conn, ConnectionPoolImpl& pool);

    ~PooledConnector() override;

    PooledConnector(const PooledConnector&) = delete;
    PooledConnector& operator=(const PooledConnector&) = delete;

    bool connect() override;
    void close() noexcept override;

    bool select_db(const std::string& db_name) override;
    bool prepare(const std::string& sql) override;
    bool execute(const std::string& sql) override;
    bool execute(const SqlInsertData& data) override;
    bool execute(const StmtV2InsertData& data) override;

    bool is_connected() const override;
    bool is_valid() const override;
    void reset_state() noexcept override;

private:
    std::unique_ptr<DatabaseConnector> real_conn_;
    ConnectionPoolImpl& pool_;
    bool is_closed_ = false;

};