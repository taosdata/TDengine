#pragma once

#include "DatabaseConnector.hpp"
#include <taos.h>

class RestfulConnector final : public DatabaseConnector {
public:
    explicit RestfulConnector(const TDengineConfig& conn_info);
    ~RestfulConnector() override;

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

    TDengineConfig conn_info_;
    // bool is_connected_{false};
};