#pragma once

#include "CreateDatabaseConfig.hpp"
#include "FormatResult.hpp"
#include "SqlInsertData.hpp"
#include "StmtV2InsertData.hpp"
#include <memory>
#include <string>

class DatabaseConnector {
public:
    virtual ~DatabaseConnector() = default;

    // Connection management
    virtual bool connect() = 0;
    virtual void close() noexcept = 0;

    // Database operations
    virtual bool select_db(const std::string& db_name) = 0;
    virtual bool prepare(const std::string& sql) = 0;
    virtual bool execute(const std::string& sql) = 0;
    virtual bool execute(const SqlInsertData& data) = 0;
    virtual bool execute(const StmtV2InsertData& data) = 0;

    // State management
    virtual bool is_connected() const = 0;
    virtual bool is_valid() const = 0;
    virtual void reset_state() noexcept = 0;
};