#pragma once
#include <memory>
#include <string>
#include "CreateDatabaseConfig.h"


class DatabaseConnector {
public:
    virtual ~DatabaseConnector() = default;

    virtual bool connect() = 0;
    virtual bool execute(const std::string& request) = 0;
    virtual void close() noexcept = 0;

    // Factory method with error handling
    static std::unique_ptr<DatabaseConnector> create(
        const DataChannel& channel,
        const ConnectionInfo& connInfo);
};