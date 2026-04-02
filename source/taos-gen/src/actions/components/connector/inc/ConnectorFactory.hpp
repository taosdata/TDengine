#pragma once

#include "DatabaseConnector.hpp"
#include <memory>

class ConnectorFactory {
public:
    static std::unique_ptr<DatabaseConnector> create(const TDengineConfig& connInfo);
};