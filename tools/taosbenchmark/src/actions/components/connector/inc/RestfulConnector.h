#pragma once
#include <taos.h>
#include "DatabaseConnector.h"

class RestfulConnector final : public DatabaseConnector {
public:
    explicit RestfulConnector(const ConnectionInfo& conn_info);
    ~RestfulConnector() override;

    bool connect() override;
    bool execute(const std::string& sql) override;
    void close() noexcept override;

private:

    ConnectionInfo conn_info_;
    bool is_connected_{false};
};