#pragma once
#include <taos.h>
#include "DatabaseConnector.h"

class NativeConnector final : public DatabaseConnector {
public:
    explicit NativeConnector(const ConnectionInfo& conn_info);
    ~NativeConnector() override;

    bool connect() override;
    bool execute(const std::string& sql) override;
    void close() noexcept override;

private:
    TAOS* conn_{nullptr};
    ConnectionInfo conn_info_;
    bool is_connected_{false};
};