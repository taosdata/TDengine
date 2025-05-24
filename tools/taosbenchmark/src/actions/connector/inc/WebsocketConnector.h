#pragma once
#include <taosws.h>
#include "DatabaseConnector.h"

class WebsocketConnector final : public DatabaseConnector {
public:
    explicit WebsocketConnector(const ConnectionInfo& conn_info);
    ~WebsocketConnector() override;

    bool connect() override;
    bool execute(const std::string& sql) override;
    void close() noexcept override;

private:
    WS_TAOS* conn_{nullptr};
    ConnectionInfo conn_info_;
    bool is_connected_{false};
};