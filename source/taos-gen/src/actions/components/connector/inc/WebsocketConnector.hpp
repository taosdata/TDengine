#pragma once

#include "TDengineConnector.hpp"

class WebsocketConnector final : public TDengineConnector {
public:
    explicit WebsocketConnector(const TDengineConfig& conn_info)
        : TDengineConnector(conn_info, "websocket", "WebSocket") {}
};