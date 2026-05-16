
#include "ConnectorFactory.hpp"
#include "NativeConnector.hpp"
#include "WebsocketConnector.hpp"
#include "RestfulConnector.hpp"
#include <stdexcept>

std::unique_ptr<DatabaseConnector> ConnectorFactory::create(const TDengineConfig& conn_info) {
    if (conn_info.protocol_type == TDengineConfig::ProtocolType::Native) {
        return std::make_unique<NativeConnector>(conn_info);
    } else if (conn_info.protocol_type == TDengineConfig::ProtocolType::WebSocket) {
        return std::make_unique<WebsocketConnector>(conn_info);
    } else if (conn_info.protocol_type == TDengineConfig::ProtocolType::RESTful) {
        return std::make_unique<RestfulConnector>(conn_info);
    }

    throw std::invalid_argument("Unsupported channel type: " + std::to_string(static_cast<int>(conn_info.protocol_type)));
}