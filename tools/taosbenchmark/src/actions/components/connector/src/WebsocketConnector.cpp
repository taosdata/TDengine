#include "WebsocketConnector.h"
#include <iostream>


WebsocketConnector::WebsocketConnector(const ConnectionInfo& conn_info)
    : conn_info_(conn_info) {}

WebsocketConnector::~WebsocketConnector() {
    close();
}

bool WebsocketConnector::connect() {
    throw std::runtime_error("WebsocketConnector::connect is not implemented.");
}

bool WebsocketConnector::execute(const std::string& sql) {
    throw std::runtime_error("WebsocketConnector::execute is not implemented.");
}

void WebsocketConnector::close() noexcept {
    std::cerr << "WebsocketConnector::close is not implemented." << std::endl;
    std::abort();
}