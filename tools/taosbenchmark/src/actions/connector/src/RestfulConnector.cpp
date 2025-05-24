#include "RestfulConnector.h"
#include <iostream>


RestfulConnector::RestfulConnector(const ConnectionInfo& conn_info)
    : conn_info_(conn_info) {}

RestfulConnector::~RestfulConnector() {
    close();
}

bool RestfulConnector::connect() {
    throw std::runtime_error("RestfulConnector::connect is not implemented.");
}

bool RestfulConnector::execute(const std::string& sql) {
    throw std::runtime_error("RestfulConnector::execute is not implemented.");
}

void RestfulConnector::close() noexcept {
    std::cerr << "RestfulConnector::close is not implemented." << std::endl;
    std::abort();
}