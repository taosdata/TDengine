#include "RestfulConnector.hpp"
#include "LogUtils.hpp"
#include <iostream>


RestfulConnector::RestfulConnector(const TDengineConfig& conn_info)
    : conn_info_(conn_info) {}

RestfulConnector::~RestfulConnector() {
    close();
}

bool RestfulConnector::connect() {
    throw std::runtime_error("RestfulConnector::connect is not implemented.");
}

bool RestfulConnector::is_connected() const {
    throw std::runtime_error("RestfulConnector::is_connected is not implemented.");
}

bool RestfulConnector::is_valid() const {
    throw std::runtime_error("RestfulConnector::is_valid is not implemented.");
}

bool RestfulConnector::select_db(const std::string& db_name) {
    (void)db_name;
    throw std::runtime_error("RestfulConnector::select_db is not implemented.");
}

bool RestfulConnector::prepare(const std::string& sql) {
    (void)sql;
    throw std::runtime_error("RestfulConnector::prepare is not implemented.");
}

bool RestfulConnector::execute(const std::string& sql) {
    (void)sql;
    throw std::runtime_error("RestfulConnector::execute is not implemented.");
}

bool RestfulConnector::execute(const SqlInsertData& data) {
    (void)data;
    throw std::runtime_error("RestfulConnector::execute is not implemented.");
}

bool RestfulConnector::execute(const StmtV2InsertData& data) {
    (void)data;
    throw std::runtime_error("RestfulConnector::execute is not implemented.");
}

void RestfulConnector::reset_state() noexcept {
    LogUtils::error("RestfulConnector::reset_state is not implemented");
    std::abort();
}

void RestfulConnector::close() noexcept {
    LogUtils::error("RestfulConnector::close is not implemented");
    std::abort();
}