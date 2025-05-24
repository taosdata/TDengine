#include "NativeConnector.h"
#include <iostream>

NativeConnector::NativeConnector(const ConnectionInfo& conn_info)
    : conn_info_(conn_info) {}

NativeConnector::~NativeConnector() {
    close();
}

bool NativeConnector::connect() {
    if (is_connected_) return true;

    conn_ = taos_connect(conn_info_.host.c_str(),
                         conn_info_.user.c_str(),
                         conn_info_.password.c_str(),
                         nullptr,
                         conn_info_.port);

    if (conn_ == nullptr) {
        std::cerr << "Native connection failed: " << taos_errstr(conn_) << std::endl;
        return false;
    }

    is_connected_ = true;
    return true;
}

bool NativeConnector::execute(const std::string& sql) {
    if (!is_connected_ && !connect()) return false;

    TAOS_RES* res = taos_query(conn_, sql.c_str());
    const int code = taos_errno(res);
    const bool success = (code == 0);

    if (!success) {
        std::cerr << "Native execute failed [" << code << "]: " 
                  << taos_errstr(res) << std::endl;
    }

    taos_free_result(res);
    return success;
}

void NativeConnector::close() noexcept {
    if (conn_) {
        taos_close(conn_);
        conn_ = nullptr;
    }
    is_connected_ = false;
}
