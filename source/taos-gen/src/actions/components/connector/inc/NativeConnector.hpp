#pragma once

#include "TDengineConnector.hpp"

class NativeConnector final : public TDengineConnector {
public:
    explicit NativeConnector(const TDengineConfig& conn_info)
        : TDengineConnector(conn_info, "native", "Native") {}
};