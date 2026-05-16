#pragma once

#include "taos.h"

class IStmtData {
public:
    virtual ~IStmtData() = default;
    virtual size_t row_count() const noexcept = 0;
    virtual size_t column_count() const noexcept = 0;
    virtual const TAOS_STMT2_BINDV* bindv_ptr() const = 0;
};