#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include "ColumnConfig.h"

struct SuperTableInfo {
    std::string name;
    ColumnConfigVector columns;
    ColumnConfigVector tags;
};
