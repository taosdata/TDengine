#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include "ColumnConfig.hpp"

struct SuperTableInfo {
    std::string name;
    ColumnConfigVector columns;
    ColumnConfigVector tags;
};
