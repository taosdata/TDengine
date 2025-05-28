#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include "ColumnConfig.h"

struct SuperTableInfo {
    std::string name;
    std::vector<ColumnConfig> columns;
    std::vector<ColumnConfig> tags;
};
