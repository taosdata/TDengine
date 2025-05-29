#pragma once
#include <memory>
#include "ColumnGenerator.h"
#include "ColumnsConfig.h"

class ColumnGeneratorFactory {
public:
    static std::unique_ptr<ColumnGenerator> create(const ColumnConfig& config);
};