#pragma once
#include <memory>
#include "ColumnGenerator.h"
#include "ColumnConfigInstance.h"

class ColumnGeneratorFactory {
public:
    static std::unique_ptr<ColumnGenerator> create(const ColumnConfigInstance& instance);
};