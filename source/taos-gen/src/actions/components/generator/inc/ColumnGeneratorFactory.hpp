#pragma once
#include <memory>
#include "ColumnGenerator.hpp"
#include "ColumnConfigInstance.hpp"

class ColumnGeneratorFactory {
public:
    static std::unique_ptr<ColumnGenerator> create(const std::string& table_name, const ColumnConfigInstance& instance);
};