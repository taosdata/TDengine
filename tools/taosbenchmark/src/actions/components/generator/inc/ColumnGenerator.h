#pragma once
#include <vector>
#include <variant>
#include "ColumnConfig.h"


class ColumnGenerator {
public:
    explicit ColumnGenerator(const ColumnConfig& config) : config_(config) {}
    virtual ~ColumnGenerator() = default;
    
    virtual ColumnType generate() const = 0;
    
    virtual ColumnTypeVector generate(size_t count) const = 0;

protected:
    ColumnConfig config_;
};
