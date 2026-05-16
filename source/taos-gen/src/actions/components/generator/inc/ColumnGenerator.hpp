#pragma once
#include <vector>
#include <variant>
#include "ColumnConfigInstance.hpp"


class ColumnGenerator {
public:
    explicit ColumnGenerator(const ColumnConfigInstance& instance) : instance_(instance) {}
    virtual ~ColumnGenerator() = default;
    
    virtual ColumnType generate() const = 0;
    
    virtual ColumnTypeVector generate(size_t count) const = 0;

    ColumnConfigInstance instance_;
};
