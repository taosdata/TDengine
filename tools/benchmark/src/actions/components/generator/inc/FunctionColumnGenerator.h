#pragma once
#include "ColumnGenerator.h"


class FunctionColumnGenerator : public ColumnGenerator {
public:
    using ColumnGenerator::ColumnGenerator;
    
    ColumnType generate() const override;
    ColumnTypeVector generate(size_t count) const override;

private:
    mutable int64_t counter_ = 0;
};