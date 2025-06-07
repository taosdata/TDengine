#pragma once
#include "ColumnGenerator.h"


class OrderColumnGenerator : public ColumnGenerator {
public:
    // using ColumnGenerator::ColumnGenerator;
    OrderColumnGenerator(const ColumnConfigInstance& instance);
    ~OrderColumnGenerator() override = default;

    ColumnType generate() const override;
    ColumnTypeVector generate(size_t count) const override;

private:
    mutable int64_t current_;
};