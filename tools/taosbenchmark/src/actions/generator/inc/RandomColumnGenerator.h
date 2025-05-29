#pragma once
#include "ColumnGenerator.h"

class RandomColumnGenerator : public ColumnGenerator {
public:
    using ColumnGenerator::ColumnGenerator;
    // RandomColumnGenerator(const ColumnConfig& config);
    // ~RandomColumnGenerator() override = default;

    ColumnType generate() const override;
    ColumnTypeVector generate(size_t count) const override;
};