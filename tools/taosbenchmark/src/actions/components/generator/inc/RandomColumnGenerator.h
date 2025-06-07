#pragma once
#include "ColumnGenerator.h"

class RandomColumnGenerator : public ColumnGenerator {
public:
    using ColumnGenerator::ColumnGenerator;
    // RandomColumnGenerator(const ColumnConfigInstance& instance);
    // ~RandomColumnGenerator() override = default;

    ColumnType generate() const override;
    ColumnTypeVector generate(size_t count) const override;
};