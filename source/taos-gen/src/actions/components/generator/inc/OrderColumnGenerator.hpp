#pragma once
#include "ColumnGenerator.hpp"
#include <functional>


class OrderColumnGenerator : public ColumnGenerator {
public:
    // using ColumnGenerator::ColumnGenerator;
    OrderColumnGenerator(const ColumnConfigInstance& instance);
    ~OrderColumnGenerator() override = default;

    ColumnType generate() const override;
    ColumnTypeVector generate(size_t count) const override;

private:
    int64_t min_;
    int64_t max_;
    mutable int64_t current_;
    std::function<ColumnType(int64_t)> convert_;
};