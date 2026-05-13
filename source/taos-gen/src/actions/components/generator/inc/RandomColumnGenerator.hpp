#pragma once
#include "ColumnGenerator.hpp"
#include <vector>
#include <functional>

class RandomColumnGenerator : public ColumnGenerator {
public:
    explicit RandomColumnGenerator(const ColumnConfigInstance& instance);
    ~RandomColumnGenerator() override = default;

    ColumnType generate() const override;
    ColumnTypeVector generate(size_t count) const override;

private:
    void initialize_generator();

    std::function<ColumnType()> generator_;
    std::vector<ColumnType> cached_values_;
};