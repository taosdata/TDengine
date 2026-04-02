#pragma once
#include "ColumnGenerator.hpp"
#include "ExpressionEngine.hpp"


class ExprColumnGenerator : public ColumnGenerator {
public:
    // using ColumnGenerator::ColumnGenerator;
    explicit ExprColumnGenerator(const ColumnConfigInstance& instance);
    ExprColumnGenerator(const std::string& table_name, const ColumnConfigInstance& instance);
    ~ExprColumnGenerator() override = default;

    ColumnType generate() const override;
    ColumnTypeVector generate(size_t count) const override;

private:
    // mutable int64_t counter_ = 0;
    mutable ExpressionEngine engine_;
};