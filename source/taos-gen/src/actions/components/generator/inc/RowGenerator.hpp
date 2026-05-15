#pragma once

#include <vector>
#include <memory>
#include <random>
#include "TimestampGenerator.hpp"
#include "ColumnGenerator.hpp"


class RowGenerator {
public:

    explicit RowGenerator(const ColumnConfigInstanceVector& col_instances);
    RowGenerator(const std::string& table_name, const ColumnConfigInstanceVector& col_instances);

    RowGenerator(const TimestampGeneratorConfig& ts_config, const ColumnConfigInstanceVector& col_instances);

    RowType generate() const;

    void generate(RowType& columns) const;

    std::vector<RowType> generate(size_t count) const;

private:
    void apply_null_none(RowType& row, size_t col_offset) const;

    std::string table_name_;
    std::unique_ptr<TimestampGenerator> timestamp_gen_;
    std::vector<std::unique_ptr<ColumnGenerator>> column_gens_;

    struct NullNoneRatio {
        float null_ratio = 0.0f;
        float none_ratio = 0.0f;
        bool has_ratio() const { return null_ratio > 0.0f || none_ratio > 0.0f; }
    };
    std::vector<NullNoneRatio> ratios_;
    bool has_any_ratio_ = false;
};