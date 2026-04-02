#pragma once

#include <vector>
#include <memory>
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
    std::string table_name_;
    std::unique_ptr<TimestampGenerator> timestamp_gen_;
    std::vector<std::unique_ptr<ColumnGenerator>> column_gens_;
};