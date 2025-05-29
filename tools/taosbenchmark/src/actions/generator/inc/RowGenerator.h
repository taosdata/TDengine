#pragma once

#include <vector>
#include <memory>
#include "TimestampGenerator.h"
#include "ColumnGenerator.h"


class RowGenerator {
public:

    explicit RowGenerator(const std::vector<ColumnConfig>& col_configs);

    RowGenerator(const TimestampGeneratorConfig& ts_config, const std::vector<ColumnConfig>& col_configs);

    RowType generate() const;

    std::vector<RowType> generate(size_t count) const;

private:
    std::unique_ptr<TimestampGenerator> timestamp_gen_;
    std::vector<std::unique_ptr<ColumnGenerator>> column_gens_;
};