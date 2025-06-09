#include "RowGenerator.h"
#include <stdexcept>
#include "ColumnGeneratorFactory.h"


RowGenerator::RowGenerator(const ColumnConfigInstanceVector& col_instances) {
    for (const auto& instance : col_instances) {
        auto generator = ColumnGeneratorFactory::create(instance);
        if (generator) {
            column_gens_.push_back(std::move(generator));
        } else {
            throw std::runtime_error("Failed to create generator for column: " + instance.name());
        }
    }
}

RowGenerator::RowGenerator(const TimestampGeneratorConfig& ts_config, const ColumnConfigInstanceVector& col_instances)
    : RowGenerator(col_instances) {
    timestamp_gen_ = std::make_unique<TimestampGenerator>(ts_config);
}

RowType RowGenerator::generate() const {
    RowType row;
    row.reserve(column_gens_.size() + (timestamp_gen_ ? 1 : 0));
    
    if (timestamp_gen_) {
        row.push_back(timestamp_gen_->generate());
    }
    
    for (const auto& gen : column_gens_) {
        row.push_back(gen->generate());
    }
    
    return row;
}

std::vector<RowType> RowGenerator::generate(size_t count) const {
    std::vector<RowType> rows;
    rows.reserve(count);

    std::vector<Timestamp> timestamps;
    if (timestamp_gen_) {
        timestamps = timestamp_gen_->generate(count);
    }

    std::vector<ColumnTypeVector> columns;
    columns.reserve(column_gens_.size());

    for (const auto& gen : column_gens_) {
        columns.push_back(gen->generate(count));
    }

    for (size_t i = 0; i < count; ++i) {
        RowType row;
        row.reserve(columns.size() + (timestamp_gen_ ? 1 : 0));

        if (timestamp_gen_) {
            row.push_back(timestamps[i]);
        }
        
        for (const auto& column : columns) {
            row.push_back(column[i]);
        }

        rows.push_back(std::move(row));
    }
    
    return rows;
}