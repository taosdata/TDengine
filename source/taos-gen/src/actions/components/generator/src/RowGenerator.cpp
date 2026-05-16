#include "RowGenerator.hpp"
#include "ColumnGeneratorFactory.hpp"
#include <cassert>
#include <stdexcept>

RowGenerator::RowGenerator(const std::string& table_name, const ColumnConfigInstanceVector& col_instances)
    : table_name_(table_name) {
    for (const auto& instance : col_instances) {
        auto generator = ColumnGeneratorFactory::create(table_name, instance);
        if (generator) {
            NullNoneRatio r;
            r.null_ratio = instance.config().null_ratio.value_or(0.0f);
            r.none_ratio = instance.config().none_ratio.value_or(0.0f);
            ratios_.push_back(r);
            if (r.has_ratio()) has_any_ratio_ = true;
            column_gens_.push_back(std::move(generator));
        } else {
            throw std::runtime_error("Failed to create generator for column: " + instance.name());
        }
    }
}

RowGenerator::RowGenerator(const ColumnConfigInstanceVector& col_instances)
    : RowGenerator("", col_instances) {
}

RowGenerator::RowGenerator(const TimestampGeneratorConfig& ts_config, const ColumnConfigInstanceVector& col_instances)
    : RowGenerator(col_instances) {
    timestamp_gen_ = std::make_unique<TimestampGenerator>(ts_config);
}

RowType RowGenerator::generate() const {
    RowType row;
    size_t col_offset = (timestamp_gen_ ? 1 : 0);
    row.reserve(column_gens_.size() + col_offset);

    if (timestamp_gen_) {
        row.push_back(timestamp_gen_->generate());
    }

    for (const auto& gen : column_gens_) {
        row.push_back(gen->generate());
    }

    if (has_any_ratio_) {
        apply_null_none(row, col_offset);
    }

    return row;
}

void RowGenerator::generate(RowType& columns) const {
    assert(columns.size() <= column_gens_.size());
    auto columns_size = columns.size();
    for (size_t i = 0; i < columns_size; ++i) {
        columns[i] = column_gens_[i]->generate();
    }
    if (has_any_ratio_) {
        apply_null_none(columns, 0);
    }
}

std::vector<RowType> RowGenerator::generate(size_t count) const {
    const bool has_timestamp = (timestamp_gen_ != nullptr);
    const size_t col_offset = has_timestamp ? 1 : 0;
    const size_t num_columns = column_gens_.size() + col_offset;

    std::vector<RowType> rows;
    rows.resize(count);

    // timestamp
    std::vector<Timestamp> timestamps;
    if (has_timestamp) {
        timestamps = timestamp_gen_->generate(count);
    }

    // data columns
    std::vector<ColumnTypeVector> columns;
    columns.reserve(column_gens_.size());
    for (const auto& gen : column_gens_) {
        columns.push_back(gen->generate(count));
    }

    for (size_t i = 0; i < count; ++i) {
        auto& row = rows[i];
        row.reserve(num_columns);

        if (has_timestamp) {
            row.emplace_back(timestamps[i]);
        }

        for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
            row.emplace_back(columns[col_idx][i]);
        }

        if (has_any_ratio_) {
            apply_null_none(row, col_offset);
        }
    }

    return rows;
}

void RowGenerator::apply_null_none(RowType& row, size_t col_offset) const {
    static thread_local std::mt19937 rng{std::random_device{}()};
    static thread_local std::uniform_real_distribution<float> dice{0.0f, 1.0f};

    for (size_t i = 0; i < ratios_.size() && (i + col_offset) < row.size(); ++i) {
        const auto& r = ratios_[i];
        if (!r.has_ratio()) continue;

        float roll = dice(rng);
        if (roll < r.null_ratio) {
            row[i + col_offset] = NullValue{};
        } else if (roll < r.null_ratio + r.none_ratio) {
            row[i + col_offset] = NoneValue{};
        }
    }
}