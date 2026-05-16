#pragma once

#include <string>
#include <vector>
#include "ColumnsCSV.hpp"

struct ColumnsConfig {
    std::string source_type = "generator";

    struct Generator {
        ColumnConfigVector schema; // Schema definition for normal columns

        struct TimestampStrategy {
            TimestampGeneratorConfig timestamp_config;
        } timestamp_strategy;
    } generator;

    ColumnsCSV csv;

    const ColumnConfigVector& get_schema() const {
        if (source_type == "generator") {
            return generator.schema;
        } else if (source_type == "csv") {
            return csv.schema;
        } else {
            throw std::runtime_error("Unknown source_type: " + source_type);
        }
    }
};