#pragma once
#include "LogUtils.hpp"
#include "TableNameConfig.hpp"
#include "ColumnConfig.hpp"
#include "GenerationConfig.hpp"
#include "FromCSVConfig.hpp"
#include "ColumnsConfig.hpp"
#include "TagsConfig.hpp"

struct SchemaConfig {
    bool enabled = false;
    std::string name = "meters";
    FromCSVConfig from_csv;
    TableNameConfig tbname;
    ColumnConfigVector columns;
    ColumnConfigVector tags;
    GenerationConfig generation;

    ColumnsConfig columns_cfg;
    TagsConfig tags_cfg;

    void apply() {
        if (columns.empty() || columns[0].type_tag != ColumnTypeTag::BIGINT) {
            columns.insert(columns.begin(), ColumnConfig("ts", "TIMESTAMP"));
        }

        if (from_csv.enabled && from_csv.tags.enabled && from_csv.tags.tbname_index >= 0) {
            tbname.enabled = true;
            tbname.source_type = "csv";
            tbname.csv.file_path = from_csv.tags.file_path;
            tbname.csv.has_header = from_csv.tags.has_header;
            tbname.csv.delimiter = from_csv.tags.delimiter;
            tbname.csv.tbname_index = from_csv.tags.tbname_index;
        }

        if (from_csv.tags.enabled) {
            tags_cfg.source_type = "csv";
            tags_cfg.csv = from_csv.tags;
            tags_cfg.csv.schema = tags;
        } else {
            tags_cfg.source_type = "generator";
            tags_cfg.generator.schema = tags;
        }

        if (from_csv.columns.enabled) {
            columns_cfg.source_type = "csv";
            columns_cfg.csv = from_csv.columns;
            columns_cfg.csv.schema = ColumnConfigVector(columns.begin() + 1, columns.end());

            if (from_csv.columns.timestamp_strategy.csv.enabled) {
                if(!from_csv.columns.timestamp_strategy.csv.timestamp_precision.has_value()) {
                    from_csv.columns.timestamp_strategy.csv.timestamp_precision = columns[0].ts.generator.timestamp_precision;
                    if (from_csv.columns.timestamp_strategy.csv.offset_config.has_value()) {
                        from_csv.columns.timestamp_strategy.csv.offset_config->parse_offset(from_csv.columns.timestamp_strategy.csv.timestamp_precision.value());
                    }
                }
                columns_cfg.csv.timestamp_strategy = from_csv.columns.timestamp_strategy;
            } else {
                columns_cfg.csv.timestamp_strategy = columns[0].ts;
            }
        } else {
            columns_cfg.source_type = "generator";
            columns_cfg.generator.schema = ColumnConfigVector(columns.begin() + 1, columns.end());
            columns_cfg.generator.timestamp_strategy.timestamp_config = columns[0].ts.generator;
        }

        if (generation.data_cache.enabled) {
            if (from_csv.columns.enabled) {
                if (from_csv.columns.tbname_index >= 0) {
                    LogUtils::warn("Configuration 'data_cache.enabled' is set to 'false' because 'from_csv.columns.tbname_index' in effect");
                    generation.data_cache.enabled = false;
                }

                if (generation.data_cache.enabled && !generation.tables_reuse_data) {
                    LogUtils::warn("Configuration 'data_cache.enabled' is set to 'false' because 'tables_reuse_data' is false");
                    generation.data_cache.enabled = false;
                }
            }
        }

        if (generation.tables_reuse_data) {
            if (from_csv.columns.enabled) {
                if (from_csv.columns.tbname_index >= 0) {
                    LogUtils::warn("Configuration 'tables_reuse_data' is set to 'false' because 'from_csv.columns.tbname_index' in effect");
                    generation.tables_reuse_data = false;
                }
            }
        }

        // Streaming mode validations
        if (from_csv.columns.enabled && from_csv.columns.loading_mode == "streaming") {
            if (from_csv.columns.tbname_index >= 0) {
                throw std::runtime_error("loading_mode 'streaming' is incompatible with 'tbname_index' >= 0. "
                    "Streaming mode requires tbname_index = -1.");
            }
            if (generation.data_disorder.enabled) {
                throw std::runtime_error("loading_mode 'streaming' is incompatible with 'data_disorder'. "
                    "Disorder requires buffering rows, which conflicts with streaming.");
            }
        }
    }
};