#pragma once

#include "GlobalConfig.hpp"
#include "ActionConfigVariant.hpp"
#include "PluginConfigRegistry.hpp"
#include <string>
#include <vector>
#include <optional>
#include <variant>
#include <yaml-cpp/yaml.h>

namespace YAML {

    template<typename T>
    std::set<T> merge_keys(const std::initializer_list<std::set<T>>& sets) {
        std::set<T> result;
        for (const auto& s : sets) {
            result.insert(s.begin(), s.end());
        }
        return result;
    }

    inline void check_unknown_keys(const YAML::Node& node, const std::set<std::string>& valid_keys, const std::string& context) {
        for (auto it = node.begin(); it != node.end(); ++it) {
            std::string key = it->first.as<std::string>();
            if (valid_keys.find(key) == valid_keys.end()) {
                throw std::runtime_error("Unknown configuration key in " + context + ": " + key);
            }
        }
    }

    inline void check_forbidden_keys(const YAML::Node& node, const std::set<std::string>& forbidden_keys, const std::string& context) {
        for (auto it = node.begin(); it != node.end(); ++it) {
            std::string key = it->first.as<std::string>();
            if (forbidden_keys.find(key) != forbidden_keys.end()) {
                throw std::runtime_error("Forbidden configuration key in " + context + ": " + key);
            }
        }
    }

    inline std::string strip_backticks_if_wrapped(const std::string& s) {
        if (s.size() >= 2 && s.front() == '`' && s.back() == '`') {
            return s.substr(1, s.size() - 2);
        }
        return s;
    }

    template<>
    struct convert<FromCSVConfig> {
        static bool decode(const Node& node, FromCSVConfig& rhs) {
            static const std::set<std::string> valid_keys = {"tags", "columns"};
            check_unknown_keys(node, valid_keys, "schema::from_csv");

            rhs.enabled = true;

            // Parse tags
            if (node["tags"]) {
                rhs.tags.enabled = true;
                const auto& tags_node = node["tags"];
                static const std::set<std::string> tags_keys = {
                    "file_path", "has_header", "delimiter", "tbname_index", "exclude_indices"
                };
                check_unknown_keys(tags_node, tags_keys, "schema::from_csv::tags");

                if (tags_node["file_path"]) {
                    rhs.tags.file_path = tags_node["file_path"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required 'file_path' configuration for schema::from_csv::tags");
                }

                if (tags_node["has_header"]) rhs.tags.has_header = tags_node["has_header"].as<bool>();
                if (tags_node["delimiter"]) rhs.tags.delimiter = tags_node["delimiter"].as<std::string>();
                if (tags_node["tbname_index"]) rhs.tags.tbname_index = tags_node["tbname_index"].as<int>();
                if (tags_node["exclude_indices"] && tags_node["exclude_indices"].IsScalar()) {
                    rhs.tags.exclude_indices_str = tags_node["exclude_indices"].as<std::string>();
                } else {
                    rhs.tags.exclude_indices_str = "";
                }
                rhs.tags.parse_exclude_indices();
            }

            // Parse columns
            if (node["columns"]) {
                rhs.columns.enabled = true;
                const auto& columns_node = node["columns"];
                static const std::set<std::string> columns_keys = {
                    "file_path", "has_header", "repeat_read", "delimiter",
                    "tbname_index", "timestamp_index", "timestamp_precision", "timestamp_offset"
                };
                check_unknown_keys(columns_node, columns_keys, "schema::from_csv::columns");

                if (columns_node["file_path"]) {
                    rhs.columns.file_path = columns_node["file_path"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required 'file_path' configuration for schema::from_csv::columns");
                }

                if (columns_node["has_header"]) rhs.columns.has_header = columns_node["has_header"].as<bool>();
                if (columns_node["repeat_read"]) rhs.columns.repeat_read = columns_node["repeat_read"].as<bool>();
                if (columns_node["delimiter"]) rhs.columns.delimiter = columns_node["delimiter"].as<std::string>();
                if (columns_node["tbname_index"]) rhs.columns.tbname_index = columns_node["tbname_index"].as<int>();
                if (columns_node["timestamp_index"]) rhs.columns.timestamp_index = columns_node["timestamp_index"].as<int>();

                if (rhs.columns.timestamp_index >= 0) {
                    rhs.columns.timestamp_strategy.strategy_type = "csv";
                    rhs.columns.timestamp_strategy.csv.enabled = true;
                    rhs.columns.timestamp_strategy.csv.timestamp_index = rhs.columns.timestamp_index;

                    if (columns_node["timestamp_precision"]) {
                        rhs.columns.timestamp_strategy.csv.timestamp_precision = columns_node["timestamp_precision"].as<std::string>();
                    }

                    if (columns_node["timestamp_offset"]) {
                        rhs.columns.timestamp_strategy.csv.offset_config = columns_node["timestamp_offset"].as<TimestampCSVConfig::OffsetConfig>();
                        if (rhs.columns.timestamp_strategy.csv.timestamp_precision.has_value()) {
                            rhs.columns.timestamp_strategy.csv.offset_config->parse_offset(rhs.columns.timestamp_strategy.csv.timestamp_precision.value());
                        }
                    }
                }
            }

            rhs.enabled = true;
            return true;
        }
    };

    template<>
    struct convert<GenerationConfig::DataDisorder::Interval> {
        static bool decode(const Node& node, GenerationConfig::DataDisorder::Interval& rhs) {
            static const std::set<std::string> keys = {"time_start", "time_end", "ratio", "latency_range"};
            check_unknown_keys(node, keys, "generation::data_disorder::intervals");
            if (node["time_start"]) {
                rhs.time_start = node["time_start"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'time_start' in data_disorder::interval.");
            }

            if (node["time_end"]) {
                rhs.time_end = node["time_end"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'time_end' in data_disorder::interval.");
            }

            if (node["ratio"]) {
                rhs.ratio = node["ratio"].as<double>();
            } else {
                throw std::runtime_error("Missing required field 'ratio' in data_disorder::interval.");
            }

            if (node["latency_range"]) {
                rhs.latency_range = node["latency_range"].as<int>();
            } else {
                throw std::runtime_error("Missing required field 'latency_range' in data_disorder::interval.");
            }

            return true;
        }
    };

    template<>
    struct convert<GenerationConfig::DataDisorder> {
        static bool decode(const Node& node, GenerationConfig::DataDisorder& rhs) {
            for (const auto& interval : node) {
                rhs.intervals.push_back(interval.as<GenerationConfig::DataDisorder::Interval>());
            }
            if (rhs.intervals.empty()) {
                rhs.enabled = false;
            } else {
                rhs.enabled = true;
            }
            return true;
        }
    };

    template<>
    struct convert<GenerationConfig> {
        static bool decode(const Node& node, GenerationConfig& rhs) {
            static const std::set<std::string> valid_keys = {
                "interlace", "num_cached_batches", "rate_limit", "data_disorder",
                "concurrency", "rows_per_table", "rows_per_batch", "per_table_rows", "per_batch_rows",
                "tables_reuse_data"
            };

            check_unknown_keys(node, valid_keys, "generation");

            // interlace
            if (node["interlace"]) {
                const auto interlace = node["interlace"].as<size_t>();
                if (interlace == 0) {
                    rhs.interlace_mode.enabled = false;
                } else {
                    rhs.interlace_mode.enabled = true;
                    rhs.interlace_mode.rows = interlace;
                }
            }

            // rate_limit
            if (node["rate_limit"]) {
                const auto rate_limit = node["rate_limit"].as<int64_t>();
                if (rate_limit <= 0) {
                    rhs.flow_control.enabled = false;
                } else {
                    rhs.flow_control.enabled = true;
                    rhs.flow_control.rate_limit = rate_limit;
                }
            }

            // data_disorder
            if (node["data_disorder"]) {
                rhs.data_disorder = node["data_disorder"].as<GenerationConfig::DataDisorder>();
            }

            // concurrency
            if (node["concurrency"]) {
                int64_t concurrency = node["concurrency"].as<int64_t>();
                if (concurrency <= 0) {
                    throw std::runtime_error("concurrency must be a positive integer.");
                }
                rhs.generate_threads = concurrency;
            } else {
                rhs.generate_threads = std::nullopt;
            }

            // rows_per_table
            std::optional<int64_t> rows_per_table;
            if (node["rows_per_table"]) {
                rows_per_table = node["rows_per_table"].as<int64_t>();
            } else if (node["per_table_rows"]) {
                LogUtils::warn("'per_table_rows' is deprecated and will be removed in future versions. Please use 'rows_per_table' instead.");
                rows_per_table = node["per_table_rows"].as<int64_t>();
            }
            if (rows_per_table.has_value()) {
                int64_t val = rows_per_table.value();
                if (val == -1) {
                    rhs.rows_per_table = std::numeric_limits<int64_t>::max();
                } else if (val <= 0) {
                    throw std::runtime_error("rows_per_table must be positive or -1 (for unlimited).");
                } else {
                    rhs.rows_per_table = val;
                }
            }

            // rows_per_batch
            std::optional<int64_t> rows_per_batch;
            if (node["rows_per_batch"]) {
                rows_per_batch = node["rows_per_batch"].as<int64_t>();
            } else if (node["per_batch_rows"]) {
                LogUtils::warn("'per_batch_rows' is deprecated and will be removed in future versions. Please use 'rows_per_batch' instead.");
                rows_per_batch = node["per_batch_rows"].as<int64_t>();
            }
            if (rows_per_batch.has_value()) {
                int64_t val = rows_per_batch.value();
                if (val <= 0) {
                    throw std::runtime_error("rows_per_batch must be positive.");
                } else {
                    rhs.rows_per_batch = val;
                }
            }

            if (rhs.interlace_mode.enabled) {
                if (rhs.interlace_mode.rows > rhs.rows_per_batch) {
                    throw std::runtime_error("interlace rows cannot be greater than rows_per_batch.");
                }
            }

            // tables_reuse_data
            if (node["tables_reuse_data"]) {
                rhs.tables_reuse_data = node["tables_reuse_data"].as<bool>();
            }

            // num_cached_batches
            bool data_cache_specified = false;
            if (node["num_cached_batches"]) {
                data_cache_specified = true;
                int64_t val = node["num_cached_batches"].as<int64_t>();
                if (val < 0) {
                    throw std::runtime_error("num_cached_batches must be non-negative.");
                } else if (val == 0) {
                    rhs.data_cache.enabled = false;
                } else {
                    rhs.data_cache.enabled = true;
                    rhs.data_cache.num_cached_batches = val;
                }
            }

            if (rhs.data_cache.enabled) {
                size_t batches_needed = 0;
                if (rhs.interlace_mode.enabled) {
                    batches_needed = (static_cast<size_t>(rhs.rows_per_table) + rhs.interlace_mode.rows - 1) / rhs.interlace_mode.rows;
                } else {
                    batches_needed = (static_cast<size_t>(rhs.rows_per_table) + rhs.rows_per_batch - 1) / rhs.rows_per_batch;
                }

                if (!data_cache_specified) {
                    rhs.data_cache.num_cached_batches = batches_needed;
                } else {
                    if (rhs.data_cache.num_cached_batches > batches_needed) {
                        throw std::runtime_error("num_cached_batches cannot be greater than the number of batches needed to fill a table.");
                    }
                }
            }

            return true;
        }
    };

    template<>
    struct convert<SchemaConfig> {
        static bool decode(const Node& node, SchemaConfig& rhs) {
            static const std::set<std::string> valid_keys = {
                "name", "from_csv", "tbname", "columns", "tags", "generation"
            };
            check_unknown_keys(node, valid_keys, "schema");

            // if (!node["name"]) {
            //     throw std::runtime_error("Missing required field 'name' in schema.");
            // }
            if (node["name"]) {
                rhs.name = node["name"].as<std::string>();
            }

            if (node["from_csv"]) {
                rhs.from_csv = node["from_csv"].as<FromCSVConfig>();
            }

            // if (!node["tbname"]) {
            //     throw std::runtime_error("Missing required field 'tbname' in schema.");
            // }
            if (node["tbname"]) {
                rhs.tbname = node["tbname"].as<TableNameConfig>();
            }

            // if (!node["columns"]) {
            //     throw std::runtime_error("Missing required field 'columns' in schema.");
            // }
            if (node["columns"]) {
                rhs.columns = node["columns"].as<ColumnConfigVector>();
                if (rhs.columns.size() == 0) {
                    throw std::runtime_error("Schema must have at least one column defined.");
                }

                if (rhs.columns[0].type_tag != ColumnTypeTag::BIGINT) {
                    // throw std::runtime_error("The first column must be of type BIGINT or TIMESTAMP to serve as the timestamp column.");
                    rhs.columns.insert(rhs.columns.begin(), ColumnConfig("ts", "TIMESTAMP"));
                }
            } else {
                rhs.columns = {
                    ColumnConfig("ts", "TIMESTAMP", "ms", "1735660800000", "1"),
                    ColumnConfig("current", "FLOAT", "random", 0, 100),
                    ColumnConfig("voltage", "INT", "random", 200, 240),
                    ColumnConfig("phase", "FLOAT", ExpressionTag(), "_i * math.pi % 180")
                };
            }

            // if (!node["tags"]) {
            //     throw std::runtime_error("Missing required field 'tags' in schema.");
            // }
            if (node["tags"]) {
                rhs.tags = node["tags"].as<ColumnConfigVector>();
            } else {
                rhs.tags = {
                    ColumnConfig("groupid", "INT", "random", 1, 10),
                    ColumnConfig("location", "BINARY(24)", std::vector<std::string>{
                            "New York",
                            "Los Angeles",
                            "Chicago",
                            "Houston",
                            "Phoenix",
                            "Philadelphia",
                            "San Antonio",
                            "San Diego",
                            "Dallas",
                            "Austin"
                        }
                    )
                };
            }

            if (node["generation"]) {
                rhs.generation = node["generation"].as<GenerationConfig>();
            }
            rhs.apply();
            rhs.enabled = true;
            return true;
        }
    };

    template<>
    struct convert<DatabaseInfo> {
        static bool decode(const Node& node, DatabaseInfo& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"name", "drop_if_exists", "precision", "props"};
            check_unknown_keys(node, valid_keys, "database_info");

            if (!node["name"]) {
                throw std::runtime_error("Missing required field 'name' in database_info.");
            }
            rhs.name = node["name"].as<std::string>();
            if (node["drop_if_exists"]) {
                rhs.drop_if_exists = node["drop_if_exists"].as<bool>();
            }
            if (node["precision"]) {
                rhs.precision = node["precision"].as<std::string>();
                // Validate time precision value
                if (rhs.precision != "ms" && rhs.precision != "us" && rhs.precision != "ns") {
                    throw std::runtime_error("Invalid precision value: " + rhs.precision);
                }
            }
            if (node["props"]) {
                rhs.properties = node["props"].as<std::string>();
            }

            return true;
        }
    };

    template<>
    struct convert<CheckpointInfo> {
        static bool decode(const Node& node, CheckpointInfo& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "enabled", "interval_sec"
            };
            check_unknown_keys(node, valid_keys, "checkpoint_info");

            if (node["enabled"]) {
                rhs.enabled = node["enabled"].as<bool>();
            }
            if (rhs.enabled) {
                if (node["interval_sec"]) {
                    rhs.interval_sec = node["interval_sec"].as<size_t>();
                    if (rhs.interval_sec == 0) {
                        throw std::runtime_error("interval_sec must be greater than 0 in checkpoint_info.");
                    }
                }
            }

            return true;
        }
    };

    template<>
    struct convert<ColumnConfig> {
        static bool decode(const Node& node, ColumnConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> common_keys = {
                "name", "type", "primary_key", "count", "gen_type", "props", "null_ratio", "none_ratio"
            };
            static const std::set<std::string> random_allowed = {
                "distribution", "min", "max", "dec_min", "dec_max", "corpus", "chinese", "values"
            };
            static const std::set<std::string> order_allowed = {
                "min", "max"
            };
            static const std::set<std::string> expression_allowed = {
                "expr"
            };
            static const std::set<std::string> timestamp_allowed = {
                "precision", "start", "step"
            };

            if (!node["name"]) {
                throw std::runtime_error("Missing required field 'name' for column or tag.");
            }
            if (!node["type"]) {
                throw std::runtime_error("Missing required field 'type' for column or tag.");
            }

            rhs.name = strip_backticks_if_wrapped(node["name"].as<std::string>());
            rhs.type = node["type"].as<std::string>();
            rhs.parse_type();

            // Valid keys
            std::set<std::string> valid_keys = common_keys;
            if (rhs.type_tag == ColumnTypeTag::BIGINT) {
                valid_keys = merge_keys<std::string>({common_keys, random_allowed, order_allowed, expression_allowed, timestamp_allowed});
            } else {
                valid_keys = merge_keys<std::string>({common_keys, random_allowed, order_allowed, expression_allowed});
            }
            check_unknown_keys(node, valid_keys, "columns or tags");

            if (rhs.type_tag == ColumnTypeTag::BIGINT) {
                // Parse timestamp config
                if (node["precision"]) {
                    rhs.ts.generator.timestamp_precision = node["precision"].as<std::string>();
                    rhs.ts.csv.timestamp_precision = rhs.ts.generator.timestamp_precision;
                }
                if (node["start"]) {
                    rhs.ts.strategy_type = "generator";
                    rhs.ts.generator.start_timestamp = node["start"].as<std::string>();
                    (void)TimestampUtils::parse_timestamp(rhs.ts.generator.start_timestamp, rhs.ts.generator.timestamp_precision);
                }
                if (node["step"]) {
                    rhs.ts.strategy_type = "generator";
                    rhs.ts.generator.timestamp_step = node["step"].as<std::string>();
                    rhs.ts.generator.timestamp_step = TimestampUtils::parse_step(rhs.ts.generator.timestamp_step, rhs.ts.generator.timestamp_precision);
                }
            }

            if (node["primary_key"]) rhs.primary_key = node["primary_key"].as<bool>();
            // if (node["len"]) rhs.len = node["len"].as<int>();
            if (node["count"]) rhs.count = node["count"].as<size_t>();
            // if (node["precision"]) rhs.precision = node["precision"].as<int>();
            // if (node["scale"]) rhs.scale = node["scale"].as<int>();
            if (node["props"]) rhs.properties = node["props"].as<std::string>();
            if (node["null_ratio"]) rhs.null_ratio = node["null_ratio"].as<float>();
            if (node["none_ratio"]) rhs.none_ratio = node["none_ratio"].as<float>();

            // Inference generation method
            if (node["gen_type"]) {
                rhs.gen_type = node["gen_type"].as<std::string>();
            } else {
                bool has_random_key = false;
                bool has_expr_key = false;

                for (const auto& key : random_allowed) {
                    if (node[key]) has_random_key = true;
                }
                for (const auto& key : expression_allowed) {
                    if (node[key]) has_expr_key = true;
                }
                int mode_count = (has_random_key ? 1 : 0) + (has_expr_key ? 1 : 0);
                if (mode_count > 1) {
                    throw std::runtime_error("ColumnConfig: random/expression keys cannot appear at the same time for column: " + rhs.name);
                }
                if (mode_count == 0) {
                    rhs.gen_type = "random";
                } else if (has_random_key) {
                    rhs.gen_type = "random";
                } else if (has_expr_key) {
                    rhs.gen_type = "expression";
                }
            }

            if (*rhs.gen_type == "random") {
                // Detect forbidden keys in random
                check_unknown_keys(node, merge_keys<std::string>({common_keys, timestamp_allowed, random_allowed}), "columns or tags::random");

                if (node["distribution"]) {
                    rhs.distribution = node["distribution"].as<std::string>();
                } else {
                    rhs.distribution = "uniform";
                }
                if (node["min"]) {
                    rhs.min = node["min"].as<double>();
                } else {
                    rhs.min = rhs.get_min_value();
                }
                if (node["max"]) {
                    rhs.max = node["max"].as<double>();
                } else {
                    rhs.max = rhs.get_max_value();
                }
                if (node["dec_min"]) rhs.dec_min = node["dec_min"].as<std::string>();
                if (node["dec_max"]) rhs.dec_max = node["dec_max"].as<std::string>();
                if (node["corpus"]) rhs.corpus = node["corpus"].as<std::string>();
                if (node["chinese"]) rhs.chinese = node["chinese"].as<bool>();
                if (node["values"]) {
                    if (rhs.type_tag == ColumnTypeTag::BOOL) {
                        auto str_values = node["values"].as<std::vector<std::string>>();
                        rhs.set_values_from_strings(str_values);
                    } else if (rhs.is_var_length()) {
                        auto str_values = node["values"].as<std::vector<std::string>>();
                        rhs.set_values_from_strings(str_values);
                    } else {
                        auto dbl_values = node["values"].as<std::vector<double>>();
                        rhs.set_values_from_doubles(dbl_values);
                    }

                    if (rhs.values_count < 1) {
                        throw std::runtime_error("values must contain at least one element for column: " + rhs.name);
                    }
                }
            } else if (*rhs.gen_type == "order") {
                // Detect forbidden keys in order
                check_unknown_keys(node, merge_keys<std::string>({common_keys, timestamp_allowed, order_allowed}), "columns or tags::order");

                if (node["min"]) rhs.order_min = node["min"].as<int64_t>();
                if (node["max"]) rhs.order_max = node["max"].as<int64_t>();

                if (rhs.order_min && rhs.order_max) {
                    if (*rhs.order_min >= *rhs.order_max) {
                        throw std::runtime_error("min value must be less than max value in column: " + rhs.name);
                    }
                }
            } else if (*rhs.gen_type == "expression") {
                // Detect forbidden keys in expression
                check_unknown_keys(node, merge_keys<std::string>({common_keys, timestamp_allowed, expression_allowed}), "columns or tags::expression");

                if (node["expr"]) {
                    rhs.formula = node["expr"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required 'expr' for expression type column: " + rhs.name);
                }
            } else {
                throw std::runtime_error("Invalid gen_type: " + *rhs.gen_type);
            }
            return true;
        }
    };
    template<>
    struct convert<TableNameConfig> {
        static bool decode(const Node& node, TableNameConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> generator_keys = {"prefix", "count", "from"};
            static const std::set<std::string> csv_keys = {"file_path", "has_header", "delimiter", "tbname_index"};
            static const std::set<std::string> valid_keys = merge_keys<std::string>({
                generator_keys, csv_keys
            });
            check_unknown_keys(node, valid_keys, "table_name");

            bool has_generator_key = false;
            bool has_csv_key = false;
            for (const auto& key : generator_keys) {
                if (node[key]) has_generator_key = true;
            }
            for (const auto& key : csv_keys) {
                if (node[key]) has_csv_key = true;
            }
            if (has_generator_key && has_csv_key) {
                throw std::runtime_error("table_name: generator keys and csv keys cannot appear at the same time.");
            }
            if (!has_generator_key && !has_csv_key) {
                throw std::runtime_error("table_name: must specify at least one of generator or csv keys.");
            }

            if (has_generator_key) {
                rhs.source_type = "generator";
                if (node["prefix"]) rhs.generator.prefix = node["prefix"].as<std::string>();
                if (node["count"]) rhs.generator.count = node["count"].as<int>();
                if (node["from"]) rhs.generator.from = node["from"].as<int>();
            } else {
                rhs.source_type = "csv";

                if (node["file_path"]) {
                    rhs.csv.file_path = node["file_path"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required 'file_path' configuration for table_name::csv.");
                }

                if (node["has_header"]) rhs.csv.has_header = node["has_header"].as<bool>();
                if (node["delimiter"]) rhs.csv.delimiter = node["delimiter"].as<std::string>();
                if (node["tbname_index"]) rhs.csv.tbname_index = node["tbname_index"].as<int>();
            }

            rhs.enabled = true;
            return true;
        }
    };


    template<>
    struct convert<TagsConfig> {
        static bool decode(const Node& node, TagsConfig& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "source_type", "generator", "csv"
            };
            check_unknown_keys(node, valid_keys, "tags");

            if (!node["source_type"]) {
                throw std::runtime_error("Missing required field 'source_type' in tags.");
            }
            rhs.source_type = node["source_type"].as<std::string>();
            if (rhs.source_type == "generator") {
                if (!node["generator"]) {
                    throw std::runtime_error("Missing required 'generator' configuration for source_type 'generator' in tags.");
                }

                const auto& generator = node["generator"];

                // Detect unknown keys in generator
                static const std::set<std::string> generator_keys = {"schema"};
                check_unknown_keys(generator, generator_keys, "tags::generator");

                if (generator["schema"]) {
                    rhs.generator.schema = generator["schema"].as<ColumnConfigVector>();
                }
            } else if (rhs.source_type == "csv") {
                if (!node["csv"]) {
                    throw std::runtime_error("Missing required 'csv' configuration for source_type 'csv' in tags.");
                }

                const auto& csv = node["csv"];

                // Detect unknown keys in csv
                static const std::set<std::string> csv_keys = {"schema", "file_path", "has_header", "delimiter", "exclude_indices"};
                check_unknown_keys(csv, csv_keys, "tags::csv");

                if (csv["schema"]) {
                    rhs.csv.schema = csv["schema"].as<ColumnConfigVector>();
                }

                if (csv["file_path"]) {
                    rhs.csv.file_path = csv["file_path"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required 'file_path' configuration for tags::csv");
                }

                if (csv["has_header"]) rhs.csv.has_header = csv["has_header"].as<bool>();
                if (csv["delimiter"]) rhs.csv.delimiter = csv["delimiter"].as<std::string>();
                if (csv["exclude_indices"]) {
                    rhs.csv.exclude_indices_str = csv["exclude_indices"].as<std::string>();
                    rhs.csv.parse_exclude_indices();
                }
            } else {
                throw std::runtime_error("Invalid source_type for tags in child_table_info.");
            }
            return true;
        }
    };


    template<>
    struct convert<SuperTableInfo> {
        static bool decode(const Node& node, SuperTableInfo& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"name", "columns", "tags"};
            check_unknown_keys(node, valid_keys, "super_table_info");

            if (!node["name"]) {
                throw std::runtime_error("Missing required field 'name' in super_table_info.");
            }
            rhs.name = node["name"].as<std::string>();
            if (node["columns"]) {
                for (const auto& item : node["columns"]) {
                    rhs.columns.push_back(item.as<ColumnConfig>());
                }
            }
            if (node["tags"]) {
                for (const auto& item : node["tags"]) {
                    rhs.tags.push_back(item.as<ColumnConfig>());
                }
            }
            return true;
        }
    };


    template<>
    struct convert<ChildTableInfo> {
        static bool decode(const Node& node, ChildTableInfo& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"table_name", "tags"};
            check_unknown_keys(node, valid_keys, "child_table_info");

            if (!node["table_name"]) {
                throw std::runtime_error("Missing required field 'table_name' in child_table_info.");
            }
            if (!node["tags"]) {
                throw std::runtime_error("Missing required field 'tags' in child_table_info.");
            }

            rhs.table_name = node["table_name"].as<TableNameConfig>();
            rhs.tags = node["tags"].as<TagsConfig>();

            return true;
        }
    };


    template<>
    struct convert<CreateChildTableConfig::BatchConfig> {
        static bool decode(const Node& node, CreateChildTableConfig::BatchConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"size", "concurrency"};
            check_unknown_keys(node, valid_keys, "batch");

            if (node["size"]) {
                rhs.size = node["size"].as<int>();
            }
            if (node["concurrency"]) {
                rhs.concurrency = node["concurrency"].as<int>();
            }
            return true;
        }
    };


    template<>
    struct convert<TimestampGeneratorConfig> {
        static bool decode(const Node& node, TimestampGeneratorConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "start_timestamp", "timestamp_precision", "timestamp_step"
            };
            check_unknown_keys(node, valid_keys, "timestamp_strategy");

            if (node["start_timestamp"]) {
                rhs.start_timestamp = node["start_timestamp"].as<std::string>();
            }
            if (node["timestamp_precision"]) {
                rhs.timestamp_precision = node["timestamp_precision"].as<std::string>();
            }
            if (node["timestamp_step"]) {
                rhs.timestamp_step = node["timestamp_step"].as<std::string>();
                rhs.timestamp_step = TimestampUtils::parse_step(rhs.timestamp_step, rhs.timestamp_precision);
            }
            return true;
        }
    };


    template<>
    struct convert<TimestampCSVConfig::OffsetConfig> {
        static bool decode(const Node& node, TimestampCSVConfig::OffsetConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"offset_type", "value"};
            check_unknown_keys(node, valid_keys, "timestamp_strategy::csv::offset_config");

            if (!node["offset_type"]) {
                throw std::runtime_error("Missing required field 'offset_type' in offset_config");
            }
            std::string offset_type = node["offset_type"].as<std::string>();
            rhs.offset_type = offset_type;

            if (!node["value"]) {
                throw std::runtime_error("Missing required field 'value' in offset_config");
            }

            std::variant<int64_t, std::string> value;
            if (offset_type == "relative") {
                value = node["value"].as<std::string>();
            } else if (offset_type == "absolute") {
                try {
                    value = node["value"].as<std::string>();
                } catch (const YAML::BadConversion&) {
                    value = node["value"].as<int64_t>();
                }
            } else {
                throw std::runtime_error("Invalid offset_type: " + offset_type);
            }
            rhs.value = value;

            return true;
        }
    };


    template<>
    struct convert<TimestampCSVConfig> {
        static bool decode(const Node& node, TimestampCSVConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "column_index", "precision", "offset_config"
            };
            check_unknown_keys(node, valid_keys, "timestamp_strategy::csv");

            if (node["column_index"]) {
                rhs.timestamp_index = node["column_index"].as<size_t>();
            }

            if (node["precision"]) {
                rhs.timestamp_precision = node["precision"].as<std::string>();
                // Validate precision
                if (rhs.timestamp_precision != "s" &&
                    rhs.timestamp_precision != "ms" &&
                    rhs.timestamp_precision != "us" &&
                    rhs.timestamp_precision != "ns") {
                    throw std::runtime_error("Invalid timestamp precision: " + rhs.timestamp_precision.value());
                }
            }

            if (node["offset_config"]) {
                rhs.offset_config = node["offset_config"].as<TimestampCSVConfig::OffsetConfig>();
                if (rhs.offset_config) {
                    rhs.offset_config->parse_offset(rhs.timestamp_precision.value());
                }
            }

            return true;
        }
    };


    template<>
    struct convert<ColumnsConfig> {
        static bool decode(const Node& node, ColumnsConfig& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {"source_type", "generator", "csv"};
            check_unknown_keys(node, valid_keys, "columns");

            if (!node["source_type"]) {
                throw std::runtime_error("Missing required field 'source_type' in columns.");
            }
            rhs.source_type = node["source_type"].as<std::string>();

            if (rhs.source_type == "generator") {
                if (!node["generator"]) {
                    throw std::runtime_error("Missing required 'generator' configuration for source_type 'generator' in columns.");
                }

                const auto& generator = node["generator"];

                // Detect unknown keys in generator
                static const std::set<std::string> generator_keys = {"schema", "timestamp_strategy"};
                check_unknown_keys(generator, generator_keys, "columns::generator");

                if (generator["schema"]) {
                    rhs.generator.schema = generator["schema"].as<ColumnConfigVector>();
                } else {
                    throw std::runtime_error("Missing required field 'schema' in columns::generator.");
                }

                if (generator["timestamp_strategy"]) {
                    const auto& ts = generator["timestamp_strategy"];

                    // Detect unknown keys in timestamp_strategy
                    static const std::set<std::string> ts_keys = {"generator"};
                    check_unknown_keys(ts, ts_keys, "columns::generator::timestamp_strategy");

                    if (!ts["generator"]) {
                        throw std::runtime_error("Missing required field 'generator' in columns::generator::timestamp_strategy");
                    }
                    rhs.generator.timestamp_strategy.timestamp_config = ts["generator"].as<TimestampGeneratorConfig>();
                }
            } else if (rhs.source_type == "csv") {
                if (!node["csv"]) {
                    throw std::runtime_error("Missing required 'csv' configuration for source_type 'csv'.");
                }

                const auto& csv = node["csv"];

                // Detect unknown keys in csv
                static const std::set<std::string> csv_keys = {
                    "schema", "file_path", "has_header", "repeat_read", "delimiter", "tbname_index", "timestamp_strategy"
                };
                check_unknown_keys(csv, csv_keys, "columns::csv");

                if (csv["schema"]) {
                    rhs.csv.schema = csv["schema"].as<ColumnConfigVector>();
                } else {
                    throw std::runtime_error("Missing required 'schema' configuration for columns::csv");
                }

                if (csv["file_path"]) {
                    rhs.csv.file_path = csv["file_path"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required 'file_path' configuration for columns::csv");
                }

                if (csv["has_header"]) {
                    rhs.csv.has_header = csv["has_header"].as<bool>();
                }
                if (csv["repeat_read"]) {
                    rhs.csv.repeat_read = csv["repeat_read"].as<bool>();
                }
                if (csv["delimiter"]) {
                    rhs.csv.delimiter = csv["delimiter"].as<std::string>();
                }
                if (csv["tbname_index"]) {
                    rhs.csv.tbname_index = csv["tbname_index"].as<int>();
                }
                if (csv["timestamp_strategy"]) {
                    const auto& ts = csv["timestamp_strategy"];

                    // Detect unknown keys in timestamp_strategy
                    static const std::set<std::string> ts_keys = {"strategy_type", "csv", "generator"};
                    check_unknown_keys(ts, ts_keys, "columns::csv::timestamp_strategy");

                    if (!ts["strategy_type"]) {
                        throw std::runtime_error("Missing required field 'strategy_type' in columns::csv::timestamp_strategy");
                    }

                    rhs.csv.timestamp_strategy.strategy_type = ts["strategy_type"].as<std::string>();

                    if (rhs.csv.timestamp_strategy.strategy_type == "csv") {
                        if (!ts["csv"]) {
                            throw std::runtime_error("Missing required field 'csv' in columns::csv::timestamp_strategy");
                        }
                        rhs.csv.timestamp_strategy.csv = ts["csv"].as<TimestampCSVConfig>();
                    } else if (rhs.csv.timestamp_strategy.strategy_type == "generator") {
                        if (!ts["generator"]) {
                            throw std::runtime_error("Missing required field 'generator' in columns::csv::timestamp_strategy");
                        }
                        rhs.csv.timestamp_strategy.generator = ts["generator"].as<TimestampGeneratorConfig>();
                    } else {
                        throw std::runtime_error("Invalid strategy_type in columns::csv::timestamp_strategy: " +
                            rhs.csv.timestamp_strategy.strategy_type);
                    }
                }
            } else {
                throw std::runtime_error("Invalid 'source_type' in columns: " + rhs.source_type);
            }

            return true;
        }
    };

    template<>
    struct convert<DataChannel> {
        static bool decode(const Node& node, DataChannel& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"channel_type"};
            check_unknown_keys(node, valid_keys, "data_channel");

            if (node["channel_type"]) {
                rhs.channel_type = node["channel_type"].as<std::string>();
            }
            return true;
        }
    };

    template<>
    struct convert<InsertDataConfig::FailureHandling> {
        static bool decode(const YAML::Node& node, InsertDataConfig::FailureHandling& rhs) {
            static const std::set<std::string> valid_keys = {
                "max_retries", "retry_interval_ms", "on_failure"
            };
            check_unknown_keys(node, valid_keys, "insert-data::failure_handling");

            if (node["max_retries"]) {
                rhs.max_retries = node["max_retries"].as<size_t>();
            }
            if (node["retry_interval_ms"]) {
                rhs.retry_interval_ms = node["retry_interval_ms"].as<int>();
            }
            if (node["on_failure"]) {
                rhs.on_failure = node["on_failure"].as<std::string>();
                if (rhs.on_failure != "exit" && rhs.on_failure != "skip") {
                    throw std::runtime_error("Invalid value for on_failure in failure_handling: " + rhs.on_failure);
                }
            }
            return true;
        }
    };

    template<>
    struct convert<InsertDataConfig::TimeInterval> {
        static bool decode(const Node& node, InsertDataConfig::TimeInterval& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "enabled", "interval_strategy", "wait_strategy", "fixed_interval", "dynamic_interval"
            };
            check_unknown_keys(node, valid_keys, "insert-data::control::time_interval");

            if (node["enabled"]) {
                rhs.enabled = node["enabled"].as<bool>();
            }

            if (rhs.enabled == true) {
                if (node["interval_strategy"]) {
                    rhs.interval_strategy = node["interval_strategy"].as<std::string>();
                }
                if (node["wait_strategy"]) {
                    rhs.wait_strategy = node["wait_strategy"].as<std::string>();
                }
                if (rhs.interval_strategy == "fixed") {
                    // if (!node["fixed_interval"]) {
                    //     throw std::runtime_error("Missing required field 'fixed_interval' in insert-data::control::time_interval.");
                    // }

                    if (node["fixed_interval"]) {
                        const auto& fixed = node["fixed_interval"];

                        // Detect unknown keys in fixed_interval
                        static const std::set<std::string> fixed_keys = {"base_interval", "random_deviation"};
                        check_unknown_keys(fixed, fixed_keys, "insert-data::control::time_interval::fixed_interval");

                        if (fixed["base_interval"]) {
                            rhs.fixed_interval.base_interval = fixed["base_interval"].as<int>();
                        }
                        if (fixed["random_deviation"]) {
                            rhs.fixed_interval.random_deviation = fixed["random_deviation"].as<int>();
                        }
                    }
                } else if (rhs.interval_strategy == "first_to_first" || rhs.interval_strategy == "last_to_first") {
                    // if (!node["dynamic_interval"]) {
                    //     throw std::runtime_error("Missing required field 'dynamic_interval' in insert-data::control::time_interval.");
                    // }

                    if (node["dynamic_interval"]) {
                        const auto& dynamic = node["dynamic_interval"];

                        // Detect unknown keys in dynamic_interval
                        static const std::set<std::string> dynamic_keys = {"min_interval", "max_interval"};
                        check_unknown_keys(dynamic, dynamic_keys, "insert-data::control::time_interval::dynamic_interval");

                        if (dynamic["min_interval"]) {
                            rhs.dynamic_interval.min_interval = dynamic["min_interval"].as<int>();
                        }
                        if (dynamic["max_interval"]) {
                            rhs.dynamic_interval.max_interval = dynamic["max_interval"].as<int>();
                        }
                    }
                }
            }
            return true;
        }
    };

    template<>
    struct convert<QueryDataConfig::Source> {
        static bool decode(const Node& node, QueryDataConfig::Source& rhs) {
            (void)rhs;
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"connection_info"};
            check_unknown_keys(node, valid_keys, "query-data::source");

            // if (node["connection_info"]) {
            //     rhs.connection_info = node["connection_info"].as<TDengineConfig>();
            // } else {
            //     throw std::runtime_error("Missing required field 'connection_info' in query-data::source.");
            // }
            return true;
        }
    };


    template<>
    struct convert<QueryDataConfig::Control::QueryControl::Execution> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl::Execution& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"mode", "threads", "times", "interval"};
            check_unknown_keys(node, valid_keys, "query-data::control::query_control::execution");

            if (node["mode"]) {
                rhs.mode = node["mode"].as<std::string>();
            }
            if (node["threads"]) {
                rhs.threads = node["threads"].as<int>();
            }
            if (node["times"]) {
                rhs.times = node["times"].as<int>();
            }
            if (node["interval"]) {
                rhs.interval = node["interval"].as<int>();
            }
            return true;
        }
    };


    template<>
    struct convert<QueryDataConfig::Control::QueryControl::Fixed> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl::Fixed& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"queries"};
            check_unknown_keys(node, valid_keys, "query-data::control::query_control::fixed");

            if (node["queries"]) {
                for (const auto& query_node : node["queries"]) {
                    // Detect unknown keys in each query
                    static const std::set<std::string> query_keys = {"sql", "output_file"};
                    check_unknown_keys(query_node, query_keys, "query-data::control::query_control::fixed::queries");

                    QueryDataConfig::Control::QueryControl::FixedQuery query;

                    if (query_node["sql"]) {
                        query.sql = query_node["sql"].as<std::string>();
                    } else {
                        throw std::runtime_error("Missing required field 'sql' in query-data::control::query_control::fixed::queries.");
                    }
                    if (query_node["output_file"]) {
                        query.output_file = query_node["output_file"].as<std::string>();
                    } else {
                        throw std::runtime_error("Missing required field 'output_file' in query-data::control::query_control::fixed::queries.");
                    }

                    rhs.queries.push_back(query);
                }
            }
            return true;
        }
    };


    template<>
    struct convert<QueryDataConfig::Control::QueryControl::SuperTable> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl::SuperTable& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "database_name", "super_table_name", "placeholder", "templates"
            };
            check_unknown_keys(node, valid_keys, "query-data::control::query_control::super_table");

            if (node["database_name"]) {
                rhs.database_name = node["database_name"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'database_name' in query-data::control::query_control::super_table.");
            }

            if (node["super_table_name"]) {
                rhs.super_table_name = node["super_table_name"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'super_table_name' in query-data::control::query_control::super_table.");
            }

            if (node["placeholder"]) {
                rhs.placeholder = node["placeholder"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'placeholder' in query-data::control::query_control::super_table.");
            }

            if (node["templates"]) {
                for (const auto& template_node : node["templates"]) {
                    // Detect unknown keys in each template
                    static const std::set<std::string> template_keys = {"sql_template", "output_file"};
                    check_unknown_keys(template_node, template_keys, "query-data::control::query_control::super_table::templates");

                    QueryDataConfig::Control::QueryControl::SuperTableQueryTemplate query_template;

                    if (template_node["sql_template"]) {
                        query_template.sql_template = template_node["sql_template"].as<std::string>();
                    } else {
                        throw std::runtime_error("Missing required field 'sql_template' in query-data::control::query_control::super_table::templates.");
                    }

                    if (template_node["output_file"]) {
                        query_template.output_file = template_node["output_file"].as<std::string>();
                    } else {
                        throw std::runtime_error("Missing required field 'output_file' in query-data::control::query_control::super_table::templates.");
                    }

                    rhs.templates.push_back(query_template);
                }
            }
            return true;
        }
    };

    template<>
    struct convert<QueryDataConfig::Control::QueryControl> {
        static bool decode(const Node& node, QueryDataConfig::Control::QueryControl& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "log_path", "enable_dryrun", "execution", "query_type", "fixed", "super_table"
            };
            check_unknown_keys(node, valid_keys, "query-data::control::query_control");

            if (node["log_path"]) {
                rhs.log_path = node["log_path"].as<std::string>();
            }
            if (node["enable_dryrun"]) {
                rhs.enable_dryrun = node["enable_dryrun"].as<bool>();
            }
            if (node["execution"]) {
                rhs.execution = node["execution"].as<QueryDataConfig::Control::QueryControl::Execution>();
            }
            if (node["query_type"]) {
                rhs.query_type = node["query_type"].as<std::string>();
                if (rhs.query_type == "fixed") {
                    if (!node["fixed"]) {
                        throw std::runtime_error("Missing required field 'fixed' in query_control.");
                    }
                    rhs.fixed = node["fixed"].as<QueryDataConfig::Control::QueryControl::Fixed>();
                } else if (rhs.query_type == "super_table") {
                    if (!node["super_table"]) {
                        throw std::runtime_error("Missing required fields 'super_table' in query_control.");
                    }
                    rhs.super_table = node["super_table"].as<QueryDataConfig::Control::QueryControl::SuperTable>();
                } else {
                    throw std::runtime_error("Invalid or missing 'query_type' in query_control.");
                }
            }
            return true;
        }
    };

    template<>
    struct convert<QueryDataConfig::Control> {
        static bool decode(const Node& node, QueryDataConfig::Control& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "data_format", "data_channel", "query_control"
            };
            check_unknown_keys(node, valid_keys, "query-data::control");

            if (node["data_format"]) {
                // rhs.data_format = node["data_format"].as<DataFormat>();
            } else {
                throw std::runtime_error("Missing required field 'data_format' in query-data::control.");
            }

            if (node["data_channel"]) {
                rhs.data_channel = node["data_channel"].as<DataChannel>();
            } else {
                throw std::runtime_error("Missing required field 'data_channel' in query-data::control.");
            }

            if (node["query_control"]) {
                rhs.query_control = node["query_control"].as<QueryDataConfig::Control::QueryControl>();
            } else {
                throw std::runtime_error("Missing required field 'query_control' in query-data::control.");
            }

            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Source> {
        static bool decode(const Node& node, SubscribeDataConfig::Source& rhs) {
            (void)rhs;
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"connection_info"};
            check_unknown_keys(node, valid_keys, "subscribe-data::source");

            // if (node["connection_info"]) {
            //     rhs.connection_info = node["connection_info"].as<TDengineConfig>();
            // } else {
            //     throw std::runtime_error("Missing required field 'connection_info' in subscribe-data::source.");
            // }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Execution> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Execution& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"consumer_concurrency", "poll_timeout"};
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control::execution");

            if (node["consumer_concurrency"]) {
                rhs.consumer_concurrency = node["consumer_concurrency"].as<int>();
            }
            if (node["poll_timeout"]) {
                rhs.poll_timeout = node["poll_timeout"].as<int>();
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Topic> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Topic& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"name", "sql"};
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control::topic");

            if (node["name"]) {
                rhs.name = node["name"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'name' in subscribe-data::control::subscribe_control::topic.");
            }

            if (node["sql"]) {
                rhs.sql = node["sql"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'sql' in subscribe-data::control::subscribe_control::topic.");
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Commit> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Commit& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"mode"};
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control::commit");

            if (node["mode"]) {
                rhs.mode = node["mode"].as<std::string>();
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::GroupID> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::GroupID& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"strategy", "custom_id"};
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control::group_id");

            if (node["strategy"]) {
                rhs.strategy = node["strategy"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'strategy' in subscribe-data::control::subscribe_control::group_id.");
            }

            if (rhs.strategy == "custom") {
                if(node["custom_id"]) {
                    rhs.custom_id = node["custom_id"].as<std::string>();
                } else {
                    throw std::runtime_error("Missing required field 'custom_id' in subscribe-data::control::subscribe_control::group_id when strategy is 'custom'.");
                }
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl::Output> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl::Output& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {"path", "file_prefix", "expected_rows"};
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control::output");

            if (node["path"]) {
                rhs.path = node["path"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'path' in subscribe-data::control::subscribe_control::output.");
            }

            if (node["file_prefix"]) {
                rhs.file_prefix = node["file_prefix"].as<std::string>();
            } else {
                throw std::runtime_error("Missing required field 'file_prefix' in subscribe-data::control::subscribe_control::output.");
            }

            if (node["expected_rows"]) {
                rhs.expected_rows = node["expected_rows"].as<int>();
            } else {
                throw std::runtime_error("Missing required field 'expected_rows' in subscribe-data::control::subscribe_control::output.");
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control::SubscribeControl> {
        static bool decode(const Node& node, SubscribeDataConfig::Control::SubscribeControl& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "log_path", "enable_dryrun", "execution", "topics", "commit", "group_id", "output", "advanced"
            };
            check_unknown_keys(node, valid_keys, "subscribe-data::control::subscribe_control");

            if (node["log_path"]) {
                rhs.log_path = node["log_path"].as<std::string>();
            }

            if (node["enable_dryrun"]) {
                rhs.enable_dryrun = node["enable_dryrun"].as<bool>();
            }

            if (node["execution"]) {
                rhs.execution = node["execution"].as<SubscribeDataConfig::Control::SubscribeControl::Execution>();
            }

            if (node["topics"]) {
                rhs.topics = node["topics"].as<std::vector<SubscribeDataConfig::Control::SubscribeControl::Topic>>();
            }

            if (node["commit"]) {
                rhs.commit = node["commit"].as<SubscribeDataConfig::Control::SubscribeControl::Commit>();
            }

            if (node["group_id"]) {
                rhs.group_id = node["group_id"].as<SubscribeDataConfig::Control::SubscribeControl::GroupID>();
            }

            if (node["output"]) {
                rhs.output = node["output"].as<SubscribeDataConfig::Control::SubscribeControl::Output>();
            }

            if (node["advanced"]) {
                rhs.advanced = node["advanced"].as<std::map<std::string, std::string>>();
            }
            return true;
        }
    };


    template<>
    struct convert<SubscribeDataConfig::Control> {
        static bool decode(const Node& node, SubscribeDataConfig::Control& rhs) {
            // Detect unknown configuration keys at the top level
            static const std::set<std::string> valid_keys = {
                "data_format", "data_channel", "subscribe_control"
            };
            check_unknown_keys(node, valid_keys, "subscribe-data::control");

            if (node["data_format"]) {
                // rhs.data_format = node["data_format"].as<DataFormat>();
            } else {
                throw std::runtime_error("Missing required field 'data_format' in subscribe-data::control.");
            }

            if (node["data_channel"]) {
                rhs.data_channel = node["data_channel"].as<DataChannel>();
            } else {
                throw std::runtime_error("Missing required field 'data_channel' in subscribe-data::control.");
            }

            if (node["subscribe_control"]) {
                rhs.subscribe_control = node["subscribe_control"].as<SubscribeDataConfig::Control::SubscribeControl>();
            } else {
                throw std::runtime_error("Missing required field 'subscribe_control' in subscribe-data::control.");
            }
            return true;
        }
    };

    template<>
    struct convert<CreateDatabaseConfig> {
        static bool decode(const Node& node, CreateDatabaseConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "checkpoint"
            };
            check_unknown_keys(node, valid_keys, "tdengine/create-database");

            if (node["checkpoint"]) {
                rhs.checkpoint_info = node["checkpoint"].as<CheckpointInfo>();
            }

            return true;
        }
    };

    template<>
    struct convert<CreateSuperTableConfig> {
        static bool decode(const Node& node, CreateSuperTableConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "schema"
            };
            check_unknown_keys(node, valid_keys, "tdengine/create-super-table");

            if (node["schema"]) {
                rhs.schema = node["schema"].as<SchemaConfig>();
            }

            return true;
        }
    };

    template<>
    struct convert<CreateChildTableConfig> {
        static bool decode(const Node& node, CreateChildTableConfig& rhs) {
            // Detect unknown configuration keys
            static const std::set<std::string> valid_keys = {
                "schema", "batch"
            };
            check_unknown_keys(node, valid_keys, "tdengine/create-child-table");

            if (node["schema"]) {
                rhs.schema = node["schema"].as<SchemaConfig>();
            }

            if (node["batch"]) {
                rhs.batch = node["batch"].as<CreateChildTableConfig::BatchConfig>();
            }

            return true;
        }
    };

    inline const std::set<std::string> insert_common_keys = {
        "schema", "target", "timestamp_precision",
        "concurrency", "queue_capacity", "queue_warmup_ratio", "shared_queue",
        "thread_affinity", "thread_realtime",
        "failure_handling", "time_interval", "checkpoint"
    };

    template<>
    struct convert<InsertDataConfig> {
        static bool decode(const Node& node, InsertDataConfig& rhs) {
            if (node["target"]) {
                rhs.target_type = node["target"].as<std::string>();
            } else {
                rhs.target_type = "tdengine";
            }

            const bool handled = PluginConfigRegistry::apply_format_decoder(rhs.target_type, node, rhs);
            if (!handled) {
                throw std::runtime_error("Invalid or unsupported target type (no format decoder registered): " + rhs.target_type);
            }

            // Parse Common Configs
            if (node["schema"]) {
                rhs.schema = node["schema"].as<SchemaConfig>();
            }

            if (node["timestamp_precision"]) {
                rhs.timestamp_precision = node["timestamp_precision"].as<std::string>();
                if (rhs.timestamp_precision != "ms" && rhs.timestamp_precision != "us" && rhs.timestamp_precision != "ns") {
                    throw std::runtime_error("Invalid timestamp_precision value: " + rhs.timestamp_precision);
                }
            }

            if (node["concurrency"]) {
                int64_t concurrency = node["concurrency"].as<int64_t>();
                if (concurrency <= 0) {
                    throw std::runtime_error("concurrency must be a positive integer.");
                }
                rhs.insert_threads = concurrency;
            }

            if (node["queue_capacity"]) {
                rhs.queue_capacity = node["queue_capacity"].as<size_t>();
            }
            if (node["queue_warmup_ratio"]) {
                rhs.queue_warmup_ratio = node["queue_warmup_ratio"].as<double>();
            }

            if (node["shared_queue"]) {
                rhs.shared_queue = node["shared_queue"].as<bool>();
            }

            if (node["thread_affinity"]) {
                rhs.thread_affinity = node["thread_affinity"].as<bool>();
            }

            if (node["thread_realtime"]) {
                rhs.thread_realtime = node["thread_realtime"].as<bool>();
            }

            if (node["failure_handling"]) {
                rhs.failure_handling = node["failure_handling"].as<InsertDataConfig::FailureHandling>();
            }

            if (node["time_interval"]) {
                rhs.time_interval = node["time_interval"].as<InsertDataConfig::TimeInterval>();
            }

            if (node["checkpoint"]) {
                rhs.checkpoint_info = node["checkpoint"].as<CheckpointInfo>();
            }

            return true;
        }
    };
}
