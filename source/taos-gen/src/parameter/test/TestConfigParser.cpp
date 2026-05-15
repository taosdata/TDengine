#include "ConfigParser.hpp"
#include "PluginRegistrar.hpp"
#include <iostream>
#include <sstream>
#include <cassert>
#include <yaml-cpp/yaml.h>

void test_FromCSVConfig_tags_columns() {
    std::string yaml = R"(
tags:
  file_path: "tags.csv"
  has_header: true
  delimiter: ";"
  tbname_index: 1
  exclude_indices: "2,3"
columns:
  file_path: "cols.csv"
  has_header: false
  repeat_read: true
  delimiter: ","
  tbname_index: 0
  timestamp_index: 5
)";
    YAML::Node node = YAML::Load(yaml);
    FromCSVConfig cfg = node.as<FromCSVConfig>();
    assert(cfg.enabled == true);

    // tags
    assert(cfg.tags.enabled == true);
    assert(cfg.tags.file_path == "tags.csv");
    assert(cfg.tags.has_header == true);
    assert(cfg.tags.delimiter == ";");
    assert(cfg.tags.tbname_index == 1);
    assert(cfg.tags.exclude_indices_str == "2,3");
    assert(!cfg.tags.exclude_indices.empty());

    // columns
    assert(cfg.columns.enabled == true);
    assert(cfg.columns.file_path == "cols.csv");
    assert(cfg.columns.has_header == false);
    assert(cfg.columns.repeat_read == true);
    assert(cfg.columns.delimiter == ",");
    assert(cfg.columns.tbname_index == 0);
    assert(cfg.columns.timestamp_index == 5);
}

void test_FromCSVConfig_missing_file_path_tags() {
    std::string yaml = R"(
tags:
  has_header: true
  delimiter: ";"
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        FromCSVConfig cfg = node.as<FromCSVConfig>();
        assert(false && "Should throw for missing file_path in tags");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Missing required 'file_path' configuration for schema::from_csv::tags") != std::string::npos);
    }
}

void test_FromCSVConfig_missing_file_path_columns() {
    std::string yaml = R"(
columns:
    has_header: false
    delimiter: ","
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        FromCSVConfig cfg = node.as<FromCSVConfig>();
        assert(false && "Should throw for missing file_path in columns");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Missing required 'file_path' configuration for schema::from_csv::columns") != std::string::npos);
    }
}

void test_FromCSVConfig_unknown_key_tags() {
    std::string yaml = R"(
tags:
  file_path: "tags.csv"
  unknown_key: "value"
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        FromCSVConfig cfg = node.as<FromCSVConfig>();
        assert(false && "Should throw for unknown key in tags");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Unknown configuration key in schema::from_csv") != std::string::npos);
    }
}

void test_FromCSVConfig_unknown_key_columns() {
    std::string yaml = R"(
columns:
  file_path: "cols.csv"
  unknown_key: "value"
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        FromCSVConfig cfg = node.as<FromCSVConfig>();
        assert(false && "Should throw for unknown key in columns");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Unknown configuration key in schema::from_csv") != std::string::npos);
    }
}

void test_GenerationConfig_DataDisorder_Interval() {
    std::string yaml = R"(
time_start: "2025-09-08T00:00:00Z"
time_end: "2025-09-08T01:00:00Z"
ratio: 0.25
latency_range: 100
)";
    YAML::Node node = YAML::Load(yaml);
    GenerationConfig::DataDisorder::Interval interval = node.as<GenerationConfig::DataDisorder::Interval>();

    assert(std::holds_alternative<std::string>(interval.time_start));
    assert(std::get<std::string>(interval.time_start) == "2025-09-08T00:00:00Z");
    assert(std::holds_alternative<std::string>(interval.time_start));
    assert(std::get<std::string>(interval.time_end) == "2025-09-08T01:00:00Z");
    assert(interval.ratio == 0.25);
    assert(interval.latency_range == 100);
}

void test_GenerationConfig_DataDisorder() {
    std::string yaml = R"(
- time_start: "2025-09-08T00:00:00Z"
  time_end: "2025-09-08T01:00:00Z"
  ratio: 0.5
  latency_range: 100
- time_start: "2025-09-08T01:00:00Z"
  time_end: "2025-09-08T02:00:00Z"
  ratio: 0.25
  latency_range: 200
)";
    YAML::Node node = YAML::Load(yaml);
    GenerationConfig::DataDisorder disorder = node.as<GenerationConfig::DataDisorder>();
    assert(disorder.enabled == true);
    assert(disorder.intervals.size() == 2);
    assert(std::holds_alternative<std::string>(disorder.intervals[0].time_start));
    assert(std::get<std::string>(disorder.intervals[0].time_start) == "2025-09-08T00:00:00Z");
    assert(std::holds_alternative<std::string>(disorder.intervals[0].time_start));
    assert(std::get<std::string>(disorder.intervals[0].time_end) == "2025-09-08T01:00:00Z");
    assert(disorder.intervals[0].ratio == 0.5);
    assert(disorder.intervals[0].latency_range == 100);

    assert(std::holds_alternative<std::string>(disorder.intervals[1].time_start));
    assert(std::get<std::string>(disorder.intervals[1].time_start) == "2025-09-08T01:00:00Z");
    assert(std::holds_alternative<std::string>(disorder.intervals[1].time_start));
    assert(std::get<std::string>(disorder.intervals[1].time_end) == "2025-09-08T02:00:00Z");
    assert(disorder.intervals[1].ratio == 0.25);
    assert(disorder.intervals[1].latency_range == 200);
}

void test_GenerationConfig_base() {
    std::string yaml = R"(
interlace: 100
num_cached_batches: 18
rate_limit: 5000
data_disorder:
  - time_start: "2025-09-08T00:00:00Z"
    time_end: "2025-09-08T01:00:00Z"
    ratio: 0.5
    latency_range: 100
concurrency: 8
rows_per_table: 10000
rows_per_batch: 500
)";
    YAML::Node node = YAML::Load(yaml);
    GenerationConfig cfg = node.as<GenerationConfig>();

    assert(cfg.interlace_mode.enabled == true);
    assert(cfg.interlace_mode.rows == 100);

    assert(cfg.data_cache.enabled == true);
    assert(cfg.data_cache.num_cached_batches == 18);

    assert(cfg.flow_control.enabled == true);
    assert(cfg.flow_control.rate_limit == 5000);

    assert(cfg.data_disorder.enabled == true);
    assert(cfg.data_disorder.intervals.size() == 1);
    assert(std::holds_alternative<std::string>(cfg.data_disorder.intervals[0].time_start));
    assert(std::get<std::string>(cfg.data_disorder.intervals[0].time_start) == "2025-09-08T00:00:00Z");
    assert(std::holds_alternative<std::string>(cfg.data_disorder.intervals[0].time_end));
    assert(std::get<std::string>(cfg.data_disorder.intervals[0].time_end) == "2025-09-08T01:00:00Z");

    assert(cfg.data_disorder.intervals[0].ratio == 0.5);
    assert(cfg.data_disorder.intervals[0].latency_range == 100);

    assert(cfg.generate_threads.has_value());
    assert(cfg.generate_threads.value() == 8);

    assert(cfg.rows_per_table == 10000);
    assert(cfg.rows_per_batch == 500);
}

void test_GenerationConfig_data_cache() {
    // interlace disabled
    {
        std::string yaml = R"(
rows_per_table: 10000
rows_per_batch: 500
)";
        YAML::Node node = YAML::Load(yaml);
        GenerationConfig cfg = node.as<GenerationConfig>();
        (void)cfg;
        assert(cfg.interlace_mode.enabled == false);
        assert(cfg.rows_per_table == 10000);
        assert(cfg.rows_per_batch == 500);
        assert(cfg.data_cache.enabled == true);
        assert(cfg.data_cache.num_cached_batches == 20);
    }

    // interlace enabled
    {
        std::string yaml = R"(
interlace: 100
rows_per_table: 10000
rows_per_batch: 500
)";
        YAML::Node node = YAML::Load(yaml);
        GenerationConfig cfg = node.as<GenerationConfig>();
        (void)cfg;
        assert(cfg.interlace_mode.enabled == true);
        assert(cfg.interlace_mode.rows == 100);
        assert(cfg.rows_per_table == 10000);
        assert(cfg.rows_per_batch == 500);
        assert(cfg.data_cache.enabled == true);
        assert(cfg.data_cache.num_cached_batches == 100);
    }

    // invalid
    {
        std::string yaml = R"(
interlace: 100
rows_per_table: 10000
rows_per_batch: 500
num_cached_batches: 200
)";
        YAML::Node node = YAML::Load(yaml);
        try {
            GenerationConfig cfg = node.as<GenerationConfig>();
            assert(false && "Should throw for invalid num_cached_batches");
        } catch (const std::runtime_error& e) {
            assert(std::string(e.what()).find("num_cached_batches cannot be greater than the number of batches needed") != std::string::npos);
        }
    }
}

void test_SchemaConfig() {
    std::string yaml = R"(
name: test_schema
columns:
  - name: ts
    type: BIGINT
    precision: ms
    start: "2025-09-08T00:00:00Z"
    step: "1000"
  - name: value
    type: DOUBLE
    min: 0
    max: 100
tags:
  - name: tag1
    type: INT
    min: 1
    max: 10
tbname:
  prefix: "tb"
  count: 5
generation:
  interlace: 10
  num_cached_batches: 8
  rate_limit: 1000
  concurrency: 2
  rows_per_table: 100
  rows_per_batch: 10
)";
    YAML::Node node = YAML::Load(yaml);
    SchemaConfig cfg = node.as<SchemaConfig>();

    assert(cfg.name == "test_schema");
    assert(cfg.columns.size() == 2);
    assert(cfg.columns[0].name == "ts");
    assert(cfg.columns[0].type == "BIGINT");
    assert(cfg.columns[1].name == "value");
    assert(cfg.columns[1].type == "DOUBLE");
    assert(cfg.tags.size() == 1);
    assert(cfg.tags[0].name == "tag1");
    assert(cfg.tags[0].type == "INT");
    assert(cfg.tbname.source_type == "generator");
    assert(cfg.tbname.generator.prefix == "tb");
    assert(cfg.tbname.generator.count == 5);
    assert(cfg.generation.interlace_mode.enabled == true);
    assert(cfg.generation.interlace_mode.rows == 10);
    assert(cfg.generation.data_cache.enabled == true);
    assert(cfg.generation.data_cache.num_cached_batches == 8);
    assert(cfg.generation.flow_control.enabled == true);
    assert(cfg.generation.flow_control.rate_limit == 1000);
    assert(cfg.generation.generate_threads.has_value());
    assert(cfg.generation.generate_threads.value() == 2);
    assert(cfg.generation.rows_per_table == 100);
    assert(cfg.generation.rows_per_batch == 10);
}

void test_SchemaConfig_csv_ts_gen() {
    std::string yaml = R"(
name: test_schema
from_csv:
  tags:
    file_path: "tags.csv"
    has_header: true
    tbname_index: 1
    exclude_indices: "2,3"
  columns:
    loading_mode: preload
    file_path: "cols.csv"
    has_header: false
    repeat_read: true
    tbname_index: 0
columns:
- name: ts
  type: BIGINT
  precision: ms
  start: "2025-09-08T00:00:00Z"
  step: "1000"
- name: value
  type: DOUBLE
  min: 0
  max: 100
tags:
- name: tag1
  type: INT
  min: 1
  max: 10
)";
    YAML::Node node = YAML::Load(yaml);
    SchemaConfig cfg = node.as<SchemaConfig>();

    assert(cfg.name == "test_schema");

    assert(cfg.from_csv.enabled == true);
    assert(cfg.from_csv.tags.enabled == true);
    assert(cfg.from_csv.tags.file_path == "tags.csv");
    assert(cfg.from_csv.tags.has_header == true);
    assert(cfg.from_csv.tags.tbname_index == 1);
    assert(cfg.from_csv.tags.exclude_indices_str == "2,3");
    assert(!cfg.from_csv.tags.exclude_indices.empty());
    assert(cfg.from_csv.columns.enabled == true);
    assert(cfg.from_csv.columns.loading_mode == "preload");
    assert(cfg.from_csv.columns.file_path == "cols.csv");
    assert(cfg.from_csv.columns.has_header == false);
    assert(cfg.from_csv.columns.repeat_read == true);
    assert(cfg.from_csv.columns.tbname_index == 0);

    assert(cfg.columns.size() == 2);
    assert(cfg.columns[0].name == "ts");
    assert(cfg.columns[0].type == "BIGINT");
    assert(cfg.columns[1].name == "value");
    assert(cfg.columns[1].type == "DOUBLE");
    assert(cfg.tags.size() == 1);
    assert(cfg.tags[0].name == "tag1");
    assert(cfg.tags[0].type == "INT");

    assert(cfg.columns_cfg.csv.timestamp_strategy.strategy_type == "generator");
    assert(std::get<std::string>(cfg.columns_cfg.csv.timestamp_strategy.generator.start_timestamp) == "2025-09-08T00:00:00Z");
    assert(std::get<Timestamp>(cfg.columns_cfg.csv.timestamp_strategy.generator.timestamp_step) == 1000);
    assert(cfg.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision == "ms");
}

void test_DatabaseInfo() {
    std::string yaml = R"(
name: testdb
drop_if_exists: true
precision: ms
props: "replica=2"
)";
    YAML::Node node = YAML::Load(yaml);
    DatabaseInfo db = node.as<DatabaseInfo>();
    assert(db.name == "testdb");
    assert(db.drop_if_exists == true);
    assert(db.precision == "ms");
    assert(db.properties == "replica=2");
}

void test_ColumnConfig_random() {
    std::string yaml = R"(
name: temperature
type: float
primary_key: false
gen_type: random
distribution: normal
min: 10.0
max: 50.0
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "temperature");
    assert(col.type == "float");
    assert(col.gen_type.has_value() && *col.gen_type == "random");
    assert(col.distribution.has_value() && *col.distribution == "normal");
    assert(col.min.has_value() && *col.min == 10.0);
    assert(col.max.has_value() && *col.max == 50.0);
}

void test_ColumnConfig_order() {
    std::string yaml = R"(
name: id
type: int
gen_type: order
min: 1
max: 100
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "id");
    assert(col.type == "int");
    assert(col.gen_type.has_value() && *col.gen_type == "order");
    assert(col.order_min.has_value() && *col.order_min == 1);
    assert(col.order_max.has_value() && *col.order_max == 100);
}

void test_ColumnConfig_min_max_length_varchar() {
    std::string yaml = R"(
name: describe
type: varchar(20)
min_length: 5
max_length: 15
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "describe");
    assert(col.type == "varchar(20)");
    assert(col.gen_type.has_value() && *col.gen_type == "random");
    assert(col.min_length.has_value() && *col.min_length == 5);
    assert(col.max_length.has_value() && *col.max_length == 15);
}

void test_ColumnConfig_min_max_length_nchar() {
    std::string yaml = R"(
name: info
type: nchar(32)
min_length: 0
max_length: 32
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "info");
    assert(col.type == "nchar(32)");
    assert(col.min_length.has_value() && *col.min_length == 0);
    assert(col.max_length.has_value() && *col.max_length == 32);
}

void test_ColumnConfig_only_min_length() {
    std::string yaml = R"(
name: label
type: binary(16)
min_length: 3
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "label");
    assert(col.min_length.has_value() && *col.min_length == 3);
    assert(col.max_length.has_value() && *col.max_length == 16);
}

void test_ColumnConfig_only_max_length() {
    std::string yaml = R"(
name: label
type: varchar(20)
max_length: 10
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "label");
    assert(col.min_length.has_value() && *col.min_length == 0);
    assert(col.max_length.has_value() && *col.max_length == 10);
}

void test_ColumnConfig_min_max_length_default() {
    std::string yaml = R"(
name: tag
type: varchar(10)
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "tag");
    assert(!col.min_length.has_value());
    assert(!col.max_length.has_value());
}

void test_ColumnConfig_max_length_exceeds_cap() {
    std::string yaml = R"(
name: bad
type: varchar(10)
max_length: 20
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for max_length exceeding capacity");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("exceeds type capacity") != std::string::npos);
    }
}

void test_ColumnConfig_min_length_exceeds_max_length() {
    std::string yaml = R"(
name: bad
type: varchar(10)
min_length: 8
max_length: 5
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for min_length > max_length");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("must be <= max_length") != std::string::npos);
    }
}

void test_ColumnConfig_negative_min_length() {
    std::string yaml = R"(
name: bad
type: varchar(10)
min_length: -1
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for negative min_length");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("min_length must be >= 0") != std::string::npos);
    }
}

void test_ColumnConfig_negative_max_length() {
    std::string yaml = R"(
name: bad
type: varchar(10)
max_length: -1
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for negative max_length");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("max_length must be >= 0") != std::string::npos);
    }
}

void test_ColumnConfig_min_max_length_non_varlen() {
    std::string yaml = R"(
name: val
type: int
min_length: 1
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for min_length on non-varlen type");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("only applicable to variable-length types") != std::string::npos);
    }
}

void test_ColumnConfig_corpus_within_len() {
    std::string yaml = R"(
name: code
type: varchar(10)
corpus: "abc"
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "code");
    assert(col.corpus.has_value() && *col.corpus == "abc");
}

void test_ColumnConfig_corpus_exceeds_len() {
    std::string yaml = R"(
name: code
type: varchar(3)
corpus: "abcdef"
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for corpus exceeding max length");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("corpus length (6) exceeds max length (3)") != std::string::npos);
    }
}

void test_ColumnConfig_corpus_exact_len() {
    std::string yaml = R"(
name: flag
type: binary(5)
corpus: "abcde"
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.corpus.has_value() && col.corpus->size() == 5);
}

void test_ColumnConfig_corpus_empty() {
    std::string yaml = R"(
name: code
type: varchar(10)
corpus: ""
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for empty corpus");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("corpus must be non-empty") != std::string::npos);
    }
}

void test_ColumnConfig_null_none_ratio_valid() {
    std::string yaml = R"(
name: temp
type: float
null_ratio: 0.2
none_ratio: 0.3
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.null_ratio.has_value() && std::abs(*col.null_ratio - 0.2f) < 1e-5f);
    assert(col.none_ratio.has_value() && std::abs(*col.none_ratio - 0.3f) < 1e-5f);
}

void test_ColumnConfig_null_ratio_only() {
    std::string yaml = R"(
name: temp
type: int
null_ratio: 0.5
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.null_ratio.has_value() && std::abs(*col.null_ratio - 0.5f) < 1e-5f);
    assert(!col.none_ratio.has_value());
}

void test_ColumnConfig_null_ratio_negative() {
    std::string yaml = R"(
name: temp
type: float
null_ratio: -0.1
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for negative null_ratio");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("null_ratio must be a finite value in [0.0, 1.0]") != std::string::npos);
    }
}

void test_ColumnConfig_none_ratio_negative() {
    std::string yaml = R"(
name: temp
type: float
none_ratio: -0.1
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for negative none_ratio");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("none_ratio must be a finite value in [0.0, 1.0]") != std::string::npos);
    }
}

void test_ColumnConfig_null_ratio_nan() {
    std::string yaml = R"(
name: temp
type: float
null_ratio: .nan
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for NaN null_ratio");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("null_ratio must be a finite value in [0.0, 1.0]") != std::string::npos);
    }
}

void test_ColumnConfig_none_ratio_inf() {
    std::string yaml = R"(
name: temp
type: float
none_ratio: .inf
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for Inf none_ratio");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("none_ratio must be a finite value in [0.0, 1.0]") != std::string::npos);
    }
}

void test_ColumnConfig_null_ratio_above_one() {
    std::string yaml = R"(
name: temp
type: float
null_ratio: 1.5
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for null_ratio > 1.0");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("null_ratio must be a finite value in [0.0, 1.0]") != std::string::npos);
    }
}

void test_ColumnConfig_null_none_ratio_sum_exceeds() {
    std::string yaml = R"(
name: temp
type: float
null_ratio: 0.6
none_ratio: 0.5
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<ColumnConfig>();
        assert(false && "Should throw for sum > 1.0");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("null_ratio + none_ratio must be <= 1.0") != std::string::npos);
    }
}

void test_ColumnConfig_null_none_ratio_sum_equals_one() {
    std::string yaml = R"(
name: temp
type: double
null_ratio: 0.5
none_ratio: 0.5
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.null_ratio.has_value() && col.none_ratio.has_value());
}

void test_ColumnConfig_expression() {
    std::string yaml = R"(
name: value
type: float
gen_type: expression
expr: "2*sinusoid(period=10,min=0,max=10)+3"
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "value");
    assert(col.type == "float");
    assert(col.gen_type.has_value() && *col.gen_type == "expression");
    assert(col.formula.has_value());
    assert(col.formula == "2*sinusoid(period=10,min=0,max=10)+3");
}

void test_ColumnConfig_strip_backticks_plain() {
    std::string yaml = R"(
name: "`id`"
type: int
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "id");
    assert(col.type == "int");

    yaml = R"(
name: "``"
type: int
)";
    node = YAML::Load(yaml);
    col = node.as<ColumnConfig>();
    assert(col.name == "");
    assert(col.type == "int");
}

void test_ColumnConfig_strip_backticks_unmatched() {
    std::string yaml = R"(
name: "`leading"
type: binary(20)
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "`leading");
    assert(col.type == "binary(20)");

    yaml = R"(
name: "trailing`"
type: binary(20)
)";
    node = YAML::Load(yaml);
    col = node.as<ColumnConfig>();
    assert(col.name == "trailing`");
    assert(col.type == "binary(20)");
}

void test_ColumnConfig_strip_backticks_none() {
    std::string yaml = R"(
name: value_no_bt
type: double
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "value_no_bt");
    assert(col.type == "double");
}

void test_TableNameConfig_generator() {
    std::string yaml = R"(
prefix: tb
count: 10
from: 1
)";
    YAML::Node node = YAML::Load(yaml);
    TableNameConfig tnc = node.as<TableNameConfig>();
    assert(tnc.source_type == "generator");
    assert(tnc.generator.prefix == "tb");
    assert(tnc.generator.count == 10);
    assert(tnc.generator.from == 1);
}

void test_TableNameConfig_csv() {
    std::string yaml = R"(
file_path: tables.csv
has_header: true
delimiter: ","
tbname_index: 0
)";
    YAML::Node node = YAML::Load(yaml);
    TableNameConfig tnc = node.as<TableNameConfig>();
    assert(tnc.source_type == "csv");
    assert(tnc.csv.file_path == "tables.csv");
    assert(tnc.csv.has_header == true);
    assert(tnc.csv.delimiter == ",");
    assert(tnc.csv.tbname_index == 0);
}

void test_TagsConfig_generator() {
    std::string yaml = R"(
source_type: generator
generator:
  schema:
    - name: tag1
      type: int
    - name: tag2
      type: binary(20)
)";
    YAML::Node node = YAML::Load(yaml);
    TagsConfig tags = node.as<TagsConfig>();
    assert(tags.source_type == "generator");
    assert(tags.generator.schema.size() == 2);
    assert(tags.generator.schema[0].name == "tag1");
    assert(tags.generator.schema[1].type == "binary(20)");
    assert(tags.generator.schema[1].type_tag == ColumnTypeTag::BINARY);
}

void test_TagsConfig_csv() {
    std::string yaml = R"(
source_type: csv
csv:
  file_path: tags.csv
  has_header: false
  delimiter: ";"
  exclude_indices: "1,2"
)";
    YAML::Node node = YAML::Load(yaml);
    TagsConfig tags = node.as<TagsConfig>();
    assert(tags.source_type == "csv");
    assert(tags.csv.file_path == "tags.csv");
    assert(tags.csv.has_header == false);
    assert(tags.csv.delimiter == ";");
    assert(!tags.csv.exclude_indices.empty());
}

void test_SuperTableInfo() {
    std::string yaml = R"(
name: st
columns:
  - name: c1
    type: int
  - name: c2
    type: float
tags:
  - name: t1
    type: binary(20)
)";
    YAML::Node node = YAML::Load(yaml);
    SuperTableInfo st = node.as<SuperTableInfo>();
    assert(st.name == "st");
    assert(st.columns.size() == 2);
    assert(st.tags.size() == 1);
}

void test_ChildTableInfo() {
    std::string yaml = R"(
table_name:
  prefix: tb
  count: 2
  from: 1
tags:
  source_type: generator
  generator:
    schema:
      - name: tag1
        type: int
)";
    YAML::Node node = YAML::Load(yaml);
    ChildTableInfo ct = node.as<ChildTableInfo>();
    assert(ct.table_name.source_type == "generator");
    assert(ct.tags.source_type == "generator");
    assert(ct.tags.generator.schema.size() == 1);
}

void test_CreateChildTableConfig_BatchConfig() {
    std::string yaml = R"(
size: 100
concurrency: 4
)";
    YAML::Node node = YAML::Load(yaml);
    CreateChildTableConfig::BatchConfig bc = node.as<CreateChildTableConfig::BatchConfig>();
    (void)bc;
    assert(bc.size == 100);
    assert(bc.concurrency == 4);
}

void test_TimestampGeneratorConfig() {
    std::string yaml = R"(
start_timestamp: "2023-01-01T00:00:00Z"
timestamp_precision: ms
timestamp_step: 10
)";
    YAML::Node node = YAML::Load(yaml);
    TimestampGeneratorConfig tgc = node.as<TimestampGeneratorConfig>();
    assert(std::holds_alternative<std::string>(tgc.start_timestamp));
    assert(std::get<std::string>(tgc.start_timestamp) == "2023-01-01T00:00:00Z");
    assert(tgc.timestamp_precision == "ms");
    assert(std::get<Timestamp>(tgc.timestamp_step) == 10);
}

void test_TimestampOriginalConfig() {
    std::string yaml = R"(
column_index: 0
precision: ms
offset_config:
  offset_type: relative
  value: "+1d"
)";
    YAML::Node node = YAML::Load(yaml);
    TimestampCSVConfig toc = node.as<TimestampCSVConfig>();
    assert(toc.timestamp_index == 0);
    assert(toc.timestamp_precision == "ms");
    assert(toc.offset_config.has_value());
    assert(std::holds_alternative<std::string>(toc.offset_config->value));
}

void test_ColumnsConfig_generator() {
    std::string yaml = R"(
source_type: generator
generator:
  schema:
    - name: c1
      type: int
  timestamp_strategy:
    generator:
      start_timestamp: "2023-01-01T00:00:00Z"
      timestamp_precision: ms
      timestamp_step: 1
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnsConfig cc = node.as<ColumnsConfig>();
    assert(cc.source_type == "generator");
    assert(cc.generator.schema.size() == 1);
    assert(std::holds_alternative<std::string>(cc.generator.timestamp_strategy.timestamp_config.start_timestamp));
    assert(std::get<std::string>(cc.generator.timestamp_strategy.timestamp_config.start_timestamp) == "2023-01-01T00:00:00Z");
    assert(cc.generator.timestamp_strategy.timestamp_config.timestamp_precision == "ms");
    assert(std::get<Timestamp>(cc.generator.timestamp_strategy.timestamp_config.timestamp_step) == 1);
}

void test_ColumnsConfig_csv() {
    std::string yaml = R"(
source_type: csv
csv:
  schema:
    - name: c1
      type: int
  file_path: data.csv
  has_header: true
  delimiter: ","
  timestamp_strategy:
    strategy_type: csv
    csv:
      column_index: 0
      precision: ms
      offset_config:
        offset_type: relative
        value: "+1d"
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnsConfig cc = node.as<ColumnsConfig>();
    assert(cc.source_type == "csv");
    assert(cc.csv.schema.size() == 1);
    assert(cc.csv.schema[0].name == "c1");
    assert(cc.csv.schema[0].type == "int");
    assert(cc.csv.file_path == "data.csv");
    assert(cc.csv.timestamp_strategy.strategy_type == "csv");
}

void test_DataChannel() {
    std::string yaml = R"(
channel_type: native
)";
    YAML::Node node = YAML::Load(yaml);
    DataChannel dc = node.as<DataChannel>();
    assert(dc.channel_type == "native");
}

void test_InsertDataConfig_FailureHandling() {
    std::string yaml = R"(
max_retries: 5
retry_interval_ms: 100
on_failure: skip
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig::FailureHandling fh = node.as<InsertDataConfig::FailureHandling>();
    assert(fh.max_retries == 5);
    assert(fh.retry_interval_ms == 100);
    assert(fh.on_failure == "skip");
}

void test_InsertDataConfig_FailureHandling_InvalidValue() {
    std::string yaml = R"(
on_failure: invalid_action
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<InsertDataConfig::FailureHandling>();
        assert(false && "Should throw for invalid on_failure value");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Invalid value for on_failure") != std::string::npos);
    }
}

void test_InsertDataConfig_FailureHandling_UnknownKey() {
    std::string yaml = R"(
unknown_key: some_value
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<InsertDataConfig::FailureHandling>();
        assert(false && "Should throw for unknown key in failure_handling");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Unknown configuration key in insert-data::failure_handling") != std::string::npos);
    }
}

void test_InsertDataConfig_Control_TimeInterval() {
    // fixed_interval
    std::string yaml_fixed = R"(
enabled: true
interval_strategy: fixed
wait_strategy: sleep
fixed_interval:
  base_interval: 1000
  random_deviation: 50
)";
    YAML::Node node_fixed = YAML::Load(yaml_fixed);
    InsertDataConfig::TimeInterval ti_fixed = node_fixed.as<InsertDataConfig::TimeInterval>();
    assert(ti_fixed.enabled == true);
    assert(ti_fixed.interval_strategy == "fixed");
    assert(ti_fixed.fixed_interval.base_interval == 1000);
    assert(ti_fixed.fixed_interval.random_deviation == 50);

    // dynamic_interval
    std::string yaml_dynamic = R"(
enabled: true
interval_strategy: first_to_first
wait_strategy: sleep
dynamic_interval:
  min_interval: 10
  max_interval: 100
)";
    YAML::Node node_dynamic = YAML::Load(yaml_dynamic);
    InsertDataConfig::TimeInterval ti_dynamic = node_dynamic.as<InsertDataConfig::TimeInterval>();
    assert(ti_dynamic.enabled == true);
    assert(ti_dynamic.interval_strategy == "first_to_first");
    assert(ti_dynamic.dynamic_interval.min_interval == 10);
    assert(ti_dynamic.dynamic_interval.max_interval == 100);
}

void test_QueryDataConfig_Source() {
    std::string yaml = R"(
connection_info:
  dsn: "taos://root:taosdata@localhost:6030/tsbench"
)";
    YAML::Node node = YAML::Load(yaml);
    QueryDataConfig::Source src = node.as<QueryDataConfig::Source>();
    assert(src.connection_info.host == "localhost");
    // assert(src.connection_info.port == 6030);
    // assert(src.connection_info.user == "root");
    // assert(src.connection_info.password == "taosdata");
}

void test_QueryDataConfig_Control_QueryControl_Execution() {
    std::string yaml = R"(
mode: parallel
threads: 4
times: 10
interval: 100
)";
    YAML::Node node = YAML::Load(yaml);
    QueryDataConfig::Control::QueryControl::Execution exec = node.as<QueryDataConfig::Control::QueryControl::Execution>();
    assert(exec.mode == "parallel");
    assert(exec.threads == 4);
    assert(exec.times == 10);
    assert(exec.interval == 100);
}

void test_QueryDataConfig_Control_QueryControl_Fixed() {
    std::string yaml = R"(
queries:
  - sql: "select * from tb1"
    output_file: "out1.txt"
  - sql: "select * from tb2"
    output_file: "out2.txt"
)";
    YAML::Node node = YAML::Load(yaml);
    QueryDataConfig::Control::QueryControl::Fixed fixed = node.as<QueryDataConfig::Control::QueryControl::Fixed>();
    assert(fixed.queries.size() == 2);
    assert(fixed.queries[0].sql == "select * from tb1");
    assert(fixed.queries[0].output_file == "out1.txt");
    assert(fixed.queries[1].sql == "select * from tb2");
    assert(fixed.queries[1].output_file == "out2.txt");
}

void test_QueryDataConfig_Control_QueryControl_SuperTable() {
    std::string yaml = R"(
database_name: testdb
super_table_name: st
placeholder: "{tb}"
templates:
  - sql_template: "select * from {tb} limit 10"
    output_file: "tb1.txt"
  - sql_template: "select count(*) from {tb}"
    output_file: "tb2.txt"
)";
    YAML::Node node = YAML::Load(yaml);
    QueryDataConfig::Control::QueryControl::SuperTable st = node.as<QueryDataConfig::Control::QueryControl::SuperTable>();
    assert(st.database_name == "testdb");
    assert(st.super_table_name == "st");
    assert(st.placeholder == "{tb}");
    assert(st.templates.size() == 2);
    assert(st.templates[0].sql_template == "select * from {tb} limit 10");
    assert(st.templates[0].output_file == "tb1.txt");
    assert(st.templates[1].sql_template == "select count(*) from {tb}");
    assert(st.templates[1].output_file == "tb2.txt");
}

void test_QueryDataConfig_Control_QueryControl() {
    // fixed type
    std::string yaml_fixed = R"(
log_path: "query.log"
enable_dryrun: true
execution:
  mode: parallel
  threads: 2
  times: 5
  interval: 50
query_type: fixed
fixed:
  queries:
    - sql: "select 1"
      output_file: "out.txt"
)";
    YAML::Node node_fixed = YAML::Load(yaml_fixed);
    QueryDataConfig::Control::QueryControl qc_fixed = node_fixed.as<QueryDataConfig::Control::QueryControl>();
    assert(qc_fixed.log_path == "query.log");
    assert(qc_fixed.enable_dryrun == true);
    assert(qc_fixed.execution.mode == "parallel");
    assert(qc_fixed.query_type == "fixed");
    assert(qc_fixed.fixed.queries.size() == 1);
    assert(qc_fixed.fixed.queries[0].sql == "select 1");

    // super_table type
    std::string yaml_st = R"(
log_path: "query.log"
enable_dryrun: false
execution:
  mode: sequential_per_thread
  threads: 1
  times: 1
  interval: 0
query_type: super_table
super_table:
  database_name: testdb
  super_table_name: st
  placeholder: "{tb}"
  templates:
    - sql_template: "select * from {tb}"
      output_file: "tb.txt"
)";
    YAML::Node node_st = YAML::Load(yaml_st);
    QueryDataConfig::Control::QueryControl qc_st = node_st.as<QueryDataConfig::Control::QueryControl>();
    assert(qc_st.query_type == "super_table");
    assert(qc_st.super_table.database_name == "testdb");
    assert(qc_st.super_table.templates.size() == 1);
    assert(qc_st.super_table.templates[0].sql_template == "select * from {tb}");
}

void test_QueryDataConfig_Control() {
    std::string yaml = R"(
data_format:
  format_type: sql
data_channel:
  channel_type: native
query_control:
  log_path: "query.log"
  enable_dryrun: false
  execution:
    mode: parallel
    threads: 2
    times: 3
    interval: 10
  query_type: fixed
  fixed:
    queries:
      - sql: "select 1"
        output_file: "out.txt"
)";
    YAML::Node node = YAML::Load(yaml);
    QueryDataConfig::Control ctrl = node.as<QueryDataConfig::Control>();
    assert(ctrl.data_format.format_type == "sql");
    assert(ctrl.data_channel.channel_type == "native");
    assert(ctrl.query_control.log_path == "query.log");
    assert(ctrl.query_control.execution.threads == 2);
    assert(ctrl.query_control.fixed.queries.size() == 1);
    assert(ctrl.query_control.fixed.queries[0].sql == "select 1");
}

void test_SubscribeDataConfig_Source() {
    std::string yaml = R"(
connection_info:
  dsn: "taos://root:taosdata@localhost:6030/tsbench"
)";
    YAML::Node node = YAML::Load(yaml);
    SubscribeDataConfig::Source src = node.as<SubscribeDataConfig::Source>();
    assert(src.connection_info.host == "localhost");
    // assert(src.connection_info.port == 6030);
    // assert(src.connection_info.user == "root");
    // assert(src.connection_info.password == "taosdata");
}

void test_SubscribeDataConfig_Control_SubscribeControl_Execution() {
    std::string yaml = R"(
consumer_concurrency: 3
poll_timeout: 2000
)";
    YAML::Node node = YAML::Load(yaml);
    SubscribeDataConfig::Control::SubscribeControl::Execution exec = node.as<SubscribeDataConfig::Control::SubscribeControl::Execution>();
    (void)exec;
    assert(exec.consumer_concurrency == 3);
    assert(exec.poll_timeout == 2000);
}

void test_SubscribeDataConfig_Control_SubscribeControl_Topic() {
    std::string yaml = R"(
name: topic1
sql: "select * from st"
)";
    YAML::Node node = YAML::Load(yaml);
    SubscribeDataConfig::Control::SubscribeControl::Topic topic = node.as<SubscribeDataConfig::Control::SubscribeControl::Topic>();
    assert(topic.name == "topic1");
    assert(topic.sql == "select * from st");
}

void test_SubscribeDataConfig_Control_SubscribeControl_Commit() {
  std::string yaml = R"(
mode: manual
)";
  YAML::Node node = YAML::Load(yaml);
  SubscribeDataConfig::Control::SubscribeControl::Commit commit = node.as<SubscribeDataConfig::Control::SubscribeControl::Commit>();
  assert(commit.mode == "manual");
}

void test_SubscribeDataConfig_Control_SubscribeControl_GroupID() {
    std::string yaml = R"(
strategy: custom
custom_id: "group-123"
)";
    YAML::Node node = YAML::Load(yaml);
    SubscribeDataConfig::Control::SubscribeControl::GroupID gid = node.as<SubscribeDataConfig::Control::SubscribeControl::GroupID>();
    assert(gid.strategy == "custom");
    assert(gid.custom_id == "group-123");
}

void test_SubscribeDataConfig_Control_SubscribeControl_Output() {
    std::string yaml = R"(
path: "./out"
file_prefix: "sub"
expected_rows: 100
)";
    YAML::Node node = YAML::Load(yaml);
    SubscribeDataConfig::Control::SubscribeControl::Output out = node.as<SubscribeDataConfig::Control::SubscribeControl::Output>();
    assert(out.path == "./out");
    assert(out.file_prefix == "sub");
    assert(out.expected_rows == 100);
}

void test_SubscribeDataConfig_Control_SubscribeControl() {
    std::string yaml = R"(
log_path: "sub.log"
enable_dryrun: true
execution:
  consumer_concurrency: 2
  poll_timeout: 1500
topics:
  - name: topic1
    sql: "select * from st"
  - name: topic2
    sql: "select count(*) from st"
commit:
  mode: auto
group_id:
  strategy: custom
  custom_id: "gid-1"
output:
  path: "./out"
  file_prefix: "sub"
  expected_rows: 100
advanced:
  key1: value1
  key2: value2
)";
    YAML::Node node = YAML::Load(yaml);
    SubscribeDataConfig::Control::SubscribeControl ctrl = node.as<SubscribeDataConfig::Control::SubscribeControl>();
    assert(ctrl.log_path == "sub.log");
    assert(ctrl.enable_dryrun == true);
    assert(ctrl.execution.consumer_concurrency == 2);
    assert(ctrl.topics.size() == 2);
    assert(ctrl.topics[0].name == "topic1");
    assert(ctrl.commit.mode == "auto");
    assert(ctrl.group_id.strategy == "custom");
    assert(ctrl.group_id.custom_id == "gid-1");
    assert(ctrl.output.path == "./out");
    assert(ctrl.advanced["key1"] == "value1");
}

void test_SubscribeDataConfig_Control() {
    std::string yaml = R"(
data_format:
  format_type: sql
data_channel:
  channel_type: native
subscribe_control:
  log_path: "sub.log"
  enable_dryrun: false
  execution:
    consumer_concurrency: 1
    poll_timeout: 1000
  topics:
    - name: topic1
      sql: "select * from st"
  commit:
    mode: auto
  group_id:
    strategy: default
  output:
    path: "./out"
    file_prefix: "sub"
    expected_rows: 10
)";
    YAML::Node node = YAML::Load(yaml);
    SubscribeDataConfig::Control ctrl = node.as<SubscribeDataConfig::Control>();
    assert(ctrl.data_format.format_type == "sql");
    assert(ctrl.data_channel.channel_type == "native");
    assert(ctrl.subscribe_control.log_path == "sub.log");
    assert(ctrl.subscribe_control.execution.consumer_concurrency == 1);
    assert(ctrl.subscribe_control.topics.size() == 1);
    assert(ctrl.subscribe_control.topics[0].name == "topic1");
    assert(ctrl.subscribe_control.commit.mode == "auto");
    assert(ctrl.subscribe_control.group_id.strategy == "default");
    assert(ctrl.subscribe_control.output.path == "./out");
    assert(ctrl.subscribe_control.output.file_prefix == "sub");
    assert(ctrl.subscribe_control.output.expected_rows == 10);
}

void test_CreateDatabaseConfig() {
    std::string yaml = R"(
checkpoint:
  enabled: true
  interval_sec: 1000
)";
    YAML::Node node = YAML::Load(yaml);
    CreateDatabaseConfig cdc = node.as<CreateDatabaseConfig>();
    assert(cdc.checkpoint_info.enabled == true);
    assert(cdc.checkpoint_info.interval_sec == 1000);
}

void test_CreateSuperTableConfig() {
    std::string yaml = R"(
schema:
  name: test_schema
  columns:
    - name: ts
      type: BIGINT
  tags:
    - name: tag1
      type: INT
  tbname:
    prefix: "tb"
    count: 1
)";
    YAML::Node node = YAML::Load(yaml);
    CreateSuperTableConfig cstc = node.as<CreateSuperTableConfig>();
    assert(cstc.schema.name == "test_schema");
    assert(cstc.schema.columns.size() == 1);
    assert(cstc.schema.tags.size() == 1);
    assert(cstc.schema.tbname.generator.prefix == "tb");
}

void test_CreateChildTableConfig() {
    std::string yaml = R"(
schema:
  name: st
  tbname:
    prefix: "tb"
    count: 10
batch:
  size: 100
  concurrency: 4
)";
    YAML::Node node = YAML::Load(yaml);
    CreateChildTableConfig cctc = node.as<CreateChildTableConfig>();
    assert(cctc.schema.name == "st");
    assert(cctc.schema.tbname.generator.count == 10);
    assert(cctc.batch.size == 100);
    assert(cctc.batch.concurrency == 4);
}

void test_InsertDataConfig_invalid_target() {
    std::string yaml = "target: invalid_target";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<InsertDataConfig>();
        assert(false && "Should throw for invalid target type");
    } catch (const std::runtime_error& e) {
        std::cout << "Caught expected exception: " << e.what() << std::endl;
        assert(std::string(e.what()).find("Invalid or unsupported target type") != std::string::npos);
    }
}

void test_InsertDataConfig_unknown_key() {
    std::string yaml = R"(
target: tdengine
unknown_key: "some_value"
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        node.as<InsertDataConfig>();
        assert(false && "Should throw for unknown key in tdengine/insert");
    } catch (const std::runtime_error& e) {
        std::cout << "Caught expected exception: " << e.what() << std::endl;
        assert(std::string(e.what()).find("Unknown configuration key in tdengine/insert") != std::string::npos);
    }
}

int main() {
    register_plugin_hooks();
    test_FromCSVConfig_tags_columns();
    test_FromCSVConfig_missing_file_path_tags();
    test_FromCSVConfig_missing_file_path_columns();
    test_FromCSVConfig_unknown_key_tags();
    test_FromCSVConfig_unknown_key_columns();
    test_GenerationConfig_DataDisorder_Interval();
    test_GenerationConfig_DataDisorder();
    test_GenerationConfig_base();
    test_GenerationConfig_data_cache();
    test_SchemaConfig();
    test_SchemaConfig_csv_ts_gen();

    test_DataChannel();
    test_DatabaseInfo();
    test_ColumnConfig_random();
    test_ColumnConfig_order();
    test_ColumnConfig_expression();
    test_ColumnConfig_min_max_length_varchar();
    test_ColumnConfig_min_max_length_nchar();
    test_ColumnConfig_only_min_length();
    test_ColumnConfig_only_max_length();
    test_ColumnConfig_min_max_length_default();
    test_ColumnConfig_max_length_exceeds_cap();
    test_ColumnConfig_min_length_exceeds_max_length();
    test_ColumnConfig_negative_min_length();
    test_ColumnConfig_negative_max_length();
    test_ColumnConfig_min_max_length_non_varlen();
    test_ColumnConfig_corpus_within_len();
    test_ColumnConfig_corpus_exceeds_len();
    test_ColumnConfig_corpus_exact_len();
    test_ColumnConfig_corpus_empty();
    test_ColumnConfig_null_none_ratio_valid();
    test_ColumnConfig_null_ratio_only();
    test_ColumnConfig_null_ratio_negative();
    test_ColumnConfig_none_ratio_negative();
    test_ColumnConfig_null_ratio_nan();
    test_ColumnConfig_none_ratio_inf();
    test_ColumnConfig_null_ratio_above_one();
    test_ColumnConfig_null_none_ratio_sum_exceeds();
    test_ColumnConfig_null_none_ratio_sum_equals_one();
    test_ColumnConfig_strip_backticks_plain();
    test_ColumnConfig_strip_backticks_unmatched();
    test_ColumnConfig_strip_backticks_none();

    test_TableNameConfig_generator();
    test_TableNameConfig_csv();
    test_TagsConfig_generator();
    test_TagsConfig_csv();
    test_SuperTableInfo();
    test_ChildTableInfo();
    test_CreateChildTableConfig_BatchConfig();
    test_TimestampGeneratorConfig();
    test_TimestampOriginalConfig();
    test_ColumnsConfig_generator();
    test_ColumnsConfig_csv();

    test_InsertDataConfig_FailureHandling();
    test_InsertDataConfig_FailureHandling_InvalidValue();
    test_InsertDataConfig_FailureHandling_UnknownKey();
    test_InsertDataConfig_Control_TimeInterval();

    test_QueryDataConfig_Source();
    test_QueryDataConfig_Control_QueryControl_Execution();
    test_QueryDataConfig_Control_QueryControl_Fixed();
    test_QueryDataConfig_Control_QueryControl_SuperTable();
    test_QueryDataConfig_Control_QueryControl();
    test_QueryDataConfig_Control();

    test_SubscribeDataConfig_Source();
    test_SubscribeDataConfig_Control_SubscribeControl_Execution();
    test_SubscribeDataConfig_Control_SubscribeControl_Topic();
    test_SubscribeDataConfig_Control_SubscribeControl_Commit();
    test_SubscribeDataConfig_Control_SubscribeControl_GroupID();
    test_SubscribeDataConfig_Control_SubscribeControl_Output();
    test_SubscribeDataConfig_Control_SubscribeControl();
    test_SubscribeDataConfig_Control();

    test_CreateDatabaseConfig();
    test_CreateSuperTableConfig();
    test_CreateChildTableConfig();

    test_InsertDataConfig_invalid_target();
    test_InsertDataConfig_unknown_key();

    std::cout << "All ConfigParser YAML tests passed!" << std::endl;
    return 0;
}