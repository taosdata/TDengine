#include "SchemaConfig.hpp"
#include <iostream>
#include <cassert>
#include <string>
#include <stdexcept>

void test_apply_prepends_timestamp_when_columns_empty() {
    SchemaConfig cfg;
    cfg.columns.clear();
    cfg.apply();
    assert(!cfg.columns.empty());
    (void)cfg;
    assert(cfg.columns[0].type_tag == ColumnTypeTag::BIGINT);
    assert(cfg.columns[0].type == "TIMESTAMP");
    std::cout << "  PASSED: test_apply_prepends_timestamp_when_columns_empty\n";
}

void test_apply_prepends_timestamp_when_first_col_not_bigint() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("value", "DOUBLE"));
    cfg.apply();
    assert(cfg.columns.size() == 2);
    (void)cfg;
    assert(cfg.columns[0].type_tag == ColumnTypeTag::BIGINT);
    assert(cfg.columns[0].type == "TIMESTAMP");
    assert(cfg.columns[1].name == "value");
    std::cout << "  PASSED: test_apply_prepends_timestamp_when_first_col_not_bigint\n";
}

void test_apply_no_prepend_when_first_col_is_bigint() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.columns.push_back(ColumnConfig("value", "DOUBLE"));
    cfg.apply();
    assert(cfg.columns.size() == 2);
    (void)cfg;
    assert(cfg.columns[0].name == "ts");
    assert(cfg.columns[0].type_tag == ColumnTypeTag::BIGINT);
    std::cout << "  PASSED: test_apply_no_prepend_when_first_col_is_bigint\n";
}

void test_apply_prepends_timestamp_when_first_col_is_int() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("id", "INT"));
    cfg.columns.push_back(ColumnConfig("value", "DOUBLE"));
    cfg.apply();
    assert(cfg.columns.size() == 3);
    (void)cfg;
    assert(cfg.columns[0].type_tag == ColumnTypeTag::BIGINT);
    assert(cfg.columns[0].type == "TIMESTAMP");
    assert(cfg.columns[1].name == "id");
    assert(cfg.columns[2].name == "value");
    std::cout << "  PASSED: test_apply_prepends_timestamp_when_first_col_is_int\n";
}

// ============================================================
// 2. from_csv.tags tbname_index setup (lines 27-34)
// ============================================================

void test_apply_csv_tags_tbname_index_sets_tbname_config() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.enabled = true;
    cfg.from_csv.tags.enabled = true;
    cfg.from_csv.tags.file_path = "tags.csv";
    cfg.from_csv.tags.has_header = true;
    cfg.from_csv.tags.delimiter = "|";
    cfg.from_csv.tags.tbname_index = 2;

    cfg.apply();

    assert(cfg.tbname.enabled == true);
    (void)cfg;
    assert(cfg.tbname.source_type == "csv");
    assert(cfg.tbname.csv.file_path == "tags.csv");
    assert(cfg.tbname.csv.has_header == true);
    assert(cfg.tbname.csv.delimiter == "|");
    assert(cfg.tbname.csv.tbname_index == 2);
    std::cout << "  PASSED: test_apply_csv_tags_tbname_index_sets_tbname_config\n";
}

void test_apply_csv_tags_tbname_index_negative_no_setup() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.enabled = true;
    cfg.from_csv.tags.enabled = true;
    cfg.from_csv.tags.file_path = "tags.csv";
    cfg.from_csv.tags.tbname_index = -1;  // negative

    cfg.apply();

    assert(cfg.tbname.enabled == false);
    (void)cfg;
    assert(cfg.tbname.source_type == "generator");
    std::cout << "  PASSED: test_apply_csv_tags_tbname_index_negative_no_setup\n";
}

void test_apply_csv_not_enabled_no_tbname_setup() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.enabled = false;
    cfg.from_csv.tags.enabled = true;
    cfg.from_csv.tags.tbname_index = 2;

    cfg.apply();

    // from_csv.enabled is false, so tbname stays default
    assert(cfg.tbname.enabled == false);
    (void)cfg;
    assert(cfg.tbname.source_type == "generator");
    std::cout << "  PASSED: test_apply_csv_not_enabled_no_tbname_setup\n";
}

void test_apply_tags_not_enabled_no_tbname_setup() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.enabled = true;
    cfg.from_csv.tags.enabled = false;
    cfg.from_csv.tags.tbname_index = 2;

    cfg.apply();

    assert(cfg.tbname.enabled == false);
    (void)cfg;
    assert(cfg.tbname.source_type == "generator");
    std::cout << "  PASSED: test_apply_tags_not_enabled_no_tbname_setup\n";
}

// ============================================================
// 3. tags_cfg source_type routing (lines 36-43)
// ============================================================

void test_apply_tags_cfg_csv_path() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.tags.push_back(ColumnConfig("tag1", "INT"));
    cfg.from_csv.tags.enabled = true;
    cfg.from_csv.tags.file_path = "tags.csv";
    cfg.from_csv.tags.delimiter = ";";

    cfg.apply();

    assert(cfg.tags_cfg.source_type == "csv");
    (void)cfg;
    assert(cfg.tags_cfg.csv.file_path == "tags.csv");
    assert(cfg.tags_cfg.csv.delimiter == ";");
    assert(cfg.tags_cfg.csv.schema.size() == 1);
    assert(cfg.tags_cfg.csv.schema[0].name == "tag1");
    std::cout << "  PASSED: test_apply_tags_cfg_csv_path\n";
}

void test_apply_tags_cfg_generator_path() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.tags.push_back(ColumnConfig("tag1", "INT"));
    cfg.tags.push_back(ColumnConfig("tag2", "VARCHAR(64)"));
    cfg.from_csv.tags.enabled = false;

    cfg.apply();

    assert(cfg.tags_cfg.source_type == "generator");
    (void)cfg;
    assert(cfg.tags_cfg.generator.schema.size() == 2);
    assert(cfg.tags_cfg.generator.schema[0].name == "tag1");
    assert(cfg.tags_cfg.generator.schema[1].name == "tag2");
    std::cout << "  PASSED: test_apply_tags_cfg_generator_path\n";
}

// ============================================================
// 4. columns_cfg source_type routing (lines 45-70)
// ============================================================

void test_apply_columns_cfg_generator_path() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.columns.push_back(ColumnConfig("v1", "DOUBLE"));
    cfg.columns.push_back(ColumnConfig("v2", "INT"));
    cfg.from_csv.columns.enabled = false;

    cfg.apply();

    assert(cfg.columns_cfg.source_type == "generator");
    (void)cfg;
    assert(cfg.columns_cfg.generator.schema.size() == 2);
    assert(cfg.columns_cfg.generator.schema[0].name == "v1");
    assert(cfg.columns_cfg.generator.schema[1].name == "v2");
    // timestamp_config comes from columns[0].ts.generator
    std::cout << "  PASSED: test_apply_columns_cfg_generator_path\n";
}

void test_apply_columns_cfg_csv_path_no_csv_ts() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.columns.push_back(ColumnConfig("v1", "DOUBLE"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.has_header = false;
    cfg.from_csv.columns.delimiter = "|";
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;

    cfg.apply();

    assert(cfg.columns_cfg.source_type == "csv");
    (void)cfg;
    assert(cfg.columns_cfg.csv.file_path == "data.csv");
    assert(cfg.columns_cfg.csv.has_header == false);
    assert(cfg.columns_cfg.csv.delimiter == "|");
    // Schema = columns[1..end]
    assert(cfg.columns_cfg.csv.schema.size() == 1);
    assert(cfg.columns_cfg.csv.schema[0].name == "v1");
    // timestamp_strategy comes from columns[0].ts (the fallback path)
    assert(cfg.columns_cfg.csv.timestamp_strategy.strategy_type == cfg.columns[0].ts.strategy_type);
    std::cout << "  PASSED: test_apply_columns_cfg_csv_path_no_csv_ts\n";
}

void test_apply_columns_cfg_csv_path_with_csv_ts_has_precision() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.columns.push_back(ColumnConfig("v1", "DOUBLE"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = true;
    cfg.from_csv.columns.timestamp_strategy.csv.timestamp_precision = "us";

    cfg.apply();

    assert(cfg.columns_cfg.source_type == "csv");
    (void)cfg;
    // csv ts is used, and precision was already set → no inheritance
    assert(cfg.columns_cfg.csv.timestamp_strategy.csv.timestamp_precision.has_value());
    assert(cfg.columns_cfg.csv.timestamp_strategy.csv.timestamp_precision.value() == "us");
    std::cout << "  PASSED: test_apply_columns_cfg_csv_path_with_csv_ts_has_precision\n";
}

void test_apply_columns_cfg_csv_ts_precision_inherited() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.columns[0].ts.generator.timestamp_precision = "ns";
    cfg.columns.push_back(ColumnConfig("v1", "DOUBLE"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = true;
    cfg.from_csv.columns.timestamp_strategy.csv.timestamp_precision = std::nullopt;

    cfg.apply();

    assert(cfg.columns_cfg.source_type == "csv");
    (void)cfg;
    // Precision should be inherited from columns[0].ts.generator.timestamp_precision
    assert(cfg.from_csv.columns.timestamp_strategy.csv.timestamp_precision.has_value());
    assert(cfg.from_csv.columns.timestamp_strategy.csv.timestamp_precision.value() == "ns");
    std::cout << "  PASSED: test_apply_columns_cfg_csv_ts_precision_inherited\n";
}

void test_apply_columns_cfg_csv_ts_precision_inherited_with_offset() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.columns[0].ts.generator.timestamp_precision = "ms";
    cfg.columns.push_back(ColumnConfig("v1", "DOUBLE"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = true;
    cfg.from_csv.columns.timestamp_strategy.csv.timestamp_precision = std::nullopt;

    TimestampCSVConfig::OffsetConfig oc;
    oc.offset_type = "absolute";
    oc.value = static_cast<int64_t>(1000000);
    cfg.from_csv.columns.timestamp_strategy.csv.offset_config = oc;

    cfg.apply();

    // Precision inherited + offset_config.parse_offset should be called
    assert(cfg.from_csv.columns.timestamp_strategy.csv.timestamp_precision.has_value());
    (void)cfg;
    assert(cfg.from_csv.columns.timestamp_strategy.csv.timestamp_precision.value() == "ms");
    assert(cfg.from_csv.columns.timestamp_strategy.csv.offset_config.has_value());
    assert(cfg.from_csv.columns.timestamp_strategy.csv.offset_config->parsed == true);
    std::cout << "  PASSED: test_apply_columns_cfg_csv_ts_precision_inherited_with_offset\n";
}

void test_apply_columns_cfg_generator_ts_config() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.columns[0].ts.generator.timestamp_precision = "us";
    cfg.columns[0].ts.generator.start_timestamp = static_cast<int64_t>(1000);
    cfg.columns[0].ts.generator.timestamp_step = static_cast<int64_t>(500);
    cfg.columns.push_back(ColumnConfig("v1", "INT"));
    cfg.from_csv.columns.enabled = false;

    cfg.apply();

    assert(cfg.columns_cfg.source_type == "generator");
    (void)cfg;
    assert(cfg.columns_cfg.generator.timestamp_strategy.timestamp_config.timestamp_precision == "us");
    assert(std::get<Timestamp>(cfg.columns_cfg.generator.timestamp_strategy.timestamp_config.start_timestamp) == 1000);
    assert(std::get<Timestamp>(cfg.columns_cfg.generator.timestamp_strategy.timestamp_config.timestamp_step) == 500);
    std::cout << "  PASSED: test_apply_columns_cfg_generator_ts_config\n";
}

void test_apply_columns_cfg_csv_schema_excludes_first_column() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.columns.push_back(ColumnConfig("a", "INT"));
    cfg.columns.push_back(ColumnConfig("b", "DOUBLE"));
    cfg.columns.push_back(ColumnConfig("c", "VARCHAR(32)"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;

    cfg.apply();

    // Schema should be columns[1..end] = {a, b, c}
    assert(cfg.columns_cfg.csv.schema.size() == 3);
    (void)cfg;
    assert(cfg.columns_cfg.csv.schema[0].name == "a");
    assert(cfg.columns_cfg.csv.schema[1].name == "b");
    assert(cfg.columns_cfg.csv.schema[2].name == "c");
    std::cout << "  PASSED: test_apply_columns_cfg_csv_schema_excludes_first_column\n";
}

// ============================================================
// 5. data_cache validation (lines 72-83)
// ============================================================

void test_apply_data_cache_disabled_by_tbname_index() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = 0;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.data_cache.enabled = true;

    cfg.apply();

    assert(cfg.generation.data_cache.enabled == false);
    (void)cfg;
    std::cout << "  PASSED: test_apply_data_cache_disabled_by_tbname_index\n";
}

void test_apply_data_cache_disabled_by_tables_reuse_data_false() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = -1;  // not by tbname_index
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.data_cache.enabled = true;
    cfg.generation.tables_reuse_data = false;

    cfg.apply();

    assert(cfg.generation.data_cache.enabled == false);
    (void)cfg;
    std::cout << "  PASSED: test_apply_data_cache_disabled_by_tables_reuse_data_false\n";
}

void test_apply_data_cache_stays_enabled_when_no_csv_columns() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = false;
    cfg.generation.data_cache.enabled = true;

    cfg.apply();

    assert(cfg.generation.data_cache.enabled == true);
    (void)cfg;
    std::cout << "  PASSED: test_apply_data_cache_stays_enabled_when_no_csv_columns\n";
}

void test_apply_data_cache_stays_enabled_csv_no_tbname_reuse_true() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = -1;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.data_cache.enabled = true;
    cfg.generation.tables_reuse_data = true;

    cfg.apply();

    assert(cfg.generation.data_cache.enabled == true);
    (void)cfg;
    std::cout << "  PASSED: test_apply_data_cache_stays_enabled_csv_no_tbname_reuse_true\n";
}

void test_apply_data_cache_already_disabled_stays_disabled() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = 0;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.data_cache.enabled = false;

    cfg.apply();

    assert(cfg.generation.data_cache.enabled == false);
    (void)cfg;
    std::cout << "  PASSED: test_apply_data_cache_already_disabled_stays_disabled\n";
}

// ============================================================
// 6. tables_reuse_data validation (lines 85-92)
// ============================================================

void test_apply_tables_reuse_data_disabled_by_tbname_index() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = 0;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.tables_reuse_data = true;
    // Also set data_cache.enabled to false so we isolate this path
    cfg.generation.data_cache.enabled = false;

    cfg.apply();

    assert(cfg.generation.tables_reuse_data == false);
    (void)cfg;
    std::cout << "  PASSED: test_apply_tables_reuse_data_disabled_by_tbname_index\n";
}

void test_apply_tables_reuse_data_stays_true_negative_tbname_index() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = -1;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.tables_reuse_data = true;

    cfg.apply();

    assert(cfg.generation.tables_reuse_data == true);
    (void)cfg;
    std::cout << "  PASSED: test_apply_tables_reuse_data_stays_true_negative_tbname_index\n";
}

void test_apply_tables_reuse_data_stays_true_no_csv_columns() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = false;
    cfg.generation.tables_reuse_data = true;

    cfg.apply();

    assert(cfg.generation.tables_reuse_data == true);
    (void)cfg;
    std::cout << "  PASSED: test_apply_tables_reuse_data_stays_true_no_csv_columns\n";
}

void test_apply_tables_reuse_data_already_false_stays_false() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = 0;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.tables_reuse_data = false;
    cfg.generation.data_cache.enabled = false;

    cfg.apply();

    assert(cfg.generation.tables_reuse_data == false);
    (void)cfg;
    std::cout << "  PASSED: test_apply_tables_reuse_data_already_false_stays_false\n";
}

// ============================================================
// 7. Streaming mode validations (lines 95-104)
// ============================================================

void test_apply_streaming_throws_when_tbname_index_positive() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.loading_mode = "streaming";
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = 0;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.data_cache.enabled = false;
    cfg.generation.tables_reuse_data = false;

    try {
        cfg.apply();
        assert(false && "Should throw for streaming + tbname_index >= 0");
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        (void)msg;
        assert(msg.find("streaming") != std::string::npos);
        assert(msg.find("tbname_index") != std::string::npos);
    }
    std::cout << "  PASSED: test_apply_streaming_throws_when_tbname_index_positive\n";
}

void test_apply_streaming_throws_when_data_disorder_enabled() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.loading_mode = "streaming";
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = -1;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.data_disorder.enabled = true;

    try {
        cfg.apply();
        assert(false && "Should throw for streaming + data_disorder");
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        (void)msg;
        assert(msg.find("streaming") != std::string::npos);
        assert(msg.find("data_disorder") != std::string::npos);
    }
    std::cout << "  PASSED: test_apply_streaming_throws_when_data_disorder_enabled\n";
}

void test_apply_streaming_ok_no_tbname_no_disorder() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.loading_mode = "streaming";
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = -1;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.data_disorder.enabled = false;

    // Should not throw
    cfg.apply();
    assert(cfg.from_csv.columns.loading_mode == "streaming");
    (void)cfg;
    std::cout << "  PASSED: test_apply_streaming_ok_no_tbname_no_disorder\n";
}

void test_apply_preload_mode_with_tbname_index_no_throw() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.loading_mode = "preload";
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = 0;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.data_cache.enabled = false;
    cfg.generation.tables_reuse_data = false;

    // Should not throw (only streaming mode throws)
    cfg.apply();
    assert(cfg.from_csv.columns.loading_mode == "preload");
    (void)cfg;
    std::cout << "  PASSED: test_apply_preload_mode_with_tbname_index_no_throw\n";
}

void test_apply_streaming_no_csv_columns_no_throw() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = false;
    cfg.from_csv.columns.loading_mode = "streaming";
    cfg.generation.data_disorder.enabled = true;

    // csv columns not enabled → streaming validation block not entered
    cfg.apply();
    assert(cfg.from_csv.columns.loading_mode == "streaming");
    (void)cfg;
    std::cout << "  PASSED: test_apply_streaming_no_csv_columns_no_throw\n";
}

// ============================================================
// 8. Combined / interaction tests
// ============================================================

void test_apply_data_cache_and_reuse_both_disabled_by_tbname_index() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.loading_mode = "preload";
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = 0;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.data_cache.enabled = true;
    cfg.generation.tables_reuse_data = true;

    cfg.apply();

    // Both should be disabled by tbname_index >= 0
    assert(cfg.generation.data_cache.enabled == false);
    (void)cfg;
    assert(cfg.generation.tables_reuse_data == false);
    std::cout << "  PASSED: test_apply_data_cache_and_reuse_both_disabled_by_tbname_index\n";
}

void test_apply_full_csv_tags_and_columns_config() {
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.columns[0].ts.generator.timestamp_precision = "ms";
    cfg.columns.push_back(ColumnConfig("v1", "DOUBLE"));
    cfg.columns.push_back(ColumnConfig("v2", "INT"));

    cfg.tags.push_back(ColumnConfig("t1", "INT"));

    cfg.from_csv.enabled = true;
    cfg.from_csv.tags.enabled = true;
    cfg.from_csv.tags.file_path = "tags.csv";
    cfg.from_csv.tags.has_header = true;
    cfg.from_csv.tags.delimiter = ",";
    cfg.from_csv.tags.tbname_index = 0;

    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.has_header = false;
    cfg.from_csv.columns.tbname_index = -1;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;

    cfg.generation.data_cache.enabled = true;
    cfg.generation.tables_reuse_data = true;

    cfg.apply();

    // tbname should be set from tags CSV
    assert(cfg.tbname.enabled == true);
    (void)cfg;
    assert(cfg.tbname.source_type == "csv");
    assert(cfg.tbname.csv.file_path == "tags.csv");
    assert(cfg.tbname.csv.tbname_index == 0);

    // tags_cfg should be CSV
    assert(cfg.tags_cfg.source_type == "csv");
    assert(cfg.tags_cfg.csv.schema.size() == 1);
    assert(cfg.tags_cfg.csv.schema[0].name == "t1");

    // columns_cfg should be CSV with fallback ts
    assert(cfg.columns_cfg.source_type == "csv");
    assert(cfg.columns_cfg.csv.schema.size() == 2);
    assert(cfg.columns_cfg.csv.schema[0].name == "v1");
    assert(cfg.columns_cfg.csv.schema[1].name == "v2");

    // data_cache and reuse stay enabled (tbname_index = -1 for columns)
    assert(cfg.generation.data_cache.enabled == true);
    assert(cfg.generation.tables_reuse_data == true);
    std::cout << "  PASSED: test_apply_full_csv_tags_and_columns_config\n";
}

void test_apply_default_config() {
    SchemaConfig cfg;
    cfg.apply();

    // Default: columns empty → ts prepended
    assert(cfg.columns.size() == 1);
    (void)cfg;
    assert(cfg.columns[0].type_tag == ColumnTypeTag::BIGINT);
    assert(cfg.columns[0].type == "TIMESTAMP");

    // tags_cfg defaults to generator (from_csv.tags.enabled = false)
    assert(cfg.tags_cfg.source_type == "generator");

    // columns_cfg defaults to generator
    assert(cfg.columns_cfg.source_type == "generator");

    // tbname stays default
    assert(cfg.tbname.enabled == false);
    std::cout << "  PASSED: test_apply_default_config\n";
}

void test_apply_columns_prepend_then_csv_schema() {
    // Test that when columns start with non-BIGINT, TIMESTAMP is prepended,
    // and then the CSV schema correctly excludes the prepended TIMESTAMP (idx 0).
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("v1", "DOUBLE"));
    cfg.columns.push_back(ColumnConfig("v2", "INT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;

    cfg.apply();

    // TIMESTAMP prepended
    assert(cfg.columns.size() == 3);
    (void)cfg;
    assert(cfg.columns[0].type_tag == ColumnTypeTag::BIGINT);
    assert(cfg.columns[0].type == "TIMESTAMP");
    assert(cfg.columns[1].name == "v1");
    assert(cfg.columns[2].name == "v2");

    // CSV schema = columns[1..end] = {v1, v2}
    assert(cfg.columns_cfg.csv.schema.size() == 2);
    assert(cfg.columns_cfg.csv.schema[0].name == "v1");
    assert(cfg.columns_cfg.csv.schema[1].name == "v2");
    std::cout << "  PASSED: test_apply_columns_prepend_then_csv_schema\n";
}

void test_apply_streaming_tbname_index_zero_throws() {
    // Edge case: tbname_index exactly 0 is still >= 0
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.columns.enabled = true;
    cfg.from_csv.columns.loading_mode = "streaming";
    cfg.from_csv.columns.file_path = "data.csv";
    cfg.from_csv.columns.tbname_index = 0;
    cfg.from_csv.columns.timestamp_strategy.csv.enabled = false;
    cfg.generation.data_cache.enabled = false;
    cfg.generation.tables_reuse_data = false;

    try {
        cfg.apply();
        assert(false && "Should throw");
    } catch (const std::runtime_error&) {
        // Expected
    }
    std::cout << "  PASSED: test_apply_streaming_tbname_index_zero_throws\n";
}

void test_apply_csv_tags_tbname_zero_sets_config() {
    // tbname_index = 0 is >= 0, should trigger tbname setup
    SchemaConfig cfg;
    cfg.columns.push_back(ColumnConfig("ts", "BIGINT"));
    cfg.from_csv.enabled = true;
    cfg.from_csv.tags.enabled = true;
    cfg.from_csv.tags.file_path = "tags.csv";
    cfg.from_csv.tags.has_header = false;
    cfg.from_csv.tags.delimiter = "\t";
    cfg.from_csv.tags.tbname_index = 0;

    cfg.apply();

    assert(cfg.tbname.enabled == true);
    (void)cfg;
    assert(cfg.tbname.source_type == "csv");
    assert(cfg.tbname.csv.file_path == "tags.csv");
    assert(cfg.tbname.csv.has_header == false);
    assert(cfg.tbname.csv.delimiter == "\t");
    assert(cfg.tbname.csv.tbname_index == 0);
    std::cout << "  PASSED: test_apply_csv_tags_tbname_zero_sets_config\n";
}

// ============================================================
// main
// ============================================================

int main() {
    std::cout << "=== TestSchemaConfig ===" << std::endl;

    // 1. Column prepend
    test_apply_prepends_timestamp_when_columns_empty();
    test_apply_prepends_timestamp_when_first_col_not_bigint();
    test_apply_no_prepend_when_first_col_is_bigint();
    test_apply_prepends_timestamp_when_first_col_is_int();

    // 2. from_csv.tags tbname_index setup
    test_apply_csv_tags_tbname_index_sets_tbname_config();
    test_apply_csv_tags_tbname_index_negative_no_setup();
    test_apply_csv_not_enabled_no_tbname_setup();
    test_apply_tags_not_enabled_no_tbname_setup();

    // 3. tags_cfg routing
    test_apply_tags_cfg_csv_path();
    test_apply_tags_cfg_generator_path();

    // 4. columns_cfg routing
    test_apply_columns_cfg_generator_path();
    test_apply_columns_cfg_csv_path_no_csv_ts();
    test_apply_columns_cfg_csv_path_with_csv_ts_has_precision();
    test_apply_columns_cfg_csv_ts_precision_inherited();
    test_apply_columns_cfg_csv_ts_precision_inherited_with_offset();
    test_apply_columns_cfg_generator_ts_config();
    test_apply_columns_cfg_csv_schema_excludes_first_column();

    // 5. data_cache validation
    test_apply_data_cache_disabled_by_tbname_index();
    test_apply_data_cache_disabled_by_tables_reuse_data_false();
    test_apply_data_cache_stays_enabled_when_no_csv_columns();
    test_apply_data_cache_stays_enabled_csv_no_tbname_reuse_true();
    test_apply_data_cache_already_disabled_stays_disabled();

    // 6. tables_reuse_data validation
    test_apply_tables_reuse_data_disabled_by_tbname_index();
    test_apply_tables_reuse_data_stays_true_negative_tbname_index();
    test_apply_tables_reuse_data_stays_true_no_csv_columns();
    test_apply_tables_reuse_data_already_false_stays_false();

    // 7. Streaming mode validations
    test_apply_streaming_throws_when_tbname_index_positive();
    test_apply_streaming_throws_when_data_disorder_enabled();
    test_apply_streaming_ok_no_tbname_no_disorder();
    test_apply_preload_mode_with_tbname_index_no_throw();
    test_apply_streaming_no_csv_columns_no_throw();

    // 8. Combined / interaction tests
    test_apply_data_cache_and_reuse_both_disabled_by_tbname_index();
    test_apply_full_csv_tags_and_columns_config();
    test_apply_default_config();
    test_apply_columns_prepend_then_csv_schema();
    test_apply_streaming_tbname_index_zero_throws();
    test_apply_csv_tags_tbname_zero_sets_config();

    std::cout << "All TestSchemaConfig tests passed! ===" << std::endl;
    return 0;
}
