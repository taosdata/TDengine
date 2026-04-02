#include "RowDataGenerator.hpp"
#include "CSVDataManager.hpp"
#include "StreamingCSVRowSource.hpp"
#include <cassert>
#include <iostream>
#include <fstream>
#include <cstdio>


// ============================================================
// Helpers
// ============================================================

static void create_test_file(const std::string& filename, const std::string& content) {
    std::ofstream f(filename);
    f << content;
    f.close();
}

static void remove_test_file(const std::string& filename) {
    std::remove(filename.c_str());
}

// Helper to build a minimal InsertDataConfig for streaming CSV mode
static InsertDataConfig make_streaming_config(
    const std::string& file_path,
    const ColumnConfigVector& columns,
    int tbname_index = -1,
    bool repeat_read = false,
    int64_t rows_per_table = 100)
{
    ColumnsConfig columns_config;
    columns_config.source_type = "csv";
    columns_config.csv.loading_mode = "streaming";
    columns_config.csv.file_path = file_path;
    columns_config.csv.has_header = true;
    columns_config.csv.delimiter = ",";
    columns_config.csv.tbname_index = tbname_index;
    columns_config.csv.repeat_read = repeat_read;

    // Default: CSV timestamp at index 0
    columns_config.csv.timestamp_strategy.strategy_type = "csv";
    columns_config.csv.timestamp_strategy.csv.enabled = true;
    columns_config.csv.timestamp_strategy.csv.timestamp_index = 0;
    columns_config.csv.timestamp_strategy.csv.timestamp_precision = "ms";

    InsertDataConfig config;
    config.schema.columns = columns;
    config.schema.generation.rows_per_table = rows_per_table;
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = columns;

    return config;
}

// Helper for preload config
static InsertDataConfig make_preload_config(
    const std::string& file_path,
    const ColumnConfigVector& columns,
    int tbname_index = -1,
    bool repeat_read = false,
    int64_t rows_per_table = 100)
{
    auto config = make_streaming_config(file_path, columns, tbname_index, repeat_read, rows_per_table);
    config.schema.columns_cfg.csv.loading_mode = "preload";
    return config;
}


// ============================================================
// Streaming CSV mode — basic
// ============================================================

void test_streaming_csv_basic() {
    const std::string filename = "rdg_streaming_basic.csv";
    create_test_file(filename,
        "ts,name,age\n"
        "1000,Alice,30\n"
        "2000,Bob,25\n"
        "3000,Charlie,35\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"},
    };
    auto config = make_streaming_config(filename, col_configs, -1, false, 10);
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    RowDataGenerator generator("any_table", config, instances);

    auto r1 = generator.next_row();
    assert(r1.has_value());
    assert(r1->timestamp == 1000);
    assert(std::get<std::string>(r1->columns[0]) == "Alice");
    assert(std::get<int32_t>(r1->columns[1]) == 30);

    auto r2 = generator.next_row();
    assert(r2.has_value());
    assert(r2->timestamp == 2000);
    assert(std::get<std::string>(r2->columns[0]) == "Bob");

    auto r3 = generator.next_row();
    assert(r3.has_value());
    assert(r3->timestamp == 3000);

    // File exhausted, no repeat
    auto r4 = generator.next_row();
    assert(!r4.has_value());
    assert(!generator.has_more());
    assert(generator.generated_rows() == 3);

    remove_test_file(filename);
    std::cout << "test_streaming_csv_basic passed\n";
}


// ============================================================
// Streaming CSV mode — repeat read
// ============================================================

void test_streaming_csv_repeat_read() {
    const std::string filename = "rdg_streaming_repeat.csv";
    create_test_file(filename,
        "ts,value\n"
        "1000,10\n"
        "2000,20\n");

    ColumnConfigVector col_configs = {
        {"value", "int"},
    };
    auto config = make_streaming_config(filename, col_configs, -1, /*repeat_read=*/true, 5);
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    RowDataGenerator generator("any_table", config, instances);

    // Should produce 5 rows (cycling through 2-row file)
    std::vector<int32_t> values;
    while (auto row = generator.next_row()) {
        values.push_back(std::get<int32_t>(row->columns[0]));
    }

    (void)values;
    assert(values.size() == 5 && "Should produce rows_per_table rows with repeat");
    // Values should cycle: 10, 20, 10, 20, 10
    assert(values[0] == 10);
    assert(values[1] == 20);
    assert(values[2] == 10);
    assert(values[3] == 20);
    assert(values[4] == 10);

    remove_test_file(filename);
    std::cout << "test_streaming_csv_repeat_read passed\n";
}


// ============================================================
// Streaming CSV mode — rows_per_table limits output
// ============================================================

void test_streaming_csv_rows_per_table_limit() {
    const std::string filename = "rdg_streaming_limit.csv";
    create_test_file(filename,
        "ts,value\n"
        "1000,1\n"
        "2000,2\n"
        "3000,3\n"
        "4000,4\n"
        "5000,5\n");

    ColumnConfigVector col_configs = {
        {"value", "int"},
    };
    // Only request 3 rows even though file has 5
    auto config = make_streaming_config(filename, col_configs, -1, false, 3);
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    RowDataGenerator generator("any_table", config, instances);

    int count = 0;
    while (generator.next_row()) {
        count++;
    }
    (void)count;
    assert(count == 3 && "Should stop at rows_per_table");
    assert(generator.generated_rows() == 3);

    remove_test_file(filename);
    std::cout << "test_streaming_csv_rows_per_table_limit passed\n";
}


// ============================================================
// Streaming CSV mode — with generator timestamp
// ============================================================

void test_streaming_csv_with_generator_timestamp() {
    const std::string filename = "rdg_streaming_gen_ts.csv";
    create_test_file(filename,
        "name,age\n"
        "Alice,30\n"
        "Bob,25\n"
        "Charlie,35\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"},
    };
    auto config = make_streaming_config(filename, col_configs, -1, false, 10);

    // Override timestamp strategy to generator mode
    config.schema.columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.start_timestamp = static_cast<int64_t>(5000);
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision = "ms";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_step = static_cast<int64_t>(100);

    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    RowDataGenerator generator("any_table", config, instances);

    auto r1 = generator.next_row();
    assert(r1.has_value());
    assert(r1->timestamp == 5000);
    assert(std::get<std::string>(r1->columns[0]) == "Alice");

    auto r2 = generator.next_row();
    assert(r2.has_value());
    assert(r2->timestamp == 5100);
    assert(std::get<std::string>(r2->columns[0]) == "Bob");

    auto r3 = generator.next_row();
    assert(r3.has_value());
    assert(r3->timestamp == 5200);

    remove_test_file(filename);
    std::cout << "test_streaming_csv_with_generator_timestamp passed\n";
}


// ============================================================
// Streaming CSV with external shared source
// ============================================================

void test_streaming_csv_external_source() {
    const std::string filename = "rdg_streaming_ext.csv";
    create_test_file(filename,
        "ts,value\n"
        "100,AAA\n"
        "200,BBB\n"
        "300,CCC\n");

    ColumnConfigVector col_configs = {
        {"value", "varchar(10)"},
    };
    auto config = make_streaming_config(filename, col_configs, -1, false, 10);
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    // Create an external streaming source
    auto external_source = std::make_shared<StreamingCSVRowSource>(
        std::vector<std::string>{filename},
        /*has_header=*/true,
        /*delimiter=*/',',
        instances,
        config.schema.columns_cfg.csv.timestamp_strategy,
        "ms",
        "ms",
        false
    );

    // Two generators share the same source (as done in TableDataManager)
    RowDataGenerator gen1("table_a", config, instances, false, external_source);
    RowDataGenerator gen2("table_b", config, instances, false, external_source);

    // gen1 reads first row from the shared source
    auto r1 = gen1.next_row();
    assert(r1.has_value());
    assert(r1->timestamp == 100);
    assert(std::get<std::string>(r1->columns[0]) == "AAA");

    // gen2 reads the next row from the same shared source
    auto r2 = gen2.next_row();
    assert(r2.has_value());
    assert(r2->timestamp == 200);
    assert(std::get<std::string>(r2->columns[0]) == "BBB");

    // gen1 reads the third row
    auto r3 = gen1.next_row();
    assert(r3.has_value());
    assert(r3->timestamp == 300);

    // No more data in the shared source
    auto r4 = gen2.next_row();
    assert(!r4.has_value());

    remove_test_file(filename);
    std::cout << "test_streaming_csv_external_source passed\n";
}


// ============================================================
// Streaming CSV — empty file path throws
// ============================================================

void test_streaming_csv_empty_file_path() {
    ColumnConfigVector col_configs = {
        {"value", "int"},
    };
    auto config = make_streaming_config("", col_configs, -1, false, 10);
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    try {
        RowDataGenerator generator("any_table", config, instances);
        assert(false && "Should throw for empty CSV file path");
    } catch (const std::exception& e) {
        (void)e;
        std::cout << "test_streaming_csv_empty_file_path passed\n";
    }
}


// ============================================================
// Preload CSV — with generator timestamp (non-cache)
// ============================================================

void test_preload_csv_with_generator_timestamp() {
    CSVDataManager::reset();
    const std::string filename = "rdg_preload_gen_ts.csv";
    create_test_file(filename,
        "name,age\n"
        "Alice,30\n"
        "Bob,25\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"},
    };
    auto config = make_preload_config(filename, col_configs, -1, false, 10);

    // Override to generator timestamp
    config.schema.columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.start_timestamp = static_cast<int64_t>(1000);
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision = "ms";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_step = static_cast<int64_t>(50);

    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    RowDataGenerator generator("any_table", config, instances);

    auto r1 = generator.next_row();
    assert(r1.has_value());
    assert(r1->timestamp == 1000);
    assert(std::get<std::string>(r1->columns[0]) == "Alice");

    auto r2 = generator.next_row();
    assert(r2.has_value());
    assert(r2->timestamp == 1050);
    assert(std::get<std::string>(r2->columns[0]) == "Bob");

    // No more data (2 rows in file, no repeat)
    auto r3 = generator.next_row();
    assert(!r3.has_value());

    remove_test_file(filename);
    std::cout << "test_preload_csv_with_generator_timestamp passed\n";
}


// ============================================================
// Preload CSV — repeat read
// ============================================================

void test_preload_csv_repeat_read() {
    CSVDataManager::reset();
    const std::string filename = "rdg_preload_repeat.csv";
    create_test_file(filename,
        "ts,value\n"
        "1000,10\n"
        "2000,20\n");

    ColumnConfigVector col_configs = {
        {"value", "int"},
    };
    auto config = make_preload_config(filename, col_configs, -1, /*repeat_read=*/true, 5);
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    RowDataGenerator generator("any_table", config, instances);

    int count = 0;
    while (auto row = generator.next_row()) {
        count++;
    }
    (void)count;
    assert(count == 5 && "Should produce rows_per_table rows with repeat_read");

    remove_test_file(filename);
    std::cout << "test_preload_csv_repeat_read passed\n";
}


// ============================================================
// Preload CSV — degenerate case: use_cache + generator timestamp
// ============================================================

void test_preload_csv_degenerate_cache_generator() {
    CSVDataManager::reset();
    const std::string filename = "rdg_preload_degen.csv";
    create_test_file(filename,
        "name,age\n"
        "Alice,30\n"
        "Bob,25\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"},
    };
    auto config = make_preload_config(filename, col_configs, -1, false, 5);

    // Set generator timestamp
    config.schema.columns_cfg.csv.timestamp_strategy.strategy_type = "generator";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.start_timestamp = static_cast<int64_t>(9000);
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_precision = "ms";
    config.schema.columns_cfg.csv.timestamp_strategy.generator.timestamp_step = static_cast<int64_t>(10);

    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    // use_cache = true triggers the degenerate path (only timestamps, no row data)
    RowDataGenerator generator("any_table", config, instances, /*use_cache=*/true);

    auto r1 = generator.next_row();
    assert(r1.has_value());
    assert(r1->timestamp == 9000);

    auto r2 = generator.next_row();
    assert(r2.has_value());
    assert(r2->timestamp == 9010);

    // Should generate up to rows_per_table
    int count = 2;
    while (generator.next_row()) {
        count++;
    }
    (void)count;
    assert(count == 5);

    remove_test_file(filename);
    std::cout << "test_preload_csv_degenerate_cache_generator passed\n";
}


// ============================================================
// Generator mode — generated_rows() tracking
// ============================================================

void test_generated_rows_tracking() {
    ColumnsConfig columns_config;
    columns_config.source_type = "generator";

    auto& ts_config = columns_config.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{0};
    ts_config.timestamp_step = 1;
    ts_config.timestamp_precision = "ms";

    InsertDataConfig config;
    config.schema.columns.emplace_back(ColumnConfig{"col1", "INT", "random", 1, 100});
    config.schema.generation.rows_per_table = 5;
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);
    RowDataGenerator generator("test_table", config, instances);

    assert(generator.generated_rows() == 0);

    generator.next_row();
    assert(generator.generated_rows() == 1);

    generator.next_row();
    generator.next_row();
    assert(generator.generated_rows() == 3);

    // Read remaining
    while (generator.next_row()) {}
    assert(generator.generated_rows() == 5);

    std::cout << "test_generated_rows_tracking passed\n";
}


// ============================================================
// Generator mode — has_more() reflects rows_per_table
// ============================================================

void test_has_more_reflects_limit() {
    ColumnsConfig columns_config;
    columns_config.source_type = "generator";

    auto& ts_config = columns_config.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{0};
    ts_config.timestamp_step = 1;
    ts_config.timestamp_precision = "ms";

    InsertDataConfig config;
    config.schema.columns.emplace_back(ColumnConfig{"col1", "INT", "random", 1, 100});
    config.schema.generation.rows_per_table = 2;
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);
    RowDataGenerator generator("test_table", config, instances);

    assert(generator.has_more());
    generator.next_row();
    assert(generator.has_more());
    generator.next_row();
    assert(!generator.has_more() && "Should be done after rows_per_table");

    std::cout << "test_has_more_reflects_limit passed\n";
}


// ============================================================
// Generator mode — reset clears generated_rows
// ============================================================

void test_reset_clears_generated_rows() {
    ColumnsConfig columns_config;
    columns_config.source_type = "generator";

    auto& ts_config = columns_config.generator.timestamp_strategy.timestamp_config;
    ts_config.start_timestamp = Timestamp{0};
    ts_config.timestamp_step = 1;
    ts_config.timestamp_precision = "ms";

    InsertDataConfig config;
    config.schema.columns.emplace_back(ColumnConfig{"col1", "INT", "random", 1, 100});
    config.schema.generation.rows_per_table = 3;
    config.schema.columns_cfg = columns_config;
    config.schema.columns_cfg.generator.schema = config.schema.columns;

    auto instances = ColumnConfigInstanceFactory::create(config.schema.columns);
    RowDataGenerator generator("test_table", config, instances);

    while (generator.next_row()) {}
    assert(generator.generated_rows() == 3);
    assert(!generator.has_more());

    generator.reset();
    assert(generator.generated_rows() == 0);
    assert(generator.has_more());

    auto row = generator.next_row();
    assert(row.has_value());
    assert(row->timestamp == 0 && "Reset should restart timestamp generator");

    std::cout << "test_reset_clears_generated_rows passed\n";
}


// ============================================================
// Streaming CSV — reset and re-read
// ============================================================

void test_streaming_csv_reset() {
    const std::string filename = "rdg_streaming_reset.csv";
    create_test_file(filename,
        "ts,value\n"
        "100,A\n"
        "200,B\n");

    ColumnConfigVector col_configs = {
        {"value", "varchar(10)"},
    };
    auto config = make_streaming_config(filename, col_configs, -1, false, 10);
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    RowDataGenerator generator("any_table", config, instances);

    auto r1 = generator.next_row();
    assert(r1.has_value() && r1->timestamp == 100);
    auto r2 = generator.next_row();
    assert(r2.has_value() && r2->timestamp == 200);
    auto r3 = generator.next_row();
    assert(!r3.has_value());

    generator.reset();

    auto r4 = generator.next_row();
    assert(r4.has_value());
    assert(r4->timestamp == 100 && "Should re-read from beginning after reset");
    assert(std::get<std::string>(r4->columns[0]) == "A");

    remove_test_file(filename);
    std::cout << "test_streaming_csv_reset passed\n";
}


// ============================================================
// Preload CSV — empty file path throws
// ============================================================

void test_preload_csv_empty_file_path() {
    CSVDataManager::reset();

    ColumnConfigVector col_configs = {
        {"value", "int"},
    };
    auto config = make_preload_config("", col_configs, -1, false, 10);
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    try {
        RowDataGenerator generator("any_table", config, instances);
        assert(false && "Should throw for empty CSV file path in preload mode");
    } catch (const std::exception& e) {
        (void)e;
        std::cout << "test_preload_csv_empty_file_path passed\n";
    }
}


// ============================================================
// Preload CSV — table not found in multi-table CSV
// ============================================================

void test_preload_csv_table_not_found() {
    CSVDataManager::reset();
    const std::string filename = "rdg_preload_notfound.csv";
    create_test_file(filename,
        "table,ts,value\n"
        "table_a,1000,10\n"
        "table_b,2000,20\n");

    ColumnConfigVector col_configs = {
        {"value", "int"},
    };
    auto config = make_preload_config(filename, col_configs, 0, false, 10);
    // tbname_index = 0, timestamp_index = 1
    config.schema.columns_cfg.csv.timestamp_strategy.csv.timestamp_index = 1;
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    try {
        RowDataGenerator generator("nonexistent_table", config, instances);
        assert(false && "Should throw for non-existent table in CSV");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("not found") != std::string::npos);
        std::cout << "test_preload_csv_table_not_found passed\n";
    }

    remove_test_file(filename);
}


// ============================================================
// Preload CSV — specific table from multi-table file
// ============================================================

void test_preload_csv_specific_table() {
    CSVDataManager::reset();
    const std::string filename = "rdg_preload_specific.csv";
    create_test_file(filename,
        "table,ts,value\n"
        "alpha,1000,10\n"
        "beta,2000,20\n"
        "alpha,3000,30\n");

    ColumnConfigVector col_configs = {
        {"value", "int"},
    };
    auto config = make_preload_config(filename, col_configs, 0, false, 10);
    config.schema.columns_cfg.csv.timestamp_strategy.csv.timestamp_index = 1;
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    // Generator for "alpha" — should get rows 1 and 3
    RowDataGenerator generator("alpha", config, instances);

    auto r1 = generator.next_row();
    assert(r1.has_value());
    assert(r1->timestamp == 1000);
    assert(std::get<int32_t>(r1->columns[0]) == 10);

    auto r2 = generator.next_row();
    assert(r2.has_value());
    assert(r2->timestamp == 3000);
    assert(std::get<int32_t>(r2->columns[0]) == 30);

    auto r3 = generator.next_row();
    assert(!r3.has_value());

    // Generator for "beta" — should get row 2
    RowDataGenerator gen_beta("beta", config, instances);

    auto rb1 = gen_beta.next_row();
    assert(rb1.has_value());
    assert(rb1->timestamp == 2000);
    assert(std::get<int32_t>(rb1->columns[0]) == 20);

    auto rb2 = gen_beta.next_row();
    assert(!rb2.has_value());

    remove_test_file(filename);
    std::cout << "test_preload_csv_specific_table passed\n";
}


// ============================================================
// Preload CSV — default table shared across generators
// ============================================================

void test_preload_csv_shared_default_table() {
    CSVDataManager::reset();
    const std::string filename = "rdg_preload_shared.csv";
    create_test_file(filename,
        "ts,value\n"
        "1000,42\n"
        "2000,84\n");

    ColumnConfigVector col_configs = {
        {"value", "int"},
    };
    auto config = make_preload_config(filename, col_configs, -1, false, 5);
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    // Two generators for different table names, but tbname_index = -1 means default_table
    RowDataGenerator gen1("sub_001", config, instances);
    RowDataGenerator gen2("sub_002", config, instances);

    auto r1 = gen1.next_row();
    auto r2 = gen2.next_row();

    assert(r1.has_value() && r2.has_value());
    // Both should read from the same shared data
    assert(r1->timestamp == r2->timestamp);
    assert(std::get<int32_t>(r1->columns[0]) == std::get<int32_t>(r2->columns[0]));

    remove_test_file(filename);
    std::cout << "test_preload_csv_shared_default_table passed\n";
}


// ============================================================
// Preload CSV — no repeat, total_rows limited by file size
// ============================================================

void test_preload_csv_total_rows_capped_by_file() {
    CSVDataManager::reset();
    const std::string filename = "rdg_preload_cap.csv";
    create_test_file(filename,
        "ts,value\n"
        "1000,1\n"
        "2000,2\n");

    ColumnConfigVector col_configs = {
        {"value", "int"},
    };
    // rows_per_table = 100, but file only has 2 rows and no repeat
    auto config = make_preload_config(filename, col_configs, -1, false, 100);
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    RowDataGenerator generator("any_table", config, instances);

    int count = 0;
    while (generator.next_row()) {
        count++;
    }
    (void)count;
    assert(count == 2 && "Should stop after file rows when no repeat_read");

    remove_test_file(filename);
    std::cout << "test_preload_csv_total_rows_capped_by_file passed\n";
}


// ============================================================
// main
// ============================================================

int main() {
    // Streaming CSV mode
    test_streaming_csv_basic();
    test_streaming_csv_repeat_read();
    test_streaming_csv_rows_per_table_limit();
    test_streaming_csv_with_generator_timestamp();
    test_streaming_csv_external_source();
    test_streaming_csv_empty_file_path();
    test_streaming_csv_reset();

    // Preload CSV supplemental tests
    test_preload_csv_with_generator_timestamp();
    test_preload_csv_repeat_read();
    test_preload_csv_degenerate_cache_generator();
    test_preload_csv_empty_file_path();
    test_preload_csv_table_not_found();
    test_preload_csv_specific_table();
    test_preload_csv_shared_default_table();
    test_preload_csv_total_rows_capped_by_file();

    // Generator mode supplemental tests
    test_generated_rows_tracking();
    test_has_more_reflects_limit();
    test_reset_clears_generated_rows();

    std::cout << "All RowDataGenerator streaming/supplemental tests passed!\n";
    return 0;
}
