#include <iostream>
#include <cassert>
#include <fstream>
#include <cmath>
#include <cstdio>
#include "StreamingCSVRowSource.hpp"


// Helper to create a dummy CSV file for tests
static void create_test_file(const std::string& filename, const std::string& content) {
    std::ofstream f(filename);
    f << content;
    f.close();
}

// Helper to clean up test files
static void remove_test_file(const std::string& filename) {
    std::remove(filename.c_str());
}


// ============================================================
// Basic functionality tests
// ============================================================

void test_basic_csv_with_generator_timestamp() {
    const std::string filename = "streaming_basic.csv";
    create_test_file(filename,
        "name,age,city\n"
        "Alice,30,New York\n"
        "Bob,25,Los Angeles\n"
        "Charlie,35,Chicago\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"},
        {"city", "varchar(30)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};
    ts.generator.timestamp_precision = "ms";

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    assert(source.has_more() && "Should have data initially");

    auto row1 = source.next();
    assert(row1.has_value() && "Should read first row");
    assert(row1->columns.size() == 3 && "Expected 3 columns");
    assert(std::get<std::string>(row1->columns[0]) == "Alice");
    assert(std::get<int32_t>(row1->columns[1]) == 30);
    assert(std::get<std::string>(row1->columns[2]) == "New York");

    auto row2 = source.next();
    assert(row2.has_value() && "Should read second row");
    assert(std::get<std::string>(row2->columns[0]) == "Bob");
    assert(std::get<int32_t>(row2->columns[1]) == 25);

    auto row3 = source.next();
    assert(row3.has_value() && "Should read third row");
    assert(std::get<std::string>(row3->columns[0]) == "Charlie");

    // Should be exhausted now
    auto row4 = source.next();
    assert(!row4.has_value() && "Should return nullopt after all rows consumed");
    assert(!source.has_more() && "has_more should be false when exhausted");

    remove_test_file(filename);
    std::cout << "test_basic_csv_with_generator_timestamp passed\n";
}

void test_basic_csv_with_csv_timestamp() {
    const std::string filename = "streaming_csv_ts.csv";
    create_test_file(filename,
        "ts,name,city\n"
        "1622505600000,Alice,New York\n"
        "1622592000000,Bob,Los Angeles\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(30)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "csv";
    ts.csv.enabled = true;
    ts.csv.timestamp_index = 0;
    ts.csv.timestamp_precision = "ms";

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    auto row1 = source.next();
    assert(row1.has_value());
    assert(row1->timestamp == 1622505600000 && "Expected first timestamp");
    assert(row1->columns.size() == 2 && "Expected 2 data columns (timestamp skipped)");
    assert(std::get<std::string>(row1->columns[0]) == "Alice");
    assert(std::get<std::string>(row1->columns[1]) == "New York");

    auto row2 = source.next();
    assert(row2.has_value());
    assert(row2->timestamp == 1622592000000 && "Expected second timestamp");
    assert(std::get<std::string>(row2->columns[0]) == "Bob");

    auto row3 = source.next();
    assert(!row3.has_value() && "Should be exhausted");

    remove_test_file(filename);
    std::cout << "test_basic_csv_with_csv_timestamp passed\n";
}

void test_csv_timestamp_middle_column() {
    const std::string filename = "streaming_ts_mid.csv";
    create_test_file(filename,
        "name,ts,city\n"
        "Alice,1622505600000,New York\n"
        "Bob,1622592000000,Los Angeles\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"city", "varchar(30)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "csv";
    ts.csv.enabled = true;
    ts.csv.timestamp_index = 1;  // middle column
    ts.csv.timestamp_precision = "ms";

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    auto row1 = source.next();
    assert(row1.has_value());
    assert(row1->timestamp == 1622505600000);
    assert(row1->columns.size() == 2 && "Should skip timestamp column");
    assert(std::get<std::string>(row1->columns[0]) == "Alice");
    assert(std::get<std::string>(row1->columns[1]) == "New York");

    remove_test_file(filename);
    std::cout << "test_csv_timestamp_middle_column passed\n";
}


// ============================================================
// Empty / edge case tests
// ============================================================

void test_empty_csv_file() {
    const std::string filename = "streaming_empty.csv";
    create_test_file(filename, "name,age\n");  // header only, no data

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    auto row = source.next();
    assert(!row.has_value() && "Empty CSV should return nullopt immediately");
    assert(!source.has_more() && "has_more should be false for empty CSV");

    remove_test_file(filename);
    std::cout << "test_empty_csv_file passed\n";
}

void test_single_row_csv() {
    const std::string filename = "streaming_single.csv";
    create_test_file(filename,
        "value\n"
        "42\n");

    ColumnConfigVector col_configs = {
        {"value", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    auto row1 = source.next();
    assert(row1.has_value());
    assert(std::get<int32_t>(row1->columns[0]) == 42);

    auto row2 = source.next();
    assert(!row2.has_value());

    remove_test_file(filename);
    std::cout << "test_single_row_csv passed\n";
}


// ============================================================
// Repeat read tests
// ============================================================

void test_repeat_read_cycles() {
    const std::string filename = "streaming_repeat.csv";
    create_test_file(filename,
        "value\n"
        "10\n"
        "20\n");

    ColumnConfigVector col_configs = {
        {"value", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};
    ts.generator.timestamp_precision = "ms";

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/true);

    // First cycle
    auto r1 = source.next();
    assert(r1.has_value());
    assert(std::get<int32_t>(r1->columns[0]) == 10);

    auto r2 = source.next();
    assert(r2.has_value());
    assert(std::get<int32_t>(r2->columns[0]) == 20);

    // Should cycle back to the beginning
    auto r3 = source.next();
    assert(r3.has_value() && "repeat_read should cycle back");
    assert(std::get<int32_t>(r3->columns[0]) == 10);

    auto r4 = source.next();
    assert(r4.has_value());
    assert(std::get<int32_t>(r4->columns[0]) == 20);

    // One more cycle to be sure
    auto r5 = source.next();
    assert(r5.has_value());
    assert(std::get<int32_t>(r5->columns[0]) == 10);

    remove_test_file(filename);
    std::cout << "test_repeat_read_cycles passed\n";
}

void test_no_repeat_read_exhausts() {
    const std::string filename = "streaming_no_repeat.csv";
    create_test_file(filename,
        "value\n"
        "100\n");

    ColumnConfigVector col_configs = {
        {"value", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    auto row1 = source.next();
    assert(row1.has_value());

    auto row2 = source.next();
    assert(!row2.has_value() && "Should not repeat");

    // Calling next() again after exhaustion should still return nullopt
    auto row3 = source.next();
    assert(!row3.has_value() && "Should stay exhausted");
    assert(!source.has_more());

    remove_test_file(filename);
    std::cout << "test_no_repeat_read_exhausts passed\n";
}


// ============================================================
// Reset tests
// ============================================================

void test_reset_allows_re_reading() {
    const std::string filename = "streaming_reset.csv";
    create_test_file(filename,
        "name\n"
        "Alice\n"
        "Bob\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    // Read all
    auto r1 = source.next();
    assert(r1.has_value());
    auto r2 = source.next();
    assert(r2.has_value());
    auto r3 = source.next();
    assert(!r3.has_value());

    // Reset and read again
    source.reset();
    assert(source.has_more() && "has_more should be true after reset");

    auto r4 = source.next();
    assert(r4.has_value());
    assert(std::get<std::string>(r4->columns[0]) == "Alice" && "Should re-read from beginning");

    auto r5 = source.next();
    assert(r5.has_value());
    assert(std::get<std::string>(r5->columns[0]) == "Bob");

    remove_test_file(filename);
    std::cout << "test_reset_allows_re_reading passed\n";
}


// ============================================================
// Timestamp precision conversion tests
// ============================================================

void test_timestamp_precision_conversion() {
    // In CSV timestamp mode, parse_timestamp interprets the raw string
    // using csv_precision. The result is stored directly (no further
    // conversion to target_precision in convert_row).
    const std::string filename = "streaming_precision.csv";
    create_test_file(filename,
        "ts,value\n"
        "1622505600,100\n"
        "1622505601,200\n");

    ColumnConfigVector col_configs = {
        {"value", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "csv";
    ts.csv.enabled = true;
    ts.csv.timestamp_index = 0;
    ts.csv.timestamp_precision = "s";

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "s", "s", /*repeat_read=*/false);

    auto row1 = source.next();
    assert(row1.has_value());
    assert(row1->timestamp == 1622505600 && "Expected parsed seconds timestamp");

    auto row2 = source.next();
    assert(row2.has_value());
    assert(row2->timestamp == 1622505601);

    remove_test_file(filename);
    std::cout << "test_timestamp_precision_conversion passed\n";
}


// ============================================================
// Timestamp offset tests
// ============================================================

void test_absolute_timestamp_offset() {
    const std::string filename = "streaming_abs_offset.csv";
    create_test_file(filename,
        "ts,value\n"
        "1000,10\n"
        "1100,20\n"
        "1300,30\n");

    ColumnConfigVector col_configs = {
        {"value", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "csv";
    ts.csv.enabled = true;
    ts.csv.timestamp_index = 0;
    ts.csv.timestamp_precision = "s";
    ts.csv.offset_config = TimestampCSVConfig::OffsetConfig(
        "absolute",
        std::string("1700000000"),
        "s"
    );

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "s", "s", /*repeat_read=*/false);

    auto row1 = source.next();
    assert(row1.has_value());
    // First row: absolute_value + (1000 - 1000) = 1700000000
    assert(row1->timestamp == 1700000000 && "First row should be absolute value");

    auto row2 = source.next();
    assert(row2.has_value());
    // Second row: absolute_value + (1100 - 1000) = 1700000100
    assert(row2->timestamp == 1700000100 && "Should maintain delta from first row");

    auto row3 = source.next();
    assert(row3.has_value());
    // Third row: absolute_value + (1300 - 1000) = 1700000300
    assert(row3->timestamp == 1700000300 && "Should maintain delta from first row");

    remove_test_file(filename);
    std::cout << "test_absolute_timestamp_offset passed\n";
}

void test_relative_timestamp_offset() {
    const std::string filename = "streaming_rel_offset.csv";
    create_test_file(filename,
        "ts,value\n"
        "1622505600,100\n");

    ColumnConfigVector col_configs = {
        {"value", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "csv";
    ts.csv.enabled = true;
    ts.csv.timestamp_index = 0;
    ts.csv.timestamp_precision = "s";
    ts.csv.offset_config = TimestampCSVConfig::OffsetConfig(
        "relative",
        std::string("+1d"),  // Add 1 day (86400s)
        "s"
    );

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "s", "s", /*repeat_read=*/false);

    auto row = source.next();
    assert(row.has_value());
    // 1622505600 + 86400 = 1622592000  (relative offset applies via civil time)
    assert(row->timestamp == 1622592000 && "Expected +1d offset");

    remove_test_file(filename);
    std::cout << "test_relative_timestamp_offset passed\n";
}

void test_negative_relative_offset() {
    const std::string filename = "streaming_neg_offset.csv";
    create_test_file(filename,
        "ts,value\n"
        "1622505600,100\n");

    ColumnConfigVector col_configs = {
        {"value", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "csv";
    ts.csv.enabled = true;
    ts.csv.timestamp_index = 0;
    ts.csv.timestamp_precision = "s";
    ts.csv.offset_config = TimestampCSVConfig::OffsetConfig(
        "relative",
        std::string("-1d"),  // Subtract 1 day
        "s"
    );

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "s", "s", /*repeat_read=*/false);

    auto row = source.next();
    assert(row.has_value());
    int64_t expected = 1622505600 - 86400;
    (void)expected;
    assert(row->timestamp == expected && "Expected -1d offset");

    remove_test_file(filename);
    std::cout << "test_negative_relative_offset passed\n";
}


// ============================================================
// Absolute offset with repeat_read (first_raw_ts_ reset)
// ============================================================

void test_absolute_offset_resets_on_repeat() {
    const std::string filename = "streaming_abs_repeat.csv";
    create_test_file(filename,
        "ts,value\n"
        "1000,10\n"
        "1200,20\n");

    ColumnConfigVector col_configs = {
        {"value", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "csv";
    ts.csv.enabled = true;
    ts.csv.timestamp_index = 0;
    ts.csv.timestamp_precision = "s";
    ts.csv.offset_config = TimestampCSVConfig::OffsetConfig(
        "absolute",
        std::string("5000"),
        "s"
    );

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "s", "s", /*repeat_read=*/true);

    // First cycle
    auto r1 = source.next();
    assert(r1.has_value());
    assert(r1->timestamp == 5000 && "First cycle row 1: absolute base");

    auto r2 = source.next();
    assert(r2.has_value());
    assert(r2->timestamp == 5200 && "First cycle row 2: base + (1200-1000)");

    // Second cycle: first_raw_ts_ should be reset
    auto r3 = source.next();
    assert(r3.has_value());
    assert(r3->timestamp == 5000 && "Second cycle row 1: absolute base again");

    auto r4 = source.next();
    assert(r4.has_value());
    assert(r4->timestamp == 5200 && "Second cycle row 2: base + delta again");

    remove_test_file(filename);
    std::cout << "test_absolute_offset_resets_on_repeat passed\n";
}


// ============================================================
// Generator timestamp monotonically increases across repeats
// ============================================================

void test_generator_timestamp_monotonic_across_repeats() {
    const std::string filename = "streaming_gen_repeat.csv";
    create_test_file(filename,
        "value\n"
        "A\n"
        "B\n");

    ColumnConfigVector col_configs = {
        {"value", "varchar(10)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};
    ts.generator.timestamp_precision = "ms";
    ts.generator.timestamp_step = static_cast<int64_t>(100);

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/true);

    int64_t prev_ts = -1;
    (void)prev_ts;
    for (int i = 0; i < 6; ++i) {  // 3 cycles of 2 rows
        auto row = source.next();
        assert(row.has_value());
        assert(row->timestamp > prev_ts && "Timestamps should monotonically increase across repeats");
        prev_ts = row->timestamp;
    }

    remove_test_file(filename);
    std::cout << "test_generator_timestamp_monotonic_across_repeats passed\n";
}


// ============================================================
// total_rows() always returns 0 for streaming source
// ============================================================

void test_total_rows_returns_zero() {
    const std::string filename = "streaming_total.csv";
    create_test_file(filename,
        "value\n"
        "1\n"
        "2\n"
        "3\n");

    ColumnConfigVector col_configs = {
        {"value", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    assert(source.total_rows() == 0 && "Streaming source total_rows() should always be 0");

    // Even after reading some rows
    source.next();
    assert(source.total_rows() == 0);

    remove_test_file(filename);
    std::cout << "test_total_rows_returns_zero passed\n";
}


// ============================================================
// Multi-file tests
// ============================================================

void test_multi_file_streaming() {
    const std::string file1 = "streaming_multi1.csv";
    const std::string file2 = "streaming_multi2.csv";
    create_test_file(file1,
        "name\n"
        "Alice\n"
        "Bob\n");
    create_test_file(file2,
        "name\n"
        "Charlie\n"
        "Diana\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};

    StreamingCSVRowSource source(
        {file1, file2}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    auto r1 = source.next();
    assert(r1.has_value());
    assert(std::get<std::string>(r1->columns[0]) == "Alice");

    auto r2 = source.next();
    assert(r2.has_value());
    assert(std::get<std::string>(r2->columns[0]) == "Bob");

    auto r3 = source.next();
    assert(r3.has_value());
    assert(std::get<std::string>(r3->columns[0]) == "Charlie");

    auto r4 = source.next();
    assert(r4.has_value());
    assert(std::get<std::string>(r4->columns[0]) == "Diana");

    auto r5 = source.next();
    assert(!r5.has_value() && "Should be exhausted after both files");

    remove_test_file(file1);
    remove_test_file(file2);
    std::cout << "test_multi_file_streaming passed\n";
}


// ============================================================
// No-header CSV test
// ============================================================

void test_csv_without_header() {
    const std::string filename = "streaming_noheader.csv";
    create_test_file(filename,
        "Alice,30\n"
        "Bob,25\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/false, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    auto row1 = source.next();
    assert(row1.has_value());
    assert(std::get<std::string>(row1->columns[0]) == "Alice");
    assert(std::get<int32_t>(row1->columns[1]) == 30);

    auto row2 = source.next();
    assert(row2.has_value());
    assert(std::get<std::string>(row2->columns[0]) == "Bob");
    assert(std::get<int32_t>(row2->columns[1]) == 25);

    auto row3 = source.next();
    assert(!row3.has_value());

    remove_test_file(filename);
    std::cout << "test_csv_without_header passed\n";
}


// ============================================================
// Different data types
// ============================================================

void test_various_column_types() {
    const std::string filename = "streaming_types.csv";
    create_test_file(filename,
        "name,flag,score,ratio\n"
        "Alice,true,100,3.14\n"
        "Bob,false,200,2.72\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"flag", "bool"},
        {"score", "bigint"},
        {"ratio", "double"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    auto row1 = source.next();
    assert(row1.has_value());
    assert(std::get<std::string>(row1->columns[0]) == "Alice");
    assert(std::get<bool>(row1->columns[1]) == true);
    assert(std::get<int64_t>(row1->columns[2]) == 100);
    assert(std::abs(std::get<double>(row1->columns[3]) - 3.14) < 0.001);

    auto row2 = source.next();
    assert(row2.has_value());
    assert(std::get<bool>(row2->columns[1]) == false);
    assert(std::get<int64_t>(row2->columns[2]) == 200);

    remove_test_file(filename);
    std::cout << "test_various_column_types passed\n";
}


// ============================================================
// Custom delimiter
// ============================================================

void test_custom_delimiter() {
    const std::string filename = "streaming_pipe.csv";
    create_test_file(filename,
        "name|age\n"
        "Alice|30\n"
        "Bob|25\n");

    ColumnConfigVector col_configs = {
        {"name", "varchar(20)"},
        {"age", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{};

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/true, /*delimiter=*/'|',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    auto row1 = source.next();
    assert(row1.has_value());
    assert(std::get<std::string>(row1->columns[0]) == "Alice");
    assert(std::get<int32_t>(row1->columns[1]) == 30);

    auto row2 = source.next();
    assert(row2.has_value());
    assert(std::get<std::string>(row2->columns[0]) == "Bob");
    assert(std::get<int32_t>(row2->columns[1]) == 25);

    remove_test_file(filename);
    std::cout << "test_custom_delimiter passed\n";
}

void test_csv_row_too_few_columns_throws() {
    const std::string filename = "streaming_row_too_few.csv";
    create_test_file(filename,
        "1000,10\n");

    ColumnConfigVector col_configs = {
        {"v1", "int"},
        {"v2", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "csv";
    ts.csv.enabled = true;
    ts.csv.timestamp_index = 0;
    ts.csv.timestamp_precision = "ms";

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/false, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    bool threw = false;
    try {
        auto row = source.next();
        (void)row;
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        (void)msg;
        threw = (msg.find("column count mismatch") != std::string::npos);
    }

    (void)threw;
    assert(threw && "Too-few streamed columns should throw");
    remove_test_file(filename);
    std::cout << "test_csv_row_too_few_columns_throws passed\n";
}

void test_csv_row_too_many_columns_throws() {
    const std::string filename = "streaming_row_too_many.csv";
    create_test_file(filename,
        "1000,10,20,30\n");

    ColumnConfigVector col_configs = {
        {"v1", "int"},
        {"v2", "int"}
    };
    auto instances = ColumnConfigInstanceFactory::create(col_configs);

    TimestampStrategy ts;
    ts.strategy_type = "csv";
    ts.csv.enabled = true;
    ts.csv.timestamp_index = 0;
    ts.csv.timestamp_precision = "ms";

    StreamingCSVRowSource source(
        {filename}, /*has_header=*/false, /*delimiter=*/',',
        instances, ts, "ms", "ms", /*repeat_read=*/false);

    bool threw = false;
    try {
        auto row = source.next();
        (void)row;
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        (void)msg;
        threw = (msg.find("column count mismatch") != std::string::npos);
    }

    (void)threw;
    assert(threw && "Too-many streamed columns should throw");
    remove_test_file(filename);
    std::cout << "test_csv_row_too_many_columns_throws passed\n";
}


int main() {
    test_basic_csv_with_generator_timestamp();
    test_basic_csv_with_csv_timestamp();
    test_csv_timestamp_middle_column();
    test_empty_csv_file();
    test_single_row_csv();
    test_repeat_read_cycles();
    test_no_repeat_read_exhausts();
    test_reset_allows_re_reading();
    test_timestamp_precision_conversion();
    test_absolute_timestamp_offset();
    test_relative_timestamp_offset();
    test_negative_relative_offset();
    test_absolute_offset_resets_on_repeat();
    test_generator_timestamp_monotonic_across_repeats();
    test_total_rows_returns_zero();
    test_multi_file_streaming();
    test_csv_without_header();
    test_various_column_types();
    test_custom_delimiter();
    test_csv_row_too_few_columns_throws();
    test_csv_row_too_many_columns_throws();

    std::cout << "All StreamingCSVRowSource tests passed!\n";
    return 0;
}
