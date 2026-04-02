#include <iostream>
#include <cassert>
#include <cstdint>
#include <vector>
#include <memory>
#include <string>
#include "PreloadCSVRowSource.hpp"
#include "TimestampUtils.hpp"


// ============================================================
// Helpers
// ============================================================

static std::vector<RowData> make_rows(std::initializer_list<std::pair<int64_t, RowType>> items) {
    std::vector<RowData> rows;
    rows.reserve(items.size());
    for (const auto& [ts, cols] : items) {
        rows.push_back({ts, cols});
    }
    return rows;
}

static CSVDataManager::SharedRows make_shared_rows(std::vector<RowData> rows) {
    return std::make_shared<const std::vector<RowData>>(std::move(rows));
}


// ============================================================
// CSV timestamp mode — owned rows constructor
// ============================================================

void test_owned_rows_basic() {
    auto rows = make_rows({
        {1000, {std::string("Alice"), int32_t(30)}},
        {2000, {std::string("Bob"),   int32_t(25)}},
        {3000, {std::string("Charlie"), int32_t(35)}},
    });

    PreloadCSVRowSource source(rows, /*repeat_read=*/false);

    assert(source.has_more());
    assert(source.total_rows() == 3);

    auto r1 = source.next();
    assert(r1.has_value());
    assert(r1->timestamp == 1000);
    assert(std::get<std::string>(r1->columns[0]) == "Alice");
    assert(std::get<int32_t>(r1->columns[1]) == 30);

    auto r2 = source.next();
    assert(r2.has_value());
    assert(r2->timestamp == 2000);

    auto r3 = source.next();
    assert(r3.has_value());
    assert(r3->timestamp == 3000);

    auto r4 = source.next();
    assert(!r4.has_value() && "Should be exhausted");
    assert(!source.has_more());

    std::cout << "test_owned_rows_basic passed\n";
}

void test_owned_rows_empty() {
    std::vector<RowData> rows;
    PreloadCSVRowSource source(rows, /*repeat_read=*/false);

    assert(source.total_rows() == 0);
    auto r = source.next();
    assert(!r.has_value() && "Empty rows should return nullopt");
    assert(!source.has_more());

    std::cout << "test_owned_rows_empty passed\n";
}

void test_owned_rows_single() {
    auto rows = make_rows({
        {500, {int64_t(42)}},
    });

    PreloadCSVRowSource source(rows, /*repeat_read=*/false);

    assert(source.total_rows() == 1);

    auto r1 = source.next();
    assert(r1.has_value());
    assert(r1->timestamp == 500);
    assert(std::get<int64_t>(r1->columns[0]) == 42);

    auto r2 = source.next();
    assert(!r2.has_value());

    std::cout << "test_owned_rows_single passed\n";
}


// ============================================================
// CSV timestamp mode — shared rows constructor
// ============================================================

void test_shared_rows_basic() {
    auto shared = make_shared_rows(make_rows({
        {100, {std::string("X")}},
        {200, {std::string("Y")}},
    }));

    PreloadCSVRowSource source(shared, /*repeat_read=*/false);

    assert(source.total_rows() == 2);
    assert(source.has_more());

    auto r1 = source.next();
    assert(r1.has_value());
    assert(r1->timestamp == 100);
    assert(std::get<std::string>(r1->columns[0]) == "X");

    auto r2 = source.next();
    assert(r2.has_value());
    assert(r2->timestamp == 200);
    assert(std::get<std::string>(r2->columns[0]) == "Y");

    auto r3 = source.next();
    assert(!r3.has_value());

    std::cout << "test_shared_rows_basic passed\n";
}

void test_shared_rows_empty() {
    auto shared = make_shared_rows({});
    PreloadCSVRowSource source(shared, /*repeat_read=*/false);

    assert(source.total_rows() == 0);
    auto r = source.next();
    assert(!r.has_value());
    assert(!source.has_more());

    std::cout << "test_shared_rows_empty passed\n";
}


// ============================================================
// Repeat read — owned rows
// ============================================================

void test_repeat_read_owned() {
    auto rows = make_rows({
        {10, {int32_t(1)}},
        {20, {int32_t(2)}},
    });

    PreloadCSVRowSource source(rows, /*repeat_read=*/true);

    assert(source.has_more());

    // First cycle
    auto r1 = source.next();
    assert(r1.has_value() && r1->timestamp == 10);
    auto r2 = source.next();
    assert(r2.has_value() && r2->timestamp == 20);

    // Second cycle (wraps around)
    auto r3 = source.next();
    assert(r3.has_value() && r3->timestamp == 10);
    auto r4 = source.next();
    assert(r4.has_value() && r4->timestamp == 20);

    // Third cycle — still going
    auto r5 = source.next();
    assert(r5.has_value() && r5->timestamp == 10);

    assert(source.has_more() && "repeat_read should always report has_more");

    std::cout << "test_repeat_read_owned passed\n";
}

void test_repeat_read_shared() {
    auto shared = make_shared_rows(make_rows({
        {100, {std::string("A")}},
        {200, {std::string("B")}},
    }));

    PreloadCSVRowSource source(shared, /*repeat_read=*/true);

    // First cycle
    auto r1 = source.next();
    assert(r1.has_value() && std::get<std::string>(r1->columns[0]) == "A");
    auto r2 = source.next();
    assert(r2.has_value() && std::get<std::string>(r2->columns[0]) == "B");

    // Second cycle
    auto r3 = source.next();
    assert(r3.has_value() && std::get<std::string>(r3->columns[0]) == "A");

    std::cout << "test_repeat_read_shared passed\n";
}

void test_no_repeat_stays_exhausted() {
    auto rows = make_rows({
        {1, {int32_t(99)}},
    });

    PreloadCSVRowSource source(rows, /*repeat_read=*/false);

    auto r1 = source.next();
    assert(r1.has_value());

    auto r2 = source.next();
    assert(!r2.has_value());

    // Calling next() again should still return nullopt
    auto r3 = source.next();
    assert(!r3.has_value());
    assert(!source.has_more());

    std::cout << "test_no_repeat_stays_exhausted passed\n";
}


// ============================================================
// Reset
// ============================================================

void test_reset_owned_rows() {
    auto rows = make_rows({
        {10, {std::string("first")}},
        {20, {std::string("second")}},
    });

    PreloadCSVRowSource source(rows, /*repeat_read=*/false);

    auto r1 = source.next();
    assert(r1.has_value());
    auto r2 = source.next();
    assert(r2.has_value());
    auto r3 = source.next();
    assert(!r3.has_value());
    assert(!source.has_more());

    // Reset and read again
    source.reset();
    assert(source.has_more());

    auto r4 = source.next();
    assert(r4.has_value());
    assert(r4->timestamp == 10);
    assert(std::get<std::string>(r4->columns[0]) == "first");

    auto r5 = source.next();
    assert(r5.has_value());
    assert(r5->timestamp == 20);

    std::cout << "test_reset_owned_rows passed\n";
}

void test_reset_shared_rows() {
    auto shared = make_shared_rows(make_rows({
        {50, {int32_t(5)}},
    }));

    PreloadCSVRowSource source(shared, /*repeat_read=*/false);

    auto r1 = source.next();
    assert(r1.has_value());
    auto r2 = source.next();
    assert(!r2.has_value());

    source.reset();

    auto r3 = source.next();
    assert(r3.has_value());
    assert(r3->timestamp == 50);

    std::cout << "test_reset_shared_rows passed\n";
}


// ============================================================
// Generator timestamp mode — owned rows
// ============================================================

void test_generator_ts_owned_rows() {
    auto rows = make_rows({
        {999, {std::string("Alice")}},
        {888, {std::string("Bob")}},
        {777, {std::string("Charlie")}},
    });

    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = static_cast<int64_t>(1000);
    ts_config.timestamp_precision = "ms";
    ts_config.timestamp_step = static_cast<int64_t>(100);

    PreloadCSVRowSource source(ts_config, "ms", std::move(rows), /*repeat_read=*/false);

    assert(source.total_rows() == 3);

    auto r1 = source.next();
    assert(r1.has_value());
    assert(r1->timestamp == 1000 && "Should use generated timestamp, not original 999");
    assert(std::get<std::string>(r1->columns[0]) == "Alice");

    auto r2 = source.next();
    assert(r2.has_value());
    assert(r2->timestamp == 1100 && "Second generated timestamp");
    assert(std::get<std::string>(r2->columns[0]) == "Bob");

    auto r3 = source.next();
    assert(r3.has_value());
    assert(r3->timestamp == 1200);

    auto r4 = source.next();
    assert(!r4.has_value());

    std::cout << "test_generator_ts_owned_rows passed\n";
}

void test_generator_ts_shared_rows() {
    auto shared = make_shared_rows(make_rows({
        {0, {int32_t(10)}},
        {0, {int32_t(20)}},
    }));

    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = static_cast<int64_t>(5000);
    ts_config.timestamp_precision = "ms";
    ts_config.timestamp_step = static_cast<int64_t>(50);

    PreloadCSVRowSource source(ts_config, "ms", shared, /*repeat_read=*/false);

    auto r1 = source.next();
    assert(r1.has_value());
    assert(r1->timestamp == 5000);
    assert(std::get<int32_t>(r1->columns[0]) == 10);

    auto r2 = source.next();
    assert(r2.has_value());
    assert(r2->timestamp == 5050);
    assert(std::get<int32_t>(r2->columns[0]) == 20);

    auto r3 = source.next();
    assert(!r3.has_value());

    std::cout << "test_generator_ts_shared_rows passed\n";
}


// ============================================================
// Generator timestamp + repeat_read: timestamps advance monotonically
// ============================================================

void test_generator_ts_repeat_monotonic() {
    auto rows = make_rows({
        {0, {std::string("A")}},
        {0, {std::string("B")}},
    });

    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = static_cast<int64_t>(1000);
    ts_config.timestamp_precision = "ms";
    ts_config.timestamp_step = static_cast<int64_t>(10);

    PreloadCSVRowSource source(ts_config, "ms", std::move(rows), /*repeat_read=*/true);

    int64_t prev_ts = -1;
    (void)prev_ts;
    for (int i = 0; i < 6; ++i) {  // 3 cycles of 2 rows
        auto row = source.next();
        assert(row.has_value());
        assert(row->timestamp > prev_ts && "Timestamps must monotonically increase across repeat cycles");
        prev_ts = row->timestamp;
    }

    std::cout << "test_generator_ts_repeat_monotonic passed\n";
}


// ============================================================
// Generator timestamp + reset: timestamp generator also resets
// ============================================================

void test_generator_ts_reset() {
    auto rows = make_rows({
        {0, {int32_t(1)}},
        {0, {int32_t(2)}},
    });

    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = static_cast<int64_t>(1000);
    ts_config.timestamp_precision = "ms";
    ts_config.timestamp_step = static_cast<int64_t>(100);

    PreloadCSVRowSource source(ts_config, "ms", std::move(rows), /*repeat_read=*/false);

    auto r1 = source.next();
    assert(r1.has_value() && r1->timestamp == 1000);
    auto r2 = source.next();
    assert(r2.has_value() && r2->timestamp == 1100);
    auto r3 = source.next();
    assert(!r3.has_value());

    source.reset();

    // After reset, timestamps should start over from the beginning
    auto r4 = source.next();
    assert(r4.has_value());
    assert(r4->timestamp == 1000 && "Reset should restart timestamp generator");
    assert(std::get<int32_t>(r4->columns[0]) == 1);

    auto r5 = source.next();
    assert(r5.has_value());
    assert(r5->timestamp == 1100);

    std::cout << "test_generator_ts_reset passed\n";
}


// ============================================================
// Generator timestamp precision conversion
// ============================================================

void test_generator_ts_precision_conversion() {
    auto rows = make_rows({
        {0, {int32_t(42)}},
    });

    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = static_cast<int64_t>(1000);   // in seconds
    ts_config.timestamp_precision = "s";
    ts_config.timestamp_step = static_cast<int64_t>(1);

    // target precision is ms
    PreloadCSVRowSource source(ts_config, "ms", std::move(rows), /*repeat_read=*/false);

    auto r1 = source.next();
    assert(r1.has_value());
    // 1000 seconds -> 1000000 milliseconds
    assert(r1->timestamp == 1000000 && "Should convert s -> ms");

    std::cout << "test_generator_ts_precision_conversion passed\n";
}


// ============================================================
// Degenerate mode — timestamp-only, no row data
// ============================================================

void test_degenerate_mode_basic() {
    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = static_cast<int64_t>(5000);
    ts_config.timestamp_precision = "ms";
    ts_config.timestamp_step = static_cast<int64_t>(10);

    PreloadCSVRowSource source(ts_config, "ms");

    assert(source.has_more() && "Degenerate mode always has more");
    assert(source.total_rows() == 0 && "Degenerate mode total_rows is 0");

    auto r1 = source.next();
    assert(r1.has_value());
    assert(r1->timestamp == 5000);
    assert(r1->columns.empty() && "Degenerate mode produces no columns");

    auto r2 = source.next();
    assert(r2.has_value());
    assert(r2->timestamp == 5010);
    assert(r2->columns.empty());

    auto r3 = source.next();
    assert(r3.has_value());
    assert(r3->timestamp == 5020);

    // Should never exhaust
    assert(source.has_more());

    std::cout << "test_degenerate_mode_basic passed\n";
}

void test_degenerate_mode_precision_conversion() {
    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = static_cast<int64_t>(100);   // in seconds
    ts_config.timestamp_precision = "s";
    ts_config.timestamp_step = static_cast<int64_t>(1);

    // target is microseconds
    PreloadCSVRowSource source(ts_config, "us");

    auto r1 = source.next();
    assert(r1.has_value());
    // 100 seconds -> 100000000 microseconds
    assert(r1->timestamp == 100000000 && "Should convert s -> us");

    auto r2 = source.next();
    assert(r2.has_value());
    // 101 seconds -> 101000000 microseconds
    assert(r2->timestamp == 101000000);

    std::cout << "test_degenerate_mode_precision_conversion passed\n";
}

void test_degenerate_mode_always_has_more() {
    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = static_cast<int64_t>(0);
    ts_config.timestamp_precision = "ms";
    ts_config.timestamp_step = static_cast<int64_t>(1);

    PreloadCSVRowSource source(ts_config, "ms");

    // Read many times, should never exhaust
    for (int i = 0; i < 100; ++i) {
        assert(source.has_more());
        auto r = source.next();
        assert(r.has_value());
        assert(r->timestamp == i);
    }

    std::cout << "test_degenerate_mode_always_has_more passed\n";
}

void test_degenerate_mode_reset() {
    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = static_cast<int64_t>(1000);
    ts_config.timestamp_precision = "ms";
    ts_config.timestamp_step = static_cast<int64_t>(50);

    PreloadCSVRowSource source(ts_config, "ms");

    auto r1 = source.next();
    assert(r1.has_value() && r1->timestamp == 1000);
    auto r2 = source.next();
    assert(r2.has_value() && r2->timestamp == 1050);

    source.reset();

    auto r3 = source.next();
    assert(r3.has_value());
    assert(r3->timestamp == 1000 && "Reset should restart degenerate generator");

    std::cout << "test_degenerate_mode_reset passed\n";
}


// ============================================================
// has_more() edge cases
// ============================================================

void test_has_more_no_repeat_partially_consumed() {
    auto rows = make_rows({
        {1, {int32_t(10)}},
        {2, {int32_t(20)}},
        {3, {int32_t(30)}},
    });

    PreloadCSVRowSource source(rows, /*repeat_read=*/false);

    assert(source.has_more());

    source.next();
    assert(source.has_more() && "Still has rows");

    source.next();
    assert(source.has_more() && "Still has 1 row left");

    source.next();
    // Now at index 3, which == size
    assert(!source.has_more() && "All consumed");

    std::cout << "test_has_more_no_repeat_partially_consumed passed\n";
}

void test_has_more_repeat_always_true() {
    auto rows = make_rows({
        {1, {int32_t(1)}},
    });

    PreloadCSVRowSource source(rows, /*repeat_read=*/true);

    for (int i = 0; i < 10; ++i) {
        assert(source.has_more());
        source.next();
    }

    std::cout << "test_has_more_repeat_always_true passed\n";
}


// ============================================================
// Column data preservation
// ============================================================

void test_column_data_preserved() {
    auto rows = make_rows({
        {100, {
            bool(true),
            int32_t(42),
            int64_t(123456789LL),
            double(3.14),
            std::string("hello"),
        }},
    });

    PreloadCSVRowSource source(rows, /*repeat_read=*/false);

    auto r = source.next();
    assert(r.has_value());
    assert(r->columns.size() == 5);
    assert(std::get<bool>(r->columns[0]) == true);
    assert(std::get<int32_t>(r->columns[1]) == 42);
    assert(std::get<int64_t>(r->columns[2]) == 123456789LL);
    assert(std::get<double>(r->columns[3]) == 3.14);
    assert(std::get<std::string>(r->columns[4]) == "hello");

    std::cout << "test_column_data_preserved passed\n";
}


// ============================================================
// main
// ============================================================

int main() {
    // CSV timestamp mode — owned rows
    test_owned_rows_basic();
    test_owned_rows_empty();
    test_owned_rows_single();

    // CSV timestamp mode — shared rows
    test_shared_rows_basic();
    test_shared_rows_empty();

    // Repeat read
    test_repeat_read_owned();
    test_repeat_read_shared();
    test_no_repeat_stays_exhausted();

    // Reset
    test_reset_owned_rows();
    test_reset_shared_rows();

    // Generator timestamp mode
    test_generator_ts_owned_rows();
    test_generator_ts_shared_rows();
    test_generator_ts_repeat_monotonic();
    test_generator_ts_reset();
    test_generator_ts_precision_conversion();

    // Degenerate mode
    test_degenerate_mode_basic();
    test_degenerate_mode_precision_conversion();
    test_degenerate_mode_always_has_more();
    test_degenerate_mode_reset();

    // has_more() edge cases
    test_has_more_no_repeat_partially_consumed();
    test_has_more_repeat_always_true();

    // Column data preservation
    test_column_data_preserved();

    std::cout << "All PreloadCSVRowSource tests passed!\n";
    return 0;
}
