#include <iostream>
#include <cassert>
#include "RowGenerator.hpp"

void test_generate_row_without_timestamp() {
    ColumnConfigVector col_configs = {
        {"col1", "int", "random", 10, 20},
        {"col2", "double", "random", 1.5, 3.5},
        {"col3", "bool", "random"}
    };

    ColumnConfigInstanceVector col_instances;
    for (size_t i = 0; i < col_configs.size(); ++i) {
        col_instances.emplace_back(col_configs[i]);
    }

    RowGenerator generator(col_instances);
    RowType row = generator.generate();

    assert(row.size() == col_configs.size());

    assert(std::holds_alternative<int>(row[0]));
    assert(std::get<int>(row[0]) >= 10 && std::get<int>(row[0]) < 20);

    assert(std::holds_alternative<double>(row[1]));
    assert(std::get<double>(row[1]) >= 1.5 && std::get<double>(row[1]) < 3.5);

    assert(std::holds_alternative<bool>(row[2]));

    std::cout << "test_generate_row_without_timestamp passed.\n";
}

void test_generate_row_with_timestamp() {
    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = Timestamp{1000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";

    ColumnConfigVector col_configs = {
        {"col1", "int", "random", 10, 20},
        {"col2", "double", "random", 1.5, 3.5}
    };

    ColumnConfigInstanceVector col_instances;
    for (size_t i = 0; i < col_configs.size(); ++i) {
        col_instances.emplace_back(col_configs[i]);
    }

    RowGenerator generator(ts_config, col_instances);
    RowType row = generator.generate();

    assert(row.size() == col_configs.size() + 1);

    assert(std::holds_alternative<Timestamp>(row[0]));
    assert(std::get<Timestamp>(row[0]) == 1000);

    assert(std::holds_alternative<int>(row[1]));
    assert(std::get<int>(row[1]) >= 10 && std::get<int>(row[1]) < 20);

    assert(std::holds_alternative<double>(row[2]));
    assert(std::get<double>(row[2]) >= 1.5 && std::get<double>(row[2]) < 3.5);

    std::cout << "test_generate_row_with_timestamp passed.\n";
}

void test_generate_multiple_rows() {
    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = Timestamp{1000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";

    ColumnConfigVector col_configs = {
        {"col1", "int", "random", 10, 20},
        {"col2", "double", "random", 1.5, 3.5}
    };

    ColumnConfigInstanceVector col_instances;
    for (size_t i = 0; i < col_configs.size(); ++i) {
        col_instances.emplace_back(col_configs[i]);
    }

    RowGenerator generator(ts_config, col_instances);
    std::vector<RowType> rows = generator.generate(5);

    assert(rows.size() == 5);

    for (size_t i = 0; i < rows.size(); ++i) {
        const RowType& row = rows[i];

        (void)row;
        assert(row.size() == col_configs.size() + 1);

        assert(std::holds_alternative<Timestamp>(row[0]));
        assert(std::get<Timestamp>(row[0]) == static_cast<Timestamp>(1000UL + i * 10));

        assert(std::holds_alternative<int>(row[1]));
        assert(std::get<int>(row[1]) >= 10 && std::get<int>(row[1]) < 20);

        assert(std::holds_alternative<double>(row[2]));
        assert(std::get<double>(row[2]) >= 1.5 && std::get<double>(row[2]) < 3.5);
    }

    std::cout << "test_generate_multiple_rows passed.\n";
}

void test_generate_row_null_ratio() {
    ColumnConfig cfg;
    cfg.name = "temp";
    cfg.type = "float";
    cfg.null_ratio = 1.0f;

    ColumnConfigInstanceVector instances;
    instances.emplace_back(cfg);

    RowGenerator generator(instances);

    for (int i = 0; i < 100; ++i) {
        RowType row = generator.generate();
        assert(row.size() == 1);
        assert(std::holds_alternative<NullValue>(row[0]));
    }
    std::cout << "test_generate_row_null_ratio passed.\n";
}

void test_generate_row_none_ratio() {
    ColumnConfig cfg;
    cfg.name = "temp";
    cfg.type = "float";
    cfg.none_ratio = 1.0f;

    ColumnConfigInstanceVector instances;
    instances.emplace_back(cfg);

    RowGenerator generator(instances);

    for (int i = 0; i < 100; ++i) {
        RowType row = generator.generate();
        assert(row.size() == 1);
        assert(std::holds_alternative<NoneValue>(row[0]));
    }
    std::cout << "test_generate_row_none_ratio passed.\n";
}

void test_generate_row_null_none_mixed() {
    ColumnConfig cfg;
    cfg.name = "temp";
    cfg.type = "int";
    cfg.null_ratio = 0.5f;
    cfg.none_ratio = 0.5f;

    ColumnConfigInstanceVector instances;
    instances.emplace_back(cfg);

    RowGenerator generator(instances);

    int null_count = 0, none_count = 0;
    const int N = 10000;
    for (int i = 0; i < N; ++i) {
        RowType row = generator.generate();
        if (std::holds_alternative<NullValue>(row[0])) null_count++;
        else if (std::holds_alternative<NoneValue>(row[0])) none_count++;
        else assert(false && "Expected NULL or NONE when ratios sum to 1.0");
    }
    // Both should appear roughly 50/50
    (void)null_count;
    (void)none_count;
    assert(null_count > N / 4 && null_count < 3 * N / 4);
    assert(none_count > N / 4 && none_count < 3 * N / 4);
    std::cout << "test_generate_row_null_none_mixed passed.\n";
}

void test_generate_row_no_ratio() {
    ColumnConfig cfg;
    cfg.name = "temp";
    cfg.type = "int";
    cfg.min = 1;
    cfg.max = 100;

    ColumnConfigInstanceVector instances;
    instances.emplace_back(cfg);

    RowGenerator generator(instances);

    for (int i = 0; i < 100; ++i) {
        RowType row = generator.generate();
        assert(std::holds_alternative<int32_t>(row[0]));
    }
    std::cout << "test_generate_row_no_ratio passed.\n";
}

void test_generate_batch_with_null_none() {
    ColumnConfig cfg;
    cfg.name = "temp";
    cfg.type = "float";
    cfg.null_ratio = 0.3f;
    cfg.none_ratio = 0.3f;

    ColumnConfigInstanceVector instances;
    instances.emplace_back(cfg);

    RowGenerator generator(instances);
    auto rows = generator.generate(1000);

    int null_count = 0, none_count = 0, val_count = 0;
    for (const auto& row : rows) {
        if (std::holds_alternative<NullValue>(row[0])) null_count++;
        else if (std::holds_alternative<NoneValue>(row[0])) none_count++;
        else val_count++;
    }
    // Roughly 30% null, 30% none, 40% value (with wide tolerance)
    (void)null_count;
    (void)none_count;
    (void)val_count;
    assert(null_count > 100 && null_count < 500);
    assert(none_count > 100 && none_count < 500);
    assert(val_count > 200 && val_count < 600);
    std::cout << "test_generate_batch_with_null_none passed.\n";
}

void test_generate_inplace_with_null_none() {
    ColumnConfig cfg1;
    cfg1.name = "temp";
    cfg1.type = "int";
    cfg1.null_ratio = 1.0f;

    ColumnConfig cfg2;
    cfg2.name = "humidity";
    cfg2.type = "float";
    cfg2.none_ratio = 1.0f;

    ColumnConfigInstanceVector instances;
    instances.emplace_back(cfg1);
    instances.emplace_back(cfg2);

    RowGenerator generator(instances);

    RowType row(2);
    for (int i = 0; i < 50; ++i) {
        generator.generate(row);
        assert(std::holds_alternative<NullValue>(row[0]));
        assert(std::holds_alternative<NoneValue>(row[1]));
    }
    std::cout << "test_generate_inplace_with_null_none passed.\n";
}

void test_generate_multi_column_different_ratios() {
    ColumnConfig cfg_null;
    cfg_null.name = "c1";
    cfg_null.type = "int";
    cfg_null.null_ratio = 1.0f;  // always NULL

    ColumnConfig cfg_none;
    cfg_none.name = "c2";
    cfg_none.type = "float";
    cfg_none.none_ratio = 1.0f;  // always NONE

    ColumnConfig cfg_normal;
    cfg_normal.name = "c3";
    cfg_normal.type = "int";
    cfg_normal.min = 1;
    cfg_normal.max = 100;
    // no ratios, always normal

    ColumnConfigInstanceVector instances;
    instances.emplace_back(cfg_null);
    instances.emplace_back(cfg_none);
    instances.emplace_back(cfg_normal);

    RowGenerator generator(instances);

    for (int i = 0; i < 100; ++i) {
        RowType row = generator.generate();
        assert(row.size() == 3);
        assert(std::holds_alternative<NullValue>(row[0]));
        assert(std::holds_alternative<NoneValue>(row[1]));
        assert(std::holds_alternative<int32_t>(row[2]));
    }
    std::cout << "test_generate_multi_column_different_ratios passed.\n";
}

void test_generate_with_timestamp_null_none() {
    TimestampGeneratorConfig ts_config;
    ts_config.start_timestamp = Timestamp{1000};
    ts_config.timestamp_step = 10;
    ts_config.timestamp_precision = "ms";

    ColumnConfig cfg;
    cfg.name = "val";
    cfg.type = "int";
    cfg.null_ratio = 1.0f;

    ColumnConfigInstanceVector instances;
    instances.emplace_back(cfg);

    RowGenerator generator(ts_config, instances);
    RowType row = generator.generate();

    // Timestamp should NOT be affected by null_ratio
    assert(row.size() == 2);
    assert(std::holds_alternative<Timestamp>(row[0]));
    assert(std::get<Timestamp>(row[0]) == 1000);
    // Data column should be NULL
    assert(std::holds_alternative<NullValue>(row[1]));

    // Batch variant
    auto rows = generator.generate(5);
    for (size_t i = 0; i < 5; ++i) {
        assert(std::holds_alternative<Timestamp>(rows[i][0]));
        assert(std::holds_alternative<NullValue>(rows[i][1]));
    }
    std::cout << "test_generate_with_timestamp_null_none passed.\n";
}

int main() {
    test_generate_row_without_timestamp();
    test_generate_row_with_timestamp();
    test_generate_multiple_rows();
    test_generate_row_null_ratio();
    test_generate_row_none_ratio();
    test_generate_row_null_none_mixed();
    test_generate_row_no_ratio();
    test_generate_batch_with_null_none();
    test_generate_inplace_with_null_none();
    test_generate_multi_column_different_ratios();
    test_generate_with_timestamp_null_none();

    std::cout << "All tests passed.\n";
    return 0;
}