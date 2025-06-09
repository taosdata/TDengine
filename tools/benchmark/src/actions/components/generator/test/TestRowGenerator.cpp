#include <iostream>
#include <cassert>
#include "RowGenerator.h"

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

        assert(row.size() == col_configs.size() + 1);

        assert(std::holds_alternative<Timestamp>(row[0]));
        assert(std::get<Timestamp>(row[0]) == 1000 + i * 10);

        assert(std::holds_alternative<int>(row[1]));
        assert(std::get<int>(row[1]) >= 10 && std::get<int>(row[1]) < 20);

        assert(std::holds_alternative<double>(row[2]));
        assert(std::get<double>(row[2]) >= 1.5 && std::get<double>(row[2]) < 3.5);
    }

    std::cout << "test_generate_multiple_rows passed.\n";
}

int main() {
    test_generate_row_without_timestamp();
    test_generate_row_with_timestamp();
    test_generate_multiple_rows();

    std::cout << "All tests passed.\n";
    return 0;
}