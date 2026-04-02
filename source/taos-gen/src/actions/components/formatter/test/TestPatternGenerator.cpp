#include "PatternGenerator.hpp"
#include "ColumnConfig.hpp"
#include "MemoryPool.hpp"
#include <cassert>
#include <iostream>
#include <vector>

// A concrete implementation for testing the abstract PatternGenerator
class TestablePatternGenerator : public PatternGenerator {
public:
    using TestToken = PatternGenerator::Token;
    static std::vector<TestToken> test_parse_pattern(const std::string& pattern) {
        return PatternGenerator::parse_pattern(pattern);
    }

    TestablePatternGenerator(const std::string& pattern,
                             const ColumnConfigInstanceVector& col_instances,
                             const ColumnConfigInstanceVector& tag_instances) {
        tokens_ = parse_pattern(pattern);
        build_mapping(col_instances, tag_instances);
    }

    std::string generate(const MemoryPool::TableBlock& data, size_t row_index) const override {
        std::string result;
        for (const auto& token : tokens_) {
            if (token.type == Token::TEXT) {
                result += token.value;
            } else {
                result += get_value_as_string(token.value, data, row_index);
            }
        }
        return result;
    }
};

void test_parse_pattern() {
    auto tokens1 = TestablePatternGenerator::test_parse_pattern("text/{ph1}/more-text/{ph2}");
    assert(tokens1.size() == 4);
    assert(tokens1[0].type == TestablePatternGenerator::TestToken::TEXT && tokens1[0].value == "text/");
    assert(tokens1[1].type == TestablePatternGenerator::TestToken::PLACEHOLDER && tokens1[1].value == "ph1");
    assert(tokens1[2].type == TestablePatternGenerator::TestToken::TEXT && tokens1[2].value == "/more-text/");
    assert(tokens1[3].type == TestablePatternGenerator::TestToken::PLACEHOLDER && tokens1[3].value == "ph2");

    auto tokens2 = TestablePatternGenerator::test_parse_pattern("{a}{b}");
    assert(tokens2.size() == 2);
    assert(tokens2[0].type == TestablePatternGenerator::TestToken::PLACEHOLDER && tokens2[0].value == "a");
    assert(tokens2[1].type == TestablePatternGenerator::TestToken::PLACEHOLDER && tokens2[1].value == "b");

    auto tokens3 = TestablePatternGenerator::test_parse_pattern("just_text");
    assert(tokens3.size() == 1);
    assert(tokens3[0].type == TestablePatternGenerator::TestToken::TEXT && tokens3[0].value == "just_text");

    auto tokens4 = TestablePatternGenerator::test_parse_pattern("");
    assert(tokens4.empty());

    std::cout << "test_parse_pattern passed." << std::endl;
}

void test_generate_with_columns() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"factory", "VARCHAR(10)"});
    col_instances.emplace_back(ColumnConfig{"device", "INT"});
    TestablePatternGenerator pg("data/{factory}/dev-{device}", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {std::string("f1"), 101}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    std::string result = pg.generate(tb, 0);
    assert(result == "data/f1/dev-101");
    std::cout << "test_generate_with_columns passed." << std::endl;
}

void test_generate_with_tags() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    // Define columns
    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});

    // Define tags
    tag_instances.emplace_back(ColumnConfig{"region", "VARCHAR(10)"});
    tag_instances.emplace_back(ColumnConfig{"sensor_id", "INT"});

    TestablePatternGenerator pg("region/{region}/sensor/{sensor_id}/val/{temp}", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {25.5f}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    // Register and assign tags to the table block
    std::vector<ColumnType> tag_values = {std::string("us-west"), int32_t(1001)};
    block->tables[0].tags_ptr = pool.register_table_tags("t1", tag_values);

    const auto& tb = block->tables[0];

    std::string result = pg.generate(tb, 0);
    assert(result.find("region/us-west/sensor/1001/val/") != std::string::npos);
    assert(result.find("25.5") != std::string::npos);
    (void)result;

    std::cout << "test_generate_with_tags passed." << std::endl;
}

void test_generate_special_placeholders() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    TestablePatternGenerator pg("prefix/{table}/ts/{ts}", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1609459200000, {}});
    batch.table_batches.emplace_back("my_super_table", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    auto& tb = block->tables[0];

    std::string result = pg.generate(tb, 0);
    assert(result == "prefix/my_super_table/ts/1609459200000");

    // Test with null table name and timestamp
    tb.table_name = nullptr;
    tb.timestamps = nullptr;
    result = pg.generate(tb, 0);
    assert(result == "prefix/UNKNOWN_TABLE/ts/INVALID_TS");

    std::cout << "test_generate_special_placeholders passed." << std::endl;
}

void test_generate_col_not_found() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"a", "INT"});
    TestablePatternGenerator pg("key/{b}", col_instances, tag_instances);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {123}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    std::string result = pg.generate(tb, 0);
    assert(result == "key/{COL_NOT_FOUND:b}");
    std::cout << "test_generate_col_not_found passed." << std::endl;
}

int main() {
    test_parse_pattern();
    test_generate_with_columns();
    test_generate_with_tags();
    test_generate_special_placeholders();
    test_generate_col_not_found();
    std::cout << "All PatternGenerator tests passed." << std::endl;
    return 0;
}