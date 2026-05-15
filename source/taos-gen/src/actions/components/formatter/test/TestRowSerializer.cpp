#include "RowSerializer.hpp"
#include "ColumnConfig.hpp"
#include "MemoryPool.hpp"
#include <cassert>
#include <iostream>
#include <vector>
#include <stdexcept>

void test_basic_serialization() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"humidity", "INT"});
    col_instances.emplace_back(ColumnConfig{"location", "VARCHAR(20)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1609459200000, {25.5f, 60, std::string("factory-1")}});
    batch.table_batches.emplace_back("weather", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    // --- Test original to_json ---
    nlohmann::ordered_json result = RowSerializer::to_json(col_instances, tag_instances, tb, 0, "tb_name");
    assert(result["tb_name"] == "weather");
    assert(result["ts"] == 1609459200000);
    assert(result["temp"] == 25.5f);
    assert(result["humidity"] == 60);
    assert(result["location"] == "factory-1");
    (void)result;

    // --- Test to_json inplace version ---
    nlohmann::ordered_json result_inplace;
    RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 0, "tb_name", result_inplace);
    assert(result_inplace["tb_name"] == "weather");
    assert(result_inplace["ts"] == 1609459200000);
    assert(result_inplace["temp"] == 25.5f);
    assert(result_inplace["humidity"] == 60);
    assert(result_inplace["location"] == "factory-1");
    (void)result_inplace;

    // --- Test to_influx inplace ---
    fmt::memory_buffer buf;
    RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0,
                                     "weather", "", buf);
    auto ilp = fmt::to_string(buf);
    assert(ilp == "weather temp=25.5,humidity=60i,location=\"factory-1\" 1609459200000");
    (void)ilp;

    std::cout << "test_basic_serialization passed." << std::endl;
}

void test_without_tbname_key() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"value", "DOUBLE"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1609459201000, {123.456}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    // --- Test original to_json ---
    nlohmann::ordered_json result = RowSerializer::to_json(col_instances, tag_instances, tb, 0, "");
    assert(result.find("tb_name") == result.end());
    assert(result["ts"] == 1609459201000);
    assert(result["value"] == 123.456);
    (void)result;

    // --- Test to_json inplace version ---
    nlohmann::ordered_json result_inplace;
    RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 0, "", result_inplace);
    assert(result_inplace.find("tb_name") == result_inplace.end());
    assert(result_inplace["ts"] == 1609459201000);
    assert(result_inplace["value"] == 123.456);
    (void)result_inplace;

    // --- Test to_influx inplace ---
    fmt::memory_buffer buf;
    RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0,
                                     "t1", "", buf);
    auto ilp = fmt::to_string(buf);
    assert(ilp == "t1 value=123.456 1609459201000");
    (void)ilp;

    std::cout << "test_without_tbname_key passed." << std::endl;
}

void test_boolean_type() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"status", "BOOL"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1, {true}});
    rows.push_back({2, {false}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    // --- Test original to_json ---
    nlohmann::ordered_json result1 = RowSerializer::to_json(col_instances, tag_instances, tb, 0, "");
    nlohmann::ordered_json result2 = RowSerializer::to_json(col_instances, tag_instances, tb, 1, "");
    assert(result1["status"] == true);
    assert(result2["status"] == false);
    (void)result1;
    (void)result2;

    // --- Test to_json inplace version ---
    nlohmann::ordered_json result_inplace;
    RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 0, "", result_inplace);
    assert(result_inplace["status"] == true);
    result_inplace.clear(); // Clear for reuse
    RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 1, "", result_inplace);
    assert(result_inplace["status"] == false);
    (void)result_inplace;

    // --- Test to_influx inplace ---
    fmt::memory_buffer buf0;
    RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0,
                                     "t1", "", buf0);
    assert(fmt::to_string(buf0) == "t1 status=true 1");
    fmt::memory_buffer buf1;
    RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 1,
                                     "t1", "", buf1);
    assert(fmt::to_string(buf1) == "t1 status=false 2");
    (void)buf0;
    (void)buf1;

    std::cout << "test_boolean_type passed." << std::endl;
}

void test_out_of_range_exception() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"c1", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1, {100}});
    batch.table_batches.emplace_back("t1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    bool exception_thrown = false;
    try {
        // Test original to_json
        RowSerializer::to_json(col_instances, tag_instances, tb, 1, "");
        assert(false && "Should have thrown an exception for to_json");
    } catch (const std::out_of_range& e) {
        std::string msg = e.what();
        assert(msg.find("is out of range") != std::string::npos);
        exception_thrown = true;
    }
    assert(exception_thrown);

    // --- Test inplace version ---
    exception_thrown = false;
    try {
        nlohmann::ordered_json result_inplace;
        RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 1, "", result_inplace);
        assert(false && "Should have thrown an exception for to_json_inplace");
    } catch (const std::out_of_range& e) {
        std::string msg = e.what();
        assert(msg.find("is out of range") != std::string::npos);
        exception_thrown = true;
    }
    assert(exception_thrown);

    // --- Test to_influx inplace ---
    exception_thrown = false;
    try {
        fmt::memory_buffer buf;
        RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 1,
                                         "t1", "", buf);
        assert(false && "Should have thrown an exception for to_influx_inplace");
    } catch (const std::out_of_range& e) {
        std::string msg = e.what();
        assert(msg.find("is out of range") != std::string::npos);
        exception_thrown = true;
    }
    assert(exception_thrown);
    (void)exception_thrown;

    std::cout << "test_out_of_range_exception passed." << std::endl;
}

void test_serialization_with_tags() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    // Define columns
    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});

    // Define tags
    tag_instances.emplace_back(ColumnConfig{"region", "VARCHAR(10)"});
    tag_instances.emplace_back(ColumnConfig{"sensor_id", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1609459200000, {25.5f}});
    batch.table_batches.emplace_back("weather_sensor", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    // Register and assign tags to the table block
    std::vector<ColumnType> tag_values = {std::string("us-west"), int32_t(1001)};
    block->tables[0].tags_ptr = pool.register_table_tags("weather_sensor", tag_values);

    const auto& tb = block->tables[0];

    // Test to_json
    nlohmann::ordered_json result = RowSerializer::to_json(col_instances, tag_instances, tb, 0, "tbname");

    assert(result["tbname"] == "weather_sensor");
    assert(result["ts"] == 1609459200000);
    assert(result["temp"] == 25.5f);
    assert(result["region"] == "us-west");
    assert(result["sensor_id"] == 1001);

    // Test to_json_inplace
    nlohmann::ordered_json result_inplace;
    RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 0, "tbname", result_inplace);

    assert(result_inplace["tbname"] == "weather_sensor");
    assert(result_inplace["ts"] == 1609459200000);
    assert(result_inplace["temp"] == 25.5f);
    assert(result_inplace["region"] == "us-west");
    assert(result_inplace["sensor_id"] == 1001);

    // Test to_influx_inplace
    fmt::memory_buffer buf;
    RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0,
                                     "weather_sensor", "", buf);
    auto ilp = fmt::to_string(buf);
    assert(ilp == "weather_sensor,region=us-west,sensor_id=1001 temp=25.5 1609459200000");
    (void)ilp;

    std::cout << "test_serialization_with_tags passed." << std::endl;
}

void test_json_serialization_with_null_values() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"humidity", "INT"});

    tag_instances.emplace_back(ColumnConfig{"region", "VARCHAR(10)"});
    tag_instances.emplace_back(ColumnConfig{"group_id", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1609459200000, {25.5f, NullValue{}}});
    batch.table_batches.emplace_back("weather_null", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    std::vector<ColumnType> tag_values = {std::string("us-west"), NullValue{}};
    block->tables[0].tags_ptr = pool.register_table_tags("weather_null", tag_values);

    const auto& tb = block->tables[0];

    nlohmann::ordered_json result = RowSerializer::to_json(col_instances, tag_instances, tb, 0, "tbname");
    assert(result["tbname"] == "weather_null");
    assert(result["ts"] == 1609459200000);
    assert(result["temp"] == 25.5f);
    assert(result["humidity"].is_null());
    assert(result["region"] == "us-west");
    assert(result["group_id"].is_null());

    nlohmann::ordered_json result_inplace;
    RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 0, "tbname", result_inplace);
    assert(result_inplace["tbname"] == "weather_null");
    assert(result_inplace["ts"] == 1609459200000);
    assert(result_inplace["temp"] == 25.5f);
    assert(result_inplace["humidity"].is_null());
    assert(result_inplace["region"] == "us-west");
    assert(result_inplace["group_id"].is_null());

    std::cout << "test_json_serialization_with_null_values passed." << std::endl;
}

void test_influx_inplace_multiple_types_and_bool() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"f_float", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"f_double", "DOUBLE"});
    col_instances.emplace_back(ColumnConfig{"f_int", "INT"});
    col_instances.emplace_back(ColumnConfig{"f_bool", "BOOL"});
    col_instances.emplace_back(ColumnConfig{"f_str", "VARCHAR(20)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({123456789, {1.5f, 2.75, int32_t(42), true, std::string("a\"b\\c")}}); // string contains quotes/backslash
    batch.table_batches.emplace_back("m", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    fmt::memory_buffer buf;
    RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0,
                                     "m", "", buf);
    auto s = fmt::to_string(buf);
    // Expect: integers have i suffix; bool true/false; string quoted with escaping
    assert(s == "m f_float=1.5,f_double=2.75,f_int=42i,f_bool=true,f_str=\"a\\\"b\\\\c\" 123456789");
    (void)s;

    std::cout << "test_influx_inplace_multiple_types_and_bool passed." << std::endl;
}

void test_influx_inplace_escaping() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    col_instances.emplace_back(ColumnConfig{"f", "FLOAT"});
    tag_instances.emplace_back(ColumnConfig{"region name", "VARCHAR(20)"}); // tag key with space
    tag_instances.emplace_back(ColumnConfig{"k=a,b", "VARCHAR(20)"});       // tag key with special chars

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({999, {1.0f}});
    // measurement with space
    batch.table_batches.emplace_back("weather station", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    // Tag values requiring escaping
    std::vector<ColumnType> tag_values = {std::string("north east"), std::string("a=b,c")};
    block->tables[0].tags_ptr = pool.register_table_tags("weather station", tag_values);

    const auto& tb = block->tables[0];

    fmt::memory_buffer buf;
    RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0,
                                     "weather station", "", buf);
    auto s = fmt::to_string(buf);

    // Expect measurement and tag key/value escaped for space/comma/equals; field normal
    assert(s == "weather\\ station,region\\ name=north\\ east,k\\=a\\,b=a\\=b\\,c f=1 999");

    std::cout << "test_influx_inplace_escaping passed." << std::endl;
}

void test_influx_inplace_unsigned_types() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"v_tinyint", "TINYINT"});
    col_instances.emplace_back(ColumnConfig{"v_tinyint_u", "TINYINT UNSIGNED"});
    col_instances.emplace_back(ColumnConfig{"v_smallint", "SMALLINT"});
    col_instances.emplace_back(ColumnConfig{"v_smallint_u", "SMALLINT UNSIGNED"});
    col_instances.emplace_back(ColumnConfig{"v_int", "INT"});
    col_instances.emplace_back(ColumnConfig{"v_int_u", "INT UNSIGNED"});
    col_instances.emplace_back(ColumnConfig{"v_bigint", "BIGINT"});
    col_instances.emplace_back(ColumnConfig{"v_bigint_u", "BIGINT UNSIGNED"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1000, {
        int8_t(-10),
        uint8_t(200),
        int16_t(-300),
        uint16_t(40000),
        int32_t(-50000),
        uint32_t(3000000000u),
        int64_t(-100000000000LL),
        uint64_t(18000000000000000000ULL)
    }});
    batch.table_batches.emplace_back("types_test", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    fmt::memory_buffer buf;
    RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0,
                                     "types_test", "", buf);
    auto s = fmt::to_string(buf);
    std::string expected = "types_test "
        "v_tinyint=-10i,"
        "v_tinyint_u=200u,"
        "v_smallint=-300i,"
        "v_smallint_u=40000u,"
        "v_int=-50000i,"
        "v_int_u=3000000000u,"
        "v_bigint=-100000000000i,"
        "v_bigint_u=18000000000000000000u"
        " 1000";
    assert(s == expected);
    (void)s;

    std::cout << "test_influx_inplace_unsigned_types passed." << std::endl;
}

void test_influx_inplace_unsigned_with_measurement_overload() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"signed_val", "INT"});
    col_instances.emplace_back(ColumnConfig{"unsigned_val", "INT UNSIGNED"});
    tag_instances.emplace_back(ColumnConfig{"host", "VARCHAR(10)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({2000, {int32_t(-42), uint32_t(42)}});
    batch.table_batches.emplace_back("dev_001", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    std::vector<ColumnType> tag_values = {std::string("srv1")};
    block->tables[0].tags_ptr = pool.register_table_tags("dev_001", tag_values);
    const auto& tb = block->tables[0];

    fmt::memory_buffer buf;
    RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0,
                                     "cpu_metric", "device_id", buf);
    auto s = fmt::to_string(buf);
    assert(s == "cpu_metric,device_id=dev_001,host=srv1 signed_val=-42i,unsigned_val=42u 2000");
    (void)s;

    std::cout << "test_influx_inplace_unsigned_with_measurement_overload passed." << std::endl;
}

void test_influx_inplace_tdengine_suffix_mode() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    col_instances.emplace_back(ColumnConfig{"v_tinyint", "TINYINT"});
    col_instances.emplace_back(ColumnConfig{"v_tinyint_u", "TINYINT UNSIGNED"});
    col_instances.emplace_back(ColumnConfig{"v_smallint", "SMALLINT"});
    col_instances.emplace_back(ColumnConfig{"v_smallint_u", "SMALLINT UNSIGNED"});
    col_instances.emplace_back(ColumnConfig{"v_int", "INT"});
    col_instances.emplace_back(ColumnConfig{"v_int_u", "INT UNSIGNED"});
    col_instances.emplace_back(ColumnConfig{"v_bigint", "BIGINT"});
    col_instances.emplace_back(ColumnConfig{"v_bigint_u", "BIGINT UNSIGNED"});
    tag_instances.emplace_back(ColumnConfig{"host", "VARCHAR(10)"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({5000, {
        int8_t(-1), uint8_t(2),
        int16_t(-3), uint16_t(4),
        int32_t(-5), uint32_t(6),
        int64_t(-7), uint64_t(8)
    }});
    batch.table_batches.emplace_back("dev_001", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    std::vector<ColumnType> tag_values = {std::string("srv1")};
    block->tables[0].tags_ptr = pool.register_table_tags("dev_001", tag_values);
    const auto& tb = block->tables[0];

    // Test TDENGINE mode — precise suffixes
    fmt::memory_buffer buf_td;
    RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0,
                                     "all_types", "id", buf_td, IntSuffixMode::TDENGINE);
    auto td = fmt::to_string(buf_td);
    std::string expected_td = "all_types,id=dev_001,host=srv1 "
        "v_tinyint=-1i8,v_tinyint_u=2u8,"
        "v_smallint=-3i16,v_smallint_u=4u16,"
        "v_int=-5i32,v_int_u=6u32,"
        "v_bigint=-7i64,v_bigint_u=8u64"
        " 5000";
    assert(td == expected_td);

    // Test STANDARD mode (default) — same data
    fmt::memory_buffer buf_std;
    RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0,
                                     "all_types", "id", buf_std);
    auto std_s = fmt::to_string(buf_std);
    std::string expected_std = "all_types,id=dev_001,host=srv1 "
        "v_tinyint=-1i,v_tinyint_u=2u,"
        "v_smallint=-3i,v_smallint_u=4u,"
        "v_int=-5i,v_int_u=6u,"
        "v_bigint=-7i,v_bigint_u=8u"
        " 5000";
    assert(std_s == expected_std);

    std::cout << "test_influx_inplace_tdengine_suffix_mode passed." << std::endl;
}

void test_json_serialization_with_none_values() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    col_instances.emplace_back(ColumnConfig{"temp", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"humidity", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    // Second column is NONE — should be skipped in JSON
    rows.push_back({1609459200000, {25.5f, NoneValue{}}});
    batch.table_batches.emplace_back("weather_none", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    nlohmann::ordered_json result = RowSerializer::to_json(col_instances, tag_instances, tb, 0, "tbname");
    assert(result["tbname"] == "weather_none");
    assert(result["ts"] == 1609459200000);
    assert(result["temp"] == 25.5f);
    // NONE field should not appear in JSON
    assert(result.find("humidity") == result.end());

    nlohmann::ordered_json result_inplace;
    RowSerializer::to_json_inplace(col_instances, tag_instances, tb, 0, "tbname", result_inplace);
    assert(result_inplace["temp"] == 25.5f);
    assert(result_inplace.find("humidity") == result_inplace.end());

    std::cout << "test_json_serialization_with_none_values passed." << std::endl;
}

void test_influx_inplace_with_null_none() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    col_instances.emplace_back(ColumnConfig{"f_val", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"f_null", "INT"});
    col_instances.emplace_back(ColumnConfig{"f_none", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({100000, {1.5f, NullValue{}, NoneValue{}}});
    batch.table_batches.emplace_back("m", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    fmt::memory_buffer buf;
    bool written = RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0, "m", "", buf);
	(void)written;
    assert(written);
    auto s = fmt::to_string(buf);
    // NULL and NONE fields should be skipped in influx line protocol
    assert(s == "m f_val=1.5 100000");

    std::cout << "test_influx_inplace_with_null_none passed." << std::endl;
}

void test_influx_inplace_all_fields_null_none() {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;

    col_instances.emplace_back(ColumnConfig{"f1", "INT"});
    col_instances.emplace_back(ColumnConfig{"f2", "FLOAT"});
    col_instances.emplace_back(ColumnConfig{"f3", "INT"});

    MultiBatch batch;
    std::vector<RowData> rows;
    // Row 0: all fields NULL/NONE
    rows.push_back({100000, {NullValue{}, NoneValue{}, NullValue{}}});
    // Row 1: one valid field among NULL/NONE
    rows.push_back({100001, {NoneValue{}, 3.14f, NullValue{}}});
    batch.table_batches.emplace_back("tbl", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    const auto& tb = block->tables[0];

    // Row 0: all fields NULL/NONE — should be dropped (return false), buffer unchanged
    fmt::memory_buffer buf;
    buf.push_back('X');  // sentinel to verify rollback
    bool written = RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 0, "m", "", buf);
    (void)written;
    assert(!written);
    assert(buf.size() == 1);
    assert(buf[0] == 'X');

    // Row 1: has one valid field — should succeed
    fmt::memory_buffer buf2;
    written = RowSerializer::to_influx_inplace(col_instances, tag_instances, tb, 1, "m", "", buf2);
    assert(written);
    auto s = fmt::to_string(buf2);
    assert(s.find("f2=") != std::string::npos);
    // f1 and f3 should NOT appear
    assert(s.find("f1=") == std::string::npos);
    assert(s.find("f3=") == std::string::npos);

    std::cout << "test_influx_inplace_all_fields_null_none passed." << std::endl;
}

int main() {
    test_basic_serialization();
    test_without_tbname_key();
    test_boolean_type();
    test_out_of_range_exception();
    test_serialization_with_tags();
    test_json_serialization_with_null_values();
    test_json_serialization_with_none_values();
    test_influx_inplace_multiple_types_and_bool();
    test_influx_inplace_escaping();
    test_influx_inplace_unsigned_types();
    test_influx_inplace_unsigned_with_measurement_overload();
    test_influx_inplace_tdengine_suffix_mode();
    test_influx_inplace_with_null_none();
    test_influx_inplace_all_fields_null_none();

    std::cout << "All RowSerializer tests passed." << std::endl;
    return 0;
}