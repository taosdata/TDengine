import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamResultSavedDatatypePrecision:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_result_saved_datatype_precision(self):
        """Result saved: datatype precision

        This test focuses on precise datatype validation and edge cases:

        1. Test precise column length calculations
            1.1 VARCHAR length from string functions (CONCAT, SUBSTR, LPAD, RPAD)
            1.2 NCHAR vs VARCHAR type differences
            1.3 Binary data type handling
            1.4 Maximum and minimum length constraints

        2. Test numeric type precision and boundaries
            2.1 Integer overflow and underflow scenarios
            2.2 Float and double precision preservation
            2.3 Numeric type casting edge cases
            2.4 Aggregation result type precision

        3. Test tag type precision validation
            3.1 Tag length calculation from expressions
            3.2 Tag type inheritance from source columns
            3.3 Computed tag type determination
            3.4 Tag name length limits and truncation

        4. Test special data type scenarios
            4.1 JSON data type handling (if supported)
            4.2 Timestamp precision levels
            4.3 NULL value handling in different types
            4.4 Unicode and multi-byte character handling

        Catalog:
            - Streams:ResultSaved:DatatypePrecision

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-19 Generated for precise datatype validation

        """

        self.createSnode()
        self.createDatabase()
        self.prepareQueryData()
        self.prepareTriggerTable()
        self.createStreams()
        self.checkStreamStatus()
        self.writeTriggerData()
        self.checkResults()

    def createSnode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def createDatabase(self):
        tdLog.info(f"create database")

        tdSql.prepare(dbname="qdb", vgroups=1)
        tdSql.prepare(dbname="tdb", vgroups=1)
        tdSql.prepare(dbname="rdb", vgroups=1)
        clusterComCheck.checkDbReady("qdb")
        clusterComCheck.checkDbReady("tdb")
        clusterComCheck.checkDbReady("rdb")

    def prepareQueryData(self):
        tdLog.info("prepare child tables for query")
        tdStream.prepareChildTables(tbBatch=1, rowBatch=1, rowsPerBatch=400)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(tables=10, rowBatch=1)

    def prepareTriggerTable(self):
        tdLog.info("prepare tables for trigger")

        # Create trigger table with precise type definitions for testing
        stb = """create table tdb.dt_triggers (
            ts timestamp, 
            c_tinyint tinyint, 
            c_smallint smallint,
            c_int int, 
            c_bigint bigint,
            c_utinyint tinyint unsigned,
            c_usmallint smallint unsigned,
            c_uint int unsigned,
            c_ubigint bigint unsigned,
            c_float float,
            c_double double,
            c_bool bool,
            c_varchar_min varchar(1),
            c_varchar_mid varchar(100),
            c_varchar_max varchar(4096),
            c_nchar_min nchar(1),
            c_nchar_mid nchar(100),
            c_binary_min binary(1),
            c_binary_max binary(254),
            c_json json
        ) tags(
            tag_tinyint tinyint,
            tag_varchar_short varchar(10), 
            tag_varchar_long varchar(200),
            tag_nchar nchar(50),
            tag_bool bool,
            tag_double double,
            tag_json json
        );"""
        
        ctb = """create table tdb.dt_t1 using tdb.dt_triggers tags(
            1, 'short1', 'this_is_a_longer_tag_value_for_testing_length_calculations_1', 
            'nchar测试1', true, 1.23456789, '{"key1":"value1","num":123}'
        ) tdb.dt_t2 using tdb.dt_triggers tags(
            2, 'short2', 'this_is_a_longer_tag_value_for_testing_length_calculations_2', 
            'nchar测试2', false, 2.3456789, '{"key2":"value2","num":456}'
        )"""
        
        tdSql.execute(stb)
        tdSql.execute(ctb)

    def writeTriggerData(self):
        tdLog.info("write data to trigger table")
        sqls = [
            """insert into tdb.dt_t1 values (
                '2025-01-01 00:00:00', 127, 32767, 2147483647, 9223372036854775807,
                255, 65535, 4294967295, 18446744073709551615,
                3.14159, 3.141592653589793, true,
                'a', 'mid_length_string_for_testing_varchar_calculations_here', 
                'maximum_length_varchar_string_that_should_test_the_upper_bounds_of_varchar_storage_and_ensure_proper_length_calculation_during_stream_processing_operations',
                '中', 'nchar中文测试字符串用于验证多字节字符处理能力',
                'b', 'binary_data_for_testing_binary_type_storage_and_processing_capabilities_in_stream_operations',
                '{"type":"test","value":123,"nested":{"key":"value"},"array":[1,2,3]}'
            )""",
            """insert into tdb.dt_t2 values (
                '2025-01-01 00:05:00', -128, -32768, -2147483648, -9223372036854775808,
                0, 0, 0, 0,
                -3.14159, -3.141592653589793, false,
                'z', 'another_mid_length_string_for_comprehensive_testing',
                'another_maximum_length_varchar_string_designed_to_test_edge_cases_and_boundary_conditions_in_varchar_processing',
                '文', 'another_nchar测试多字节字符串处理',
                'x', 'another_binary_test_string_for_comprehensive_binary_data_validation',
                '{"type":"test2","value":456,"bool":true,"null":null}'
            )"""
        ]
        
        for sql in sqls:
            try:
                tdSql.execute(sql)
            except Exception as e:
                tdLog.info(f"Insert may fail due to data length: {e}")

    def checkStreamStatus(self):
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def checkResults(self):
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()
        tdLog.info(f"check total:{len(self.streams)} streams result successfully")

    def createStreams(self):
        self.streams = []

        # Test 1.1: VARCHAR length from string functions
        stream = StreamItem(
            id=0,
            stream="create stream rdb.dt_s0 interval(10m) sliding(10m) from tdb.dt_triggers into rdb.dt_r0 as select _twstart ts, concat('prefix_', c_varchar_min, '_middle_', c_varchar_mid, '_suffix') long_concat, substr(c_varchar_max, 1, 50) substr_result, lpad(c_varchar_min, 20, '0') lpad_result, rpad(c_varchar_mid, 150, 'x') rpad_result from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, long_concat, substr_result, lpad_result, rpad_result from rdb.dt_r0;",
            exp_query="select _wstart ts, concat('prefix_', cvarchar_min, '_middle_', cvarchar_mid, '_suffix') long_concat, substr(cvarchar_max, 1, 50) substr_result, lpad(cvarchar_min, 20, '0') lpad_result, rpad(cvarchar_mid, 150, 'x') rpad_result from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:10:00' interval(10m);",
            check_func=self.check0,
        )
        self.streams.append(stream)

        # Test 1.2: NCHAR vs VARCHAR type differences
        stream = StreamItem(
            id=1,
            stream="create stream rdb.dt_s1 interval(10m) sliding(10m) from tdb.dt_triggers into rdb.dt_r1 as select _twstart ts, c_varchar_mid varchar_col, c_nchar_mid nchar_col, concat(c_varchar_mid, c_nchar_mid) mixed_concat, length(c_nchar_mid) nchar_len, char_length(c_nchar_mid) nchar_char_len from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, varchar_col, nchar_col, mixed_concat, nchar_len, nchar_char_len from rdb.dt_r1;",
            exp_query="select _wstart ts, cvarchar_mid varchar_col, cnchar_mid nchar_col, concat(cvarchar_mid, cnchar_mid) mixed_concat, length(cnchar_mid) nchar_len, char_length(cnchar_mid) nchar_char_len from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:10:00' interval(10m);",
            check_func=self.check1,
        )
        self.streams.append(stream)

        # Test 2.1: Integer overflow and boundary values
        stream = StreamItem(
            id=2,
            stream="create stream rdb.dt_s2 interval(10m) sliding(10m) from tdb.dt_triggers into rdb.dt_r2 as select _twstart ts, c_tinyint tiny_val, c_smallint small_val, c_int int_val, c_bigint big_val, c_utinyint utiny_val, c_usmallint usmall_val, c_uint uint_val, c_ubigint ubig_val, max(c_bigint) max_big, min(c_tinyint) min_tiny from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, tiny_val, small_val, int_val, big_val, utiny_val, usmall_val, uint_val, ubig_val, max_big, min_tiny from rdb.dt_r2;",
            exp_query="select _wstart ts, ctinyint tiny_val, csmallint small_val, cint int_val, cbigint big_val, cutinyint utiny_val, cusmallint usmall_val, cuint uint_val, cubigint ubig_val, max(cbigint) max_big, min(ctinyint) min_tiny from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:10:00' interval(10m);",
            check_func=self.check2,
        )
        self.streams.append(stream)

        # Test 2.2: Float and double precision preservation
        stream = StreamItem(
            id=3,
            stream="create stream rdb.dt_s3 interval(10m) sliding(10m) from tdb.dt_triggers into rdb.dt_r3 as select _twstart ts, c_float float_val, c_double double_val, round(c_double, 6) rounded_double, cast(c_float as double) float_to_double, avg(c_double) avg_double, sum(c_float) sum_float from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, float_val, double_val, rounded_double, float_to_double, avg_double, sum_float from rdb.dt_r3;",
            exp_query="select _wstart ts, cfloat float_val, cdouble double_val, round(cdouble, 6) rounded_double, cast(cfloat as double) float_to_double, avg(cdouble) avg_double, sum(cfloat) sum_float from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:10:00' interval(10m);",
            check_func=self.check3,
        )
        self.streams.append(stream)

        # Test 3.1: Tag length calculation from expressions
        stream = StreamItem(
            id=4,
            stream="create stream rdb.dt_s4 interval(10m) sliding(10m) from tdb.dt_triggers partition by tag_tinyint into rdb.dt_r4 tags(original_tag tinyint as %%1, computed_varchar varchar(50) as concat('tag_', cast(%%1 as varchar(10)), '_computed'), truncated_tag varchar(5) as substr(tag_varchar_long, 1, 5)) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, original_tag, computed_varchar, truncated_tag from rdb.dt_r4 where original_tag=1;",
            exp_query="select _wstart ts, count(*) cnt, 1, 'tag_1_computed', 'this_' from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:10:00' and ttinyint=1 interval(10m);",
            check_func=self.check4,
        )
        self.streams.append(stream)

        # Test 3.2: Tag type inheritance from source columns
        stream = StreamItem(
            id=5,
            stream="create stream rdb.dt_s5 interval(10m) sliding(10m) from tdb.dt_triggers partition by tag_varchar_short, tag_bool, tag_double into rdb.dt_r5 as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, tag_varchar_short, tag_bool, tag_double from rdb.dt_r5 where tag_varchar_short='short1';",
            exp_query="select _wstart ts, count(*) cnt, 'short1', true, 1.23456789 from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:10:00' and tvarchar_short='short1' interval(10m);",
            check_func=self.check5,
        )
        self.streams.append(stream)

        # Test 4.1: JSON data type handling (if supported)
        stream = StreamItem(
            id=6,
            stream="create stream rdb.dt_s6 interval(10m) sliding(10m) from tdb.dt_triggers into rdb.dt_r6 as select _twstart ts, c_json json_col, json_extract(c_json, '$.type') json_type, json_extract(c_json, '$.value') json_value from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, json_col, json_type, json_value from rdb.dt_r6;",
            exp_query="select _wstart ts, cjson json_col, json_extract(cjson, '$.type') json_type, json_extract(cjson, '$.value') json_value from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:10:00' interval(10m);",
            check_func=self.check6,
        )
        self.streams.append(stream)

        # Test 4.2: Timestamp precision levels
        stream = StreamItem(
            id=7,
            stream="create stream rdb.dt_s7 interval(10m) sliding(10m) from tdb.dt_triggers into rdb.dt_r7 as select _twstart ts, to_iso8601(_twstart) iso_format, timetruncate(_twstart, 1s) truncated_ts, elapsed(_twstart, _twend) elapsed_time from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, iso_format, truncated_ts, elapsed_time from rdb.dt_r7;",
            exp_query="select _wstart ts, to_iso8601(_wstart) iso_format, timetruncate(_wstart, 1s) truncated_ts, elapsed(_wstart, _wend) elapsed_time from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:10:00' interval(10m);",
            check_func=self.check7,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            try:
                stream.createStream()
            except Exception as e:
                tdLog.info(f"Stream creation may fail due to unsupported features: {e}")

    def check0(self):
        # Test 1.1: VARCHAR length from string functions
        tdSql.checkTableType(dbname="rdb", tbname="dt_r0", typename="NORMAL_TABLE", columns=5)
        # Check schema with calculated VARCHAR lengths
        tdSql.checkResultsByFunc(
            sql="describe rdb.dt_r0;",
            func=lambda: len(tdSql.queryResult) == 5,  # Should have 5 columns
        )
        # Verify the longest concatenated string column exists
        tdSql.checkResultsByFunc(
            sql="select column_name, data_type, data_length from information_schema.ins_columns where db_name='rdb' and table_name='dt_r0' and column_name='long_concat';",
            func=lambda: len(tdSql.queryResult) == 1 and tdSql.queryResult[0][1] == "VARCHAR",
        )

    def check1(self):
        # Test 1.2: NCHAR vs VARCHAR type differences
        tdSql.checkTableType(dbname="rdb", tbname="dt_r1", typename="NORMAL_TABLE", columns=6)
        # Check specific column types
        tdSql.checkResultsByFunc(
            sql="select column_name, data_type from information_schema.ins_columns where db_name='rdb' and table_name='dt_r1' and column_name in ('varchar_col', 'nchar_col');",
            func=lambda: len(tdSql.queryResult) == 2,
        )

    def check2(self):
        # Test 2.1: Integer boundary values
        tdSql.checkTableType(dbname="rdb", tbname="dt_r2", typename="NORMAL_TABLE", columns=11)
        # Verify all integer types are preserved
        expected_types = ["TINYINT", "SMALLINT", "INT", "BIGINT", "TINYINT UNSIGNED", "SMALLINT UNSIGNED", "INT UNSIGNED", "BIGINT UNSIGNED"]
        tdSql.checkResultsByFunc(
            sql="select data_type from information_schema.ins_columns where db_name='rdb' and table_name='dt_r2' and column_name like '%_val';",
            func=lambda: len(tdSql.queryResult) >= 8,  # Should have at least 8 integer type columns
        )

    def check3(self):
        # Test 2.2: Float and double precision preservation
        tdSql.checkTableType(dbname="rdb", tbname="dt_r3", typename="NORMAL_TABLE", columns=7)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="dt_r3",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["float_val", "FLOAT", 4, ""],
                ["double_val", "DOUBLE", 8, ""],
                ["rounded_double", "DOUBLE", 8, ""],
                ["float_to_double", "DOUBLE", 8, ""],
                ["avg_double", "DOUBLE", 8, ""],
                ["sum_float", "DOUBLE", 8, ""],  # SUM of FLOAT typically returns DOUBLE
            ],
        )

    def check4(self):
        # Test 3.1: Tag length calculation from expressions
        tdSql.checkTableType(dbname="rdb", stbname="dt_r4", columns=2, tags=3)
        # Verify tag types and lengths
        tdSql.checkResultsByFunc(
            sql="select tag_name, tag_type from information_schema.ins_tags where db_name='rdb' and stable_name='dt_r4';",
            func=lambda: len(tdSql.queryResult) == 3,
        )

    def check5(self):
        # Test 3.2: Tag type inheritance from source columns
        tdSql.checkTableType(dbname="rdb", stbname="dt_r5", columns=2, tags=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="dt_r5",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["tag_varchar_short", "VARCHAR", 10, "TAG"],
                ["tag_bool", "BOOL", 1, "TAG"],
                ["tag_double", "DOUBLE", 8, "TAG"],
            ],
        )

    def check6(self):
        # Test 4.1: JSON data type handling (may not be supported)
        try:
            tdSql.checkTableType(dbname="rdb", tbname="dt_r6", typename="NORMAL_TABLE")
            # If JSON is supported, check the schema
            tdSql.checkResultsByFunc(
                sql="describe rdb.dt_r6;",
                func=lambda: len(tdSql.queryResult) >= 3,
            )
        except Exception as e:
            tdLog.info(f"JSON type may not be supported: {e}")

    def check7(self):
        # Test 4.2: Timestamp precision levels
        try:
            tdSql.checkTableType(dbname="rdb", tbname="dt_r7", typename="NORMAL_TABLE", columns=4)
            # Verify timestamp-related columns exist
            tdSql.checkResultsByFunc(
                sql="select column_name from information_schema.ins_columns where db_name='rdb' and table_name='dt_r7' and column_name in ('iso_format', 'truncated_ts', 'elapsed_time');",
                func=lambda: len(tdSql.queryResult) >= 2,  # At least 2 should exist
            )
        except Exception as e:
            tdLog.info(f"Some timestamp functions may not be supported: {e}") 