###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, etool, tdCom


class TestVtablePerformance:

    def setup_class(cls):
        tdLog.info(f"prepare databases and tables for performance testing.")

        tdSql.execute("drop database if exists test_vtable_perf;")
        tdSql.execute("drop database if exists test_vtable_perf_cross;")
        tdSql.execute("create database test_vtable_perf vgroups 2;")
        tdSql.execute("use test_vtable_perf;")
        tdSql.execute("create database test_vtable_perf_cross vgroups 1;")

        # large dataset source table (10K rows)
        tdLog.info(f"prepare large dataset source table.")
        tdSql.execute("CREATE TABLE `src_large` ("
                      "ts timestamp, "
                      "int_col int, "
                      "bigint_col bigint, "
                      "float_col float, "
                      "double_col double, "
                      "binary_col binary(64))")

        base_ts = 1700000000000
        for batch in range(0, 10000, 1000):
            sql = "INSERT INTO src_large VALUES "
            values = []
            for i in range(batch, min(batch + 1000, 10000)):
                values.append(f"({base_ts + i}, {i}, {i * 1000}, {i * 0.1}, "
                              f"{i * 0.01}, 'data_{i}')")
            sql += ", ".join(values)
            tdSql.execute(sql)

        tdSql.execute("CREATE VTABLE `vtb_large` ("
                      "ts timestamp, "
                      "v_int int from src_large.int_col, "
                      "v_bigint bigint from src_large.bigint_col, "
                      "v_float float from src_large.float_col, "
                      "v_double double from src_large.double_col, "
                      "v_binary binary(64) from src_large.binary_col)")

        # wide source table (100 columns, 100 rows)
        tdLog.info(f"prepare wide source table (100 columns).")
        sql = "CREATE TABLE `src_wide` (ts timestamp"
        for i in range(100):
            sql += f", col_{i} double"
        sql += ")"
        tdSql.execute(sql)

        for row in range(100):
            ts_val = 1700000000000 + row * 1000
            sql = f"INSERT INTO src_wide VALUES ({ts_val}"
            for i in range(100):
                sql += f", {(i + row) * 0.1}"
            sql += ")"
            tdSql.execute(sql)

        sql = "CREATE VTABLE `vtb_wide_100` (ts timestamp"
        for i in range(100):
            sql += f", v_col_{i} double from src_wide.col_{i}"
        sql += ")"
        tdSql.execute(sql)

        # wide source table (500 columns, 1 row)
        tdLog.info(f"prepare wide source table (500 columns).")
        sql = "CREATE TABLE `src_wide_500` (ts timestamp"
        for i in range(500):
            sql += f", col_{i} double"
        sql += ")"
        tdSql.execute(sql)

        sql = f"INSERT INTO src_wide_500 VALUES (1700000000000"
        for i in range(500):
            sql += f", {i * 0.1}"
        sql += ")"
        tdSql.execute(sql)

        sql = "CREATE VTABLE `vtb_wide_500` (ts timestamp"
        for i in range(500):
            sql += f", v_col_{i} double from src_wide_500.col_{i}"
        sql += ")"
        tdSql.execute(sql)

        # cross-database source table (10K rows)
        tdLog.info(f"prepare cross-database source table.")
        tdSql.execute("use test_vtable_perf_cross;")
        tdSql.execute("CREATE TABLE `cross_src` ("
                      "ts timestamp, "
                      "val int, "
                      "name binary(32))")

        for batch in range(0, 10000, 1000):
            sql = "INSERT INTO cross_src VALUES "
            values = []
            for i in range(batch, min(batch + 1000, 10000)):
                values.append(f"({base_ts + i}, {i}, 'cross_{i}')")
            sql += ", ".join(values)
            tdSql.execute(sql)

        tdSql.execute("use test_vtable_perf;")
        tdSql.execute("CREATE VTABLE `vtb_cross_db` ("
                      "ts timestamp, "
                      "v_val int from test_vtable_perf_cross.cross_src.val, "
                      "v_name binary(32) from test_vtable_perf_cross.cross_src.name)")

        # string source table (1K rows)
        tdLog.info(f"prepare string source table.")
        tdSql.execute("CREATE TABLE `src_str` ("
                      "ts timestamp, "
                      "nchar_col nchar(128), "
                      "binary_col binary(128))")

        sql = "INSERT INTO src_str VALUES "
        values = []
        for i in range(1000):
            values.append(f"({base_ts + i}, '中文字符串_{i}', 'binary_string_{i}')")
        sql += ", ".join(values)
        tdSql.execute(sql)

        tdSql.execute("CREATE VTABLE `vtb_str` ("
                      "ts timestamp, "
                      "v_nchar nchar(128) from src_str.nchar_col, "
                      "v_binary binary(128) from src_str.binary_col)")

        # interval source table (10K rows, 1s apart)
        tdLog.info(f"prepare interval source table.")
        tdSql.execute("CREATE TABLE `src_interval` ("
                      "ts timestamp, "
                      "val int, "
                      "dval double)")

        for batch in range(0, 10000, 1000):
            sql = "INSERT INTO src_interval VALUES "
            values = []
            for i in range(batch, min(batch + 1000, 10000)):
                values.append(f"({base_ts + i * 1000}, {i}, {i * 0.1})")
            sql += ", ".join(values)
            tdSql.execute(sql)

        tdSql.execute("CREATE VTABLE `vtb_interval` ("
                      "ts timestamp, "
                      "v_int int from src_interval.val, "
                      "v_double double from src_interval.dval)")

        # mixed reference source tables (5 tables, 1K rows each)
        tdLog.info(f"prepare mixed reference source tables.")
        for t in range(5):
            tdSql.execute(f"CREATE TABLE `src_mix_{t}` ("
                          "ts timestamp, "
                          f"value_{t} double, "
                          f"name_{t} binary(32))")

            sql = f"INSERT INTO src_mix_{t} VALUES "
            values = []
            for i in range(1000):
                values.append(f"({base_ts + i}, {i * 0.1 * (t + 1)}, 'name_{t}_{i}')")
            sql += ", ".join(values)
            tdSql.execute(sql)

        tdSql.execute("CREATE VTABLE `vtb_mixed` ("
                      "ts timestamp, "
                      "v0 double from src_mix_0.value_0, "
                      "v1 double from src_mix_1.value_1, "
                      "v2 double from src_mix_2.value_2, "
                      "v3 double from src_mix_3.value_3, "
                      "v4 double from src_mix_4.value_4)")

        # tag reference source tables
        tdLog.info(f"prepare tag reference source tables.")
        tdSql.execute("CREATE STABLE `src_tag_stb` ("
                      "ts timestamp, "
                      "val double) TAGS ("
                      "t_id int, "
                      "t_name binary(32))")

        for i in range(10):
            tdSql.execute(f"CREATE TABLE `src_ctb_{i}` USING src_tag_stb "
                          f"TAGS ({i}, 'device_{i}')")

            sql = f"INSERT INTO src_ctb_{i} VALUES "
            values = []
            for j in range(1000):
                values.append(f"({base_ts + j}, {i * 100 + j * 0.1})")
            sql += ", ".join(values)
            tdSql.execute(sql)

        tdSql.execute("CREATE STABLE `vtb_tag_stb` ("
                      "ts timestamp, "
                      "v_val double) TAGS ("
                      "vt_id int, "
                      "vt_name binary(32)) VIRTUAL 1")

        for i in range(10):
            tdSql.execute(f"CREATE VTABLE `vctb_{i}` ("
                          f"v_val from src_ctb_{i}.val) "
                          f"USING vtb_tag_stb TAGS ({i}, 'device_{i}')")

    def test_perf_large_dataset_basic_query(self):
        """Performance: basic query on 10K-row virtual table

        1. COUNT on 10K rows
        2. projection with LIMIT
        3. time range filter

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test large dataset basic query performance.")
        tdSql.execute("use test_vtable_perf;")

        tdSql.query("SELECT COUNT(*) FROM vtb_large;")
        tdSql.checkData(0, 0, 10000)

        tdSql.query("SELECT ts, v_int, v_binary FROM vtb_large LIMIT 1000;")
        tdSql.checkRows(1000)

        tdSql.query("SELECT * FROM vtb_large WHERE ts >= 1700000000000 AND ts < 1700000001000;")
        tdSql.checkRows(1000)

    def test_perf_large_dataset_aggregation(self):
        """Performance: aggregation queries on 10K-row virtual table

        1. SUM aggregation
        2. AVG aggregation
        3. MIN/MAX aggregation
        4. FIRST/LAST aggregation
        5. GROUP BY aggregation

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test large dataset aggregation performance.")
        tdSql.execute("use test_vtable_perf;")

        tdSql.query("SELECT SUM(v_int), SUM(v_bigint), SUM(v_double) FROM vtb_large;")
        tdSql.checkRows(1)

        tdSql.query("SELECT AVG(v_int), AVG(v_float), AVG(v_double) FROM vtb_large;")
        tdSql.checkRows(1)

        tdSql.query("SELECT MIN(v_int), MAX(v_int), MIN(v_double), MAX(v_double) FROM vtb_large;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 9999)

        tdSql.query("SELECT FIRST(v_int), LAST(v_int), FIRST(v_binary), LAST(v_binary) FROM vtb_large;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 9999)

        tdSql.query("SELECT v_int % 100 as grp, COUNT(*), AVG(v_double) "
                     "FROM vtb_large GROUP BY v_int % 100 ORDER BY grp;")
        tdSql.checkRows(100)

    def test_perf_large_dataset_filter(self):
        """Performance: filter queries on 10K-row virtual table

        1. equality filter
        2. range filter
        3. LIKE filter on string column
        4. combined numeric filter

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test large dataset filter performance.")
        tdSql.execute("use test_vtable_perf;")

        tdSql.query("SELECT * FROM vtb_large WHERE v_int = 5000;")
        tdSql.checkRows(1)

        tdSql.query("SELECT * FROM vtb_large WHERE v_int >= 1000 AND v_int < 2000;")
        tdSql.checkRows(1000)

        tdSql.query("SELECT * FROM vtb_large WHERE v_binary LIKE 'data_1%';")
        tdSql.checkRows(1111)

        tdSql.query("SELECT * FROM vtb_large WHERE v_int > 5000 AND v_double > 50.0;")
        tdSql.checkRows(4999)

    def test_perf_wide_table_100_columns(self):
        """Performance: query on 100-column virtual table

        1. SELECT * full projection
        2. partial projection (10 columns)
        3. aggregation on 10 columns

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test wide table 100 columns performance.")
        tdSql.execute("use test_vtable_perf;")

        tdSql.query("SELECT * FROM vtb_wide_100;")
        tdSql.checkCols(101)
        tdSql.checkRows(100)

        col_list = ", ".join([f"v_col_{i}" for i in range(10)])
        tdSql.query(f"SELECT ts, {col_list} FROM vtb_wide_100;")
        tdSql.checkCols(11)
        tdSql.checkRows(100)

        agg_list = ", ".join([f"SUM(v_col_{i})" for i in range(10)])
        tdSql.query(f"SELECT {agg_list} FROM vtb_wide_100;")
        tdSql.checkRows(1)

    def test_perf_wide_table_500_columns(self):
        """Performance: query on 500-column virtual table

        1. SELECT * full projection
        2. partial projection (50 columns)

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test wide table 500 columns performance.")
        tdSql.execute("use test_vtable_perf;")

        tdSql.query("SELECT * FROM vtb_wide_500;")
        tdSql.checkCols(501)
        tdSql.checkRows(1)

        col_list = ", ".join([f"v_col_{i}" for i in range(50)])
        tdSql.query(f"SELECT ts, {col_list} FROM vtb_wide_500;")
        tdSql.checkCols(51)
        tdSql.checkRows(1)

    def test_perf_cross_database_query(self):
        """Performance: cross-database reference query

        1. cross-db count
        2. cross-db aggregation
        3. cross-db filter

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test cross-database query performance.")
        tdSql.execute("use test_vtable_perf;")

        tdSql.query("SELECT COUNT(*) FROM vtb_cross_db;")
        tdSql.checkData(0, 0, 10000)

        tdSql.query("SELECT SUM(v_val), AVG(v_val), MIN(v_val), MAX(v_val) FROM vtb_cross_db;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, 9999)

        tdSql.query("SELECT * FROM vtb_cross_db WHERE v_val >= 5000 AND v_val < 6000;")
        tdSql.checkRows(1000)

    def test_perf_query_stability(self):
        """Performance: repeated query stability

        Execute the same aggregation query 10 times on 10K-row vtable,
        verify results are consistent across iterations.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test query stability with repeated execution.")
        tdSql.execute("use test_vtable_perf;")

        for i in range(10):
            tdSql.query("SELECT COUNT(*), AVG(v_int), AVG(v_double) FROM vtb_large;")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 10000)

    def test_perf_string_functions(self):
        """Performance: string function queries on virtual table

        1. length / char_length
        2. lower / upper
        3. concat
        4. substring

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test string function performance.")
        tdSql.execute("use test_vtable_perf;")

        tdSql.query("SELECT length(v_binary), char_length(v_nchar) FROM vtb_str;")
        tdSql.checkRows(1000)

        tdSql.query("SELECT lower(v_binary), upper(v_binary) FROM vtb_str;")
        tdSql.checkRows(1000)

        tdSql.query("SELECT concat(v_binary, '_suffix') FROM vtb_str;")
        tdSql.checkRows(1000)

        tdSql.query("SELECT substring(v_binary, 1, 10) FROM vtb_str;")
        tdSql.checkRows(1000)

    def test_perf_interval_query(self):
        """Performance: interval (time window) query on virtual table

        1. 1-second interval windows
        2. 10-second interval windows

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test interval query performance.")
        tdSql.execute("use test_vtable_perf;")

        tdSql.query("SELECT _wstart, COUNT(*), AVG(v_int), AVG(v_double) "
                     "FROM vtb_interval "
                     "WHERE ts >= 1700000000000 AND ts < 1700001000000 "
                     "INTERVAL(1s) FILL(NULL) "
                     "ORDER BY _wstart;")
        tdSql.checkRows(1000)

        tdSql.query("SELECT _wstart, COUNT(*), SUM(v_int) "
                     "FROM vtb_interval "
                     "WHERE ts >= 1700000000000 AND ts < 1700010000000 "
                     "INTERVAL(10s) "
                     "ORDER BY _wstart;")
        tdSql.checkRows(1000)

    def test_perf_mixed_column_references(self):
        """Performance: virtual table with columns from 5 different source tables

        1. full projection
        2. aggregation across all referenced columns

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test mixed column references performance.")
        tdSql.execute("use test_vtable_perf;")

        tdSql.query("SELECT * FROM vtb_mixed;")
        tdSql.checkRows(1000)
        tdSql.checkCols(6)

        tdSql.query("SELECT AVG(v0), AVG(v1), AVG(v2), AVG(v3), AVG(v4) FROM vtb_mixed;")
        tdSql.checkRows(1)

    def test_perf_tag_reference_query(self):
        """Performance: virtual child table and super table queries

        1. single virtual child table query
        2. virtual super table aggregation (all children)

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test tag reference query performance.")
        tdSql.execute("use test_vtable_perf;")

        tdSql.query("SELECT COUNT(*), AVG(v_val) FROM vctb_5;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1000)

        tdSql.query("SELECT COUNT(*), SUM(v_val) FROM vtb_tag_stb;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10000)
