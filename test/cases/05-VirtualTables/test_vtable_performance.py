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
"""
Virtual Table Performance Test Suite

This module provides comprehensive performance tests for virtual table functionality,
covering:
1. Large dataset query performance (100K rows)
2. Wide table query performance (100/500 columns)
3. Aggregation function performance
4. Filter query performance
5. Cross-database reference performance
6. Query stability (repeated execution)
7. String function performance

Since: v3.3.6.0
"""

from new_test_framework.utils import tdLog, tdSql, etool, tdCom
import time


class TestVtablePerformance:
    """Performance test suite for virtual table functionality."""

    DB_NAME = "test_vtable_perf"
    CROSS_DB_NAME = "test_vtable_perf_cross"

    # Performance thresholds (in seconds) - adjust based on hardware
    THRESHOLD_LARGE_DATA_QUERY = 10.0      # 100K rows query
    THRESHOLD_AGG_QUERY = 5.0               # Aggregation query
    THRESHOLD_FILTER_QUERY = 3.0            # Filter query
    THRESHOLD_WIDE_TABLE_100COL = 3.0       # 100 columns projection
    THRESHOLD_WIDE_TABLE_500COL = 8.0       # 500 columns projection
    THRESHOLD_CROSS_DB_QUERY = 5.0          # Cross-db query

    def setup_class(cls):
        """Prepare databases and source tables for performance testing."""
        tdLog.info("=== Performance Test Setup ===")

        # Clean up existing databases
        tdSql.execute(f"drop database if exists {cls.DB_NAME};")
        tdSql.execute(f"drop database if exists {cls.CROSS_DB_NAME};")

        # Create main database
        tdSql.execute(f"create database {cls.DB_NAME} vgroups 2;")
        tdSql.execute(f"use {cls.DB_NAME};")

        # Create cross-database for cross-db performance tests
        tdSql.execute(f"create database {cls.CROSS_DB_NAME} vgroups 1;")

    def _measure_query_time(self, sql, description="Query"):
        """Helper to measure query execution time.

        Args:
            sql: SQL query string
            description: Description for logging

        Returns:
            Execution time in seconds
        """
        start_time = time.time()
        tdSql.query(sql)
        elapsed = time.time() - start_time
        tdLog.info(f"{description} took {elapsed:.3f} seconds, returned {tdSql.queryRows} rows")
        return elapsed

    def _prepare_large_dataset(self, num_rows=100000):
        """Prepare a large dataset for performance testing.

        Args:
            num_rows: Number of rows to insert
        """
        tdLog.info(f"Preparing large dataset with {num_rows} rows...")
        tdSql.execute(f"use {self.DB_NAME};")

        # Create source table
        tdSql.execute("CREATE TABLE `src_large` ("
                      "ts timestamp, "
                      "int_col int, "
                      "bigint_col bigint, "
                      "float_col float, "
                      "double_col double, "
                      "binary_col binary(64))")

        # Batch insert for better performance
        batch_size = 1000
        base_ts = 1700000000000

        for batch in range(0, num_rows, batch_size):
            sql = "INSERT INTO src_large VALUES "
            values = []
            for i in range(batch, min(batch + batch_size, num_rows)):
                ts = base_ts + i
                val = i
                values.append(f"({ts}, {val}, {val * 1000}, {val * 0.1}, "
                             f"{val * 0.01}, 'data_{i}')")
            sql += ", ".join(values)
            tdSql.execute(sql)

            if (batch + batch_size) % 10000 == 0:
                tdLog.info(f"Inserted {batch + batch_size} rows...")

        tdLog.info(f"Large dataset preparation complete: {num_rows} rows")

    def _prepare_wide_table(self, num_cols=100):
        """Prepare a wide table with many columns.

        Args:
            num_cols: Number of non-timestamp columns
        """
        tdLog.info(f"Preparing wide table with {num_cols} columns...")
        tdSql.execute(f"use {self.DB_NAME};")

        # Create wide source table
        sql = "CREATE TABLE `src_wide` (ts timestamp"
        for i in range(num_cols):
            sql += f", col_{i} double"
        sql += ")"
        tdSql.execute(sql)

        # Insert first row
        sql = f"INSERT INTO src_wide VALUES (1700000000000"
        for i in range(num_cols):
            sql += f", {i * 0.1}"
        sql += ")"
        tdSql.execute(sql)

        # Insert more rows for aggregation tests
        for row in range(1, 100):
            ts_val = 1700000000000 + row * 1000
            sql = f"INSERT INTO src_wide VALUES ({ts_val}"
            for i in range(num_cols):
                sql += f", {(i + row) * 0.1}"
            sql += ")"
            tdSql.execute(sql)

        tdLog.info(f"Wide table preparation complete: {num_cols} columns, 100 rows")

    def _prepare_cross_db_data(self, num_rows=10000):
        """Prepare data in cross-database for cross-db performance tests."""
        tdLog.info(f"Preparing cross-database data with {num_rows} rows...")
        tdSql.execute(f"use {self.CROSS_DB_NAME};")

        tdSql.execute("CREATE TABLE `cross_src` ("
                      "ts timestamp, "
                      "val int, "
                      "name binary(32))")

        # Batch insert
        batch_size = 1000
        base_ts = 1700000000000

        for batch in range(0, num_rows, batch_size):
            sql = "INSERT INTO cross_src VALUES "
            values = []
            for i in range(batch, min(batch + batch_size, num_rows)):
                values.append(f"({base_ts + i}, {i}, 'cross_{i}')")
            sql += ", ".join(values)
            tdSql.execute(sql)

        tdLog.info(f"Cross-database data preparation complete: {num_rows} rows")

    def test_perf_large_dataset_basic_query(self):
        """Performance: Basic query on large dataset (100K rows)

        Measure query performance on virtual table referencing
        a source table with 100,000 rows.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance, large-data

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Large Dataset Basic Query Performance ===")

        num_rows = 100000
        self._prepare_large_dataset(num_rows)

        tdSql.execute(f"use {self.DB_NAME};")

        # Create virtual table (with explicit types for referenced columns)
        tdSql.execute("CREATE VTABLE `vtb_large` ("
                      "ts timestamp, "
                      "v_int int from src_large.int_col, "
                      "v_bigint bigint from src_large.bigint_col, "
                      "v_float float from src_large.float_col, "
                      "v_double double from src_large.double_col, "
                      "v_binary binary(64) from src_large.binary_col)")

        # Test 1: Full table scan
        elapsed = self._measure_query_time(
            "SELECT COUNT(*) FROM vtb_large;",
            "Full table count"
        )
        assert elapsed < self.THRESHOLD_LARGE_DATA_QUERY, \
            f"Count query too slow: {elapsed:.3f}s > {self.THRESHOLD_LARGE_DATA_QUERY}s"
        tdSql.checkData(0, 0, num_rows)

        # Test 2: Projection with limit
        elapsed = self._measure_query_time(
            "SELECT ts, v_int, v_binary FROM vtb_large LIMIT 1000;",
            "Projection with limit 1000"
        )
        assert elapsed < 2.0, f"Projection query too slow: {elapsed:.3f}s"
        tdSql.checkRows(1000)

        # Test 3: Time range query
        elapsed = self._measure_query_time(
            f"SELECT * FROM vtb_large WHERE ts >= 1700000000000 AND ts < 1700001000000;",
            "Time range query (100K rows)"
        )
        assert elapsed < self.THRESHOLD_LARGE_DATA_QUERY, \
            f"Time range query too slow: {elapsed:.3f}s"

        tdLog.info(f"Large dataset basic query performance test passed")

    def test_perf_large_dataset_aggregation(self):
        """Performance: Aggregation queries on large dataset

        Measure aggregation function performance on virtual table
        with 100,000 rows.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance, aggregation

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Large Dataset Aggregation Performance ===")

        tdSql.execute(f"use {self.DB_NAME};")

        # Test SUM
        elapsed = self._measure_query_time(
            "SELECT SUM(v_int), SUM(v_bigint), SUM(v_double) FROM vtb_large;",
            "SUM aggregation"
        )
        assert elapsed < self.THRESHOLD_AGG_QUERY, \
            f"SUM query too slow: {elapsed:.3f}s > {self.THRESHOLD_AGG_QUERY}s"

        # Test AVG
        elapsed = self._measure_query_time(
            "SELECT AVG(v_int), AVG(v_float), AVG(v_double) FROM vtb_large;",
            "AVG aggregation"
        )
        assert elapsed < self.THRESHOLD_AGG_QUERY, \
            f"AVG query too slow: {elapsed:.3f}s > {self.THRESHOLD_AGG_QUERY}s"

        # Test MIN/MAX
        elapsed = self._measure_query_time(
            "SELECT MIN(v_int), MAX(v_int), MIN(v_double), MAX(v_double) FROM vtb_large;",
            "MIN/MAX aggregation"
        )
        assert elapsed < self.THRESHOLD_AGG_QUERY, \
            f"MIN/MAX query too slow: {elapsed:.3f}s > {self.THRESHOLD_AGG_QUERY}s"

        # Test FIRST/LAST
        elapsed = self._measure_query_time(
            "SELECT FIRST(v_int), LAST(v_int), FIRST(v_binary), LAST(v_binary) FROM vtb_large;",
            "FIRST/LAST aggregation"
        )
        assert elapsed < self.THRESHOLD_AGG_QUERY, \
            f"FIRST/LAST query too slow: {elapsed:.3f}s > {self.THRESHOLD_AGG_QUERY}s"

        # Test GROUP BY
        elapsed = self._measure_query_time(
            "SELECT v_int % 1000 as grp, COUNT(*), AVG(v_double) "
            "FROM vtb_large GROUP BY v_int % 1000 ORDER BY grp;",
            "GROUP BY aggregation (100 groups)"
        )
        assert elapsed < self.THRESHOLD_AGG_QUERY * 2, \
            f"GROUP BY query too slow: {elapsed:.3f}s"

        tdLog.info("Large dataset aggregation performance test passed")

    def test_perf_large_dataset_filter(self):
        """Performance: Filter queries on large dataset

        Measure filter query performance with various conditions
        on virtual table with 100,000 rows.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance, filter

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Large Dataset Filter Performance ===")

        tdSql.execute(f"use {self.DB_NAME};")

        # Test equality filter
        elapsed = self._measure_query_time(
            "SELECT * FROM vtb_large WHERE v_int = 50000;",
            "Equality filter (single row)"
        )
        assert elapsed < self.THRESHOLD_FILTER_QUERY, \
            f"Equality filter too slow: {elapsed:.3f}s"
        tdSql.checkRows(1)

        # Test range filter
        elapsed = self._measure_query_time(
            "SELECT * FROM vtb_large WHERE v_int >= 10000 AND v_int < 20000;",
            "Range filter (10K rows)"
        )
        assert elapsed < self.THRESHOLD_FILTER_QUERY, \
            f"Range filter too slow: {elapsed:.3f}s"
        tdSql.checkRows(10000)

        # Test string filter with LIKE
        elapsed = self._measure_query_time(
            "SELECT * FROM vtb_large WHERE v_binary LIKE 'data_1%';",
            "LIKE filter on string column"
        )
        assert elapsed < self.THRESHOLD_FILTER_QUERY * 2, \
            f"LIKE filter too slow: {elapsed:.3f}s"

        # Test combined filter
        elapsed = self._measure_query_time(
            "SELECT * FROM vtb_large WHERE v_int > 50000 AND v_double > 500.0;",
            "Combined numeric filter"
        )
        assert elapsed < self.THRESHOLD_FILTER_QUERY, \
            f"Combined filter too slow: {elapsed:.3f}s"

        tdLog.info("Large dataset filter performance test passed")

    def test_perf_wide_table_100_columns(self):
        """Performance: Wide virtual table query (100 columns)

        Measure query performance on virtual table with 100 columns.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance, wide-table

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Wide Table 100 Columns Performance ===")

        num_cols = 100
        self._prepare_wide_table(num_cols)

        tdSql.execute(f"use {self.DB_NAME};")

        # Create virtual table with 100 columns (with explicit types)
        sql = "CREATE VTABLE `vtb_wide_100` (ts timestamp"
        for i in range(num_cols):
            sql += f", v_col_{i} double from src_wide.col_{i}"
        sql += ")"
        tdSql.execute(sql)

        # Test 1: SELECT *
        elapsed = self._measure_query_time(
            "SELECT * FROM vtb_wide_100;",
            "Full projection (100 columns)"
        )
        assert elapsed < self.THRESHOLD_WIDE_TABLE_100COL, \
            f"Full projection too slow: {elapsed:.3f}s > {self.THRESHOLD_WIDE_TABLE_100COL}s"
        tdSql.checkCols(num_cols + 1)

        # Test 2: Partial projection (10 columns)
        col_list = ", ".join([f"v_col_{i}" for i in range(10)])
        elapsed = self._measure_query_time(
            f"SELECT ts, {col_list} FROM vtb_wide_100;",
            "Partial projection (10 columns)"
        )
        assert elapsed < 1.0, f"Partial projection too slow: {elapsed:.3f}s"
        tdSql.checkCols(11)

        # Test 3: Aggregation on wide table
        agg_list = ", ".join([f"SUM(v_col_{i})" for i in range(10)])
        elapsed = self._measure_query_time(
            f"SELECT {agg_list} FROM vtb_wide_100;",
            "Aggregation on 10 columns"
        )
        assert elapsed < 2.0, f"Wide table aggregation too slow: {elapsed:.3f}s"

        tdLog.info("Wide table 100 columns performance test passed")

    def test_perf_wide_table_500_columns(self):
        """Performance: Wide virtual table query (500 columns)

        Measure query performance on virtual table with 500 columns.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance, wide-table

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Wide Table 500 Columns Performance ===")

        num_cols = 500

        tdSql.execute(f"use {self.DB_NAME};")

        # Create wide source table
        sql = "CREATE TABLE `src_wide_500` (ts timestamp"
        for i in range(num_cols):
            sql += f", col_{i} double"
        sql += ")"
        tdSql.execute(sql)

        # Insert single row (for simplicity)
        sql = f"INSERT INTO src_wide_500 VALUES (1700000000000"
        for i in range(num_cols):
            sql += f", {i * 0.1}"
        sql += ")"
        tdSql.execute(sql)

        # Create virtual table (with explicit types)
        sql = "CREATE VTABLE `vtb_wide_500` (ts timestamp"
        for i in range(num_cols):
            sql += f", v_col_{i} double from src_wide_500.col_{i}"
        sql += ")"
        tdSql.execute(sql)

        # Test: SELECT * with 500 columns
        elapsed = self._measure_query_time(
            "SELECT * FROM vtb_wide_500;",
            "Full projection (500 columns)"
        )
        assert elapsed < self.THRESHOLD_WIDE_TABLE_500COL, \
            f"500 column projection too slow: {elapsed:.3f}s > {self.THRESHOLD_WIDE_TABLE_500COL}s"
        tdSql.checkCols(num_cols + 1)

        # Test: Partial projection (50 columns)
        col_list = ", ".join([f"v_col_{i}" for i in range(50)])
        elapsed = self._measure_query_time(
            f"SELECT ts, {col_list} FROM vtb_wide_500;",
            "Partial projection (50 columns)"
        )
        assert elapsed < 2.0, f"Partial projection too slow: {elapsed:.3f}s"

        tdLog.info("Wide table 500 columns performance test passed")

    def test_perf_cross_database_query(self):
        """Performance: Cross-database reference query

        Measure query performance on virtual table referencing
        columns from a different database.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance, cross-db

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Cross-Database Query Performance ===")

        num_rows = 10000
        self._prepare_cross_db_data(num_rows)

        tdSql.execute(f"use {self.DB_NAME};")

        # Create virtual table referencing cross-database table
        tdSql.execute(f"CREATE VTABLE `vtb_cross_db` ("
                      "ts timestamp, "
                      f"v_val int from {self.CROSS_DB_NAME}.cross_src.val, "
                      f"v_name binary(32) from {self.CROSS_DB_NAME}.cross_src.name)")

        # Test 1: Cross-db count
        elapsed = self._measure_query_time(
            "SELECT COUNT(*) FROM vtb_cross_db;",
            "Cross-db count"
        )
        assert elapsed < self.THRESHOLD_CROSS_DB_QUERY, \
            f"Cross-db count too slow: {elapsed:.3f}s"
        tdSql.checkData(0, 0, num_rows)

        # Test 2: Cross-db aggregation
        elapsed = self._measure_query_time(
            "SELECT SUM(v_val), AVG(v_val), MIN(v_val), MAX(v_val) FROM vtb_cross_db;",
            "Cross-db aggregation"
        )

        # Test 3: Cross-db filter
        elapsed = self._measure_query_time(
            "SELECT * FROM vtb_cross_db WHERE v_val >= 5000 AND v_val < 6000;",
            "Cross-db filter"
        )

        tdLog.info("Cross-database query performance test passed")

    def test_perf_query_stability(self):
        """Performance: Query stability (repeated execution)

        Measure query stability by executing the same query
        multiple times and checking for consistent performance.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance, stability

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Query Stability (Repeated Execution) ===")

        tdSql.execute(f"use {self.DB_NAME};")

        # Use existing vtb_large from previous test
        num_iterations = 10
        times = []

        for i in range(num_iterations):
            elapsed = self._measure_query_time(
                "SELECT COUNT(*), AVG(v_int), AVG(v_double) FROM vtb_large;",
                f"Iteration {i+1}/{num_iterations}"
            )
            times.append(elapsed)

        # Calculate statistics
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        variance = max_time - min_time

        tdLog.info(f"Query stability stats: avg={avg_time:.3f}s, "
                  f"min={min_time:.3f}s, max={max_time:.3f}s, variance={variance:.3f}s")

        # Variance should not exceed 50% of average
        assert variance < avg_time * 0.5, \
            f"Query time variance too high: {variance:.3f}s > {avg_time * 0.5:.3f}s"

        # No single query should be excessively slow
        assert max_time < self.THRESHOLD_AGG_QUERY * 2, \
            f"Single query too slow: {max_time:.3f}s"

        tdLog.info("Query stability test passed")

    def test_perf_string_functions(self):
        """Performance: String function queries

        Measure string function performance on virtual tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance, string-functions

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: String Function Performance ===")

        tdSql.execute(f"use {self.DB_NAME};")

        # Create source table with string columns
        tdSql.execute("CREATE TABLE `src_str` ("
                      "ts timestamp, "
                      "nchar_col nchar(128), "
                      "binary_col binary(128))")

        # Insert 1000 rows with string data
        sql = "INSERT INTO src_str VALUES "
        values = []
        for i in range(1000):
            ts = 1700000000000 + i
            values.append(f"({ts}, '中文字符串_{i}', 'binary_string_{i}')")
        sql += ", ".join(values)
        tdSql.execute(sql)

        # Create virtual table
        tdSql.execute("CREATE VTABLE `vtb_str` ("
                      "ts timestamp, "
                      "v_nchar nchar(128) from src_str.nchar_col, "
                      "v_binary binary(128) from src_str.binary_col)")

        # Test 1: length/char_length
        elapsed = self._measure_query_time(
            "SELECT length(v_binary), char_length(v_nchar) FROM vtb_str;",
            "String length functions"
        )
        assert elapsed < 3.0, f"Length functions too slow: {elapsed:.3f}s"
        tdSql.checkRows(1000)

        # Test 2: lower/upper
        elapsed = self._measure_query_time(
            "SELECT lower(v_binary), upper(v_binary) FROM vtb_str;",
            "Lower/upper functions"
        )
        assert elapsed < 3.0, f"Lower/upper too slow: {elapsed:.3f}s"

        # Test 3: concat
        elapsed = self._measure_query_time(
            "SELECT concat(v_binary, '_suffix') FROM vtb_str;",
            "Concat function"
        )
        assert elapsed < 3.0, f"Concat too slow: {elapsed:.3f}s"

        # Test 4: substring
        elapsed = self._measure_query_time(
            "SELECT substring(v_binary, 1, 10) FROM vtb_str;",
            "Substring function"
        )
        assert elapsed < 3.0, f"Substring too slow: {elapsed:.3f}s"

        tdLog.info("String function performance test passed")

    def test_perf_interval_query(self):
        """Performance: Interval (time window) query

        Measure interval/time window query performance on virtual tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance, interval

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Interval Query Performance ===")

        tdSql.execute(f"use {self.DB_NAME};")

        # Create source table for interval test
        tdSql.execute("CREATE TABLE IF NOT EXISTS `src_interval` ("
                      "ts timestamp, "
                      "val int, "
                      "dval double)")

        # Insert data for interval testing (10000 rows, 1 second intervals)
        batch_size = 1000
        for batch in range(0, 10000, batch_size):
            sql = "INSERT INTO src_interval VALUES "
            values = []
            for i in range(batch, min(batch + batch_size, 10000)):
                ts = 1700000000000 + i * 1000
                values.append(f"({ts}, {i}, {i * 0.1})")
            sql += ", ".join(values)
            tdSql.execute(sql)

        # Create virtual table for interval test
        tdSql.execute("CREATE VTABLE IF NOT EXISTS `vtb_interval` ("
                      "ts timestamp, "
                      "v_int int from src_interval.val, "
                      "v_double double from src_interval.dval)")

        # Test interval query with time range (1 second windows)
        elapsed = self._measure_query_time(
            "SELECT _wstart, COUNT(*), AVG(v_int), AVG(v_double) "
            "FROM vtb_interval "
            "WHERE ts >= 1700000000000 AND ts < 1700001000000 "
            "INTERVAL(1s) FILL(NULL) "
            "ORDER BY _wstart;",
            "Interval query (1s windows)"
        )
        assert elapsed < self.THRESHOLD_AGG_QUERY * 2, \
            f"Interval query too slow: {elapsed:.3f}s"

        # Test interval query with larger windows (10 seconds)
        elapsed = self._measure_query_time(
            "SELECT _wstart, COUNT(*), SUM(v_int) "
            "FROM vtb_interval "
            "WHERE ts >= 1700000000000 AND ts < 1700010000000 "
            "INTERVAL(10s) "
            "ORDER BY _wstart;",
            "Interval query (10s windows)"
        )

    def test_perf_mixed_column_references(self):
        """Performance: Mixed column references from multiple tables

        Measure performance of virtual table with columns referencing
        multiple different source tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance, mixed-refs

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Mixed Column References Performance ===")

        tdSql.execute(f"use {self.DB_NAME};")

        # Create multiple source tables
        for t in range(5):
            tdSql.execute(f"CREATE TABLE `src_mix_{t}` ("
                          "ts timestamp, "
                          f"value_{t} double, "
                          f"name_{t} binary(32))")

            # Insert data
            sql = f"INSERT INTO src_mix_{t} VALUES "
            values = []
            for i in range(1000):
                ts = 1700000000000 + i
                values.append(f"({ts}, {i * 0.1 * (t + 1)}, 'name_{t}_{i}')")
            sql += ", ".join(values)
            tdSql.execute(sql)

        # Create virtual table with mixed references
        tdSql.execute("CREATE VTABLE `vtb_mixed` ("
                      "ts timestamp, "
                      "v0 double from src_mix_0.value_0, "
                      "v1 double from src_mix_1.value_1, "
                      "v2 double from src_mix_2.value_2, "
                      "v3 double from src_mix_3.value_3, "
                      "v4 double from src_mix_4.value_4)")

        # Test projection
        elapsed = self._measure_query_time(
            "SELECT * FROM vtb_mixed;",
            "Mixed references projection"
        )
        assert elapsed < 5.0, f"Mixed references projection too slow: {elapsed:.3f}s"
        tdSql.checkRows(1000)

        # Test aggregation
        elapsed = self._measure_query_time(
            "SELECT AVG(v0), AVG(v1), AVG(v2), AVG(v3), AVG(v4) FROM vtb_mixed;",
            "Mixed references aggregation"
        )
        assert elapsed < 3.0, f"Mixed references aggregation too slow: {elapsed:.3f}s"

        tdLog.info("Mixed column references performance test passed")

    def test_perf_tag_reference_query(self):
        """Performance: Virtual table with tag column references

        Measure query performance on virtual child table with
        tag column references.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Tag Reference Query Performance ===")

        tdSql.execute(f"use {self.DB_NAME};")

        # Create source super table and child tables
        tdSql.execute("CREATE STABLE `src_tag_stb` ("
                      "ts timestamp, "
                      "val double) TAGS ("
                      "t_id int, "
                      "t_name binary(32))")

        for i in range(10):
            tdSql.execute(f"CREATE TABLE `src_ctb_{i}` USING src_tag_stb "
                         f"TAGS ({i}, 'device_{i}')")

            # Insert data
            sql = f"INSERT INTO src_ctb_{i} VALUES "
            values = []
            for j in range(1000):
                ts = 1700000000000 + j
                values.append(f"({ts}, {i * 100 + j * 0.1})")
            sql += ", ".join(values)
            tdSql.execute(sql)

        # Create virtual super table
        tdSql.execute("CREATE STABLE `vtb_tag_stb` ("
                      "ts timestamp, "
                      "v_val double) TAGS ("
                      "vt_id int, "
                      "vt_name binary(32)) VIRTUAL 1")

        # Create virtual child tables with literal tag values
        for i in range(10):
            tdSql.execute(f"CREATE VTABLE `vctb_{i}` ("
                         f"v_val from src_ctb_{i}.val) "
                         f"USING vtb_tag_stb TAGS ({i}, 'device_{i}')")

        # Test: Query single virtual child table
        elapsed = self._measure_query_time(
            "SELECT COUNT(*), AVG(v_val) FROM vctb_5;",
            "Single vctb query"
        )
        assert elapsed < 3.0, f"Single vctb query too slow: {elapsed:.3f}s"

        # Test: Query virtual super table (all children)
        elapsed = self._measure_query_time(
            "SELECT COUNT(*), SUM(v_val) FROM vtb_tag_stb;",
            "Virtual super table aggregation"
        )

    def teardown_class(cls):
        """Clean up performance test databases."""
        tdLog.info("=== Performance Test Cleanup ===")
        tdSql.execute(f"drop database if exists {cls.DB_NAME};")
        tdSql.execute(f"drop database if exists {cls.CROSS_DB_NAME};")
        tdLog.info("Performance test cleanup complete")
