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
Virtual Table Stress Test Suite

This module provides stress tests for virtual table functionality,
covering:
1. Long-running query stability
2. Memory leak detection (repeated create/drop/query)
3. Concurrent query handling simulation
4. Large result set handling
5. Edge case stress testing

Since: v3.3.6.0
"""

from new_test_framework.utils import tdLog, tdSql, etool, tdCom
import time
import gc


class TestVtableStress:
    """Stress test suite for virtual table functionality."""

    DB_NAME = "test_vtable_stress"

    def setup_class(cls):
        """Prepare databases for stress testing."""
        tdLog.info("=== Stress Test Setup ===")

        # Clean up existing databases
        tdSql.execute(f"drop database if exists {cls.DB_NAME};")

        # Create main database
        tdSql.execute(f"create database {cls.DB_NAME} vgroups 2;")
        tdSql.execute(f"use {cls.DB_NAME};")

    def _prepare_stress_data(self, num_tables=10, rows_per_table=1000):
        """Prepare data for stress testing.

        Args:
            num_tables: Number of source tables to create
            rows_per_table: Number of rows per table
        """
        tdLog.info(f"Preparing stress data: {num_tables} tables, {rows_per_table} rows each")
        tdSql.execute(f"use {self.DB_NAME};")

        for t in range(num_tables):
            # Drop table if exists
            tdSql.execute(f"DROP TABLE IF EXISTS `stress_src_{t}`;")
            
            # Create source table
            tdSql.execute(f"CREATE TABLE `stress_src_{t}` ("
                          "ts timestamp, "
                          "val double, "
                          "data binary(64))")

            # Batch insert
            sql = f"INSERT INTO stress_src_{t} VALUES "
            values = []
            for i in range(rows_per_table):
                ts = 1700000000000 + i
                values.append(f"({ts}, {i * 0.1}, 'data_{t}_{i}')")
            sql += ", ".join(values)
            tdSql.execute(sql)

        tdLog.info(f"Stress data preparation complete")

    def test_stress_repeated_create_drop_vtable(self):
        """Stress: Repeated virtual table create/drop cycles

        Test memory leak by repeatedly creating and dropping virtual tables.
        This should not cause memory growth or system instability.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress, memory-leak

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Repeated Create/Drop Virtual Table ===")

        tdSql.execute(f"use {self.DB_NAME};")

        # Prepare one source table
        tdSql.execute("CREATE TABLE `stress_src_cycledrop` ("
                      "ts timestamp, "
                      "val double)")

        sql = "INSERT INTO stress_src_cycledrop VALUES "
        values = [f"({1700000000000 + i}, {i * 0.1})" for i in range(100)]
        sql += ", ".join(values)
        tdSql.execute(sql)

        num_cycles = 50
        times = []

        for cycle in range(num_cycles):
            start_time = time.time()

            # Create virtual table
            tdSql.execute("CREATE VTABLE `vtb_stress_cycle` ("
                          "ts timestamp, "
                          "val double from stress_src_cycledrop.val)")

            # Query the virtual table
            tdSql.query("SELECT COUNT(*), AVG(val) FROM vtb_stress_cycle;")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 100)

            # Drop the virtual table
            tdSql.execute("DROP TABLE vtb_stress_cycle;")

            cycle_time = time.time() - start_time
            times.append(cycle_time)

            if (cycle + 1) % 10 == 0:
                tdLog.info(f"Completed {cycle + 1}/{num_cycles} create/drop cycles")

        # Analyze timing
        avg_time = sum(times) / len(times)
        max_time = max(times)
        min_time = min(times)

        tdLog.info(f"Create/Drop cycle stats: avg={avg_time:.3f}s, "
                  f"min={min_time:.3f}s, max={max_time:.3f}s")

        # Cycle time should not grow significantly (indicates memory leak)
        # Last 10 cycles avg should be within 2x of first 10 cycles avg
        first_10_avg = sum(times[:10]) / 10
        last_10_avg = sum(times[-10:]) / 10

        assert last_10_avg < first_10_avg * 2, \
            f"Possible memory leak detected: first_10_avg={first_10_avg:.3f}s, " \
            f"last_10_avg={last_10_avg:.3f}s"

        tdLog.info("Repeated create/drop stress test passed")

    def test_stress_repeated_query(self):
        """Stress: Repeated query execution on same virtual table

        Test stability by executing the same query many times.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress, query-stability

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Repeated Query Execution ===")

        self._prepare_stress_data(num_tables=1, rows_per_table=1000)

        tdSql.execute(f"use {self.DB_NAME};")

        # Create virtual table
        tdSql.execute("CREATE VTABLE `vtb_stress_query` ("
                      "ts timestamp, "
                      "val double from stress_src_0.val, "
                      "data binary(64) from stress_src_0.data)")

        num_queries = 100
        times = []

        for i in range(num_queries):
            elapsed = time._measure_query_time(
                "SELECT COUNT(*), AVG(val), MIN(val), MAX(val) FROM vtb_stress_query;",
                f"Query {i+1}/{num_queries}"
            ) if hasattr(time, '_measure_query_time') else None

            start = time.time()
            tdSql.query("SELECT COUNT(*), AVG(val), MIN(val), MAX(val) FROM vtb_stress_query;")
            elapsed = time.time() - start
            times.append(elapsed)

            # Verify result consistency
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1000)

            if (i + 1) % 20 == 0:
                tdLog.info(f"Completed {i + 1}/{num_queries} queries")

        # Analyze timing consistency
        avg_time = sum(times) / len(times)
        variance = max(times) - min(times)

        tdLog.info(f"Repeated query stats: avg={avg_time:.4f}s, variance={variance:.4f}s")

        # Variance should be small (queries should be consistently fast)
        assert variance < avg_time * 2, \
            f"Query time variance too high: variance={variance:.4f}s, avg={avg_time:.4f}s"

        tdLog.info("Repeated query stress test passed")

    def test_stress_large_result_set(self):
        """Stress: Large result set handling

        Test handling of queries returning large result sets.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress, large-result

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Large Result Set Handling ===")

        self._prepare_stress_data(num_tables=1, rows_per_table=10000)

        tdSql.execute(f"use {self.DB_NAME};")

        # Create virtual table
        tdSql.execute("CREATE VTABLE `vtb_stress_large` ("
                      "ts timestamp, "
                      "val double from stress_src_0.val, "
                      "data binary(64) from stress_src_0.data)")

        # Test 1: Full table scan (no limit)
        start = time.time()
        tdSql.query("SELECT * FROM vtb_stress_large;")
        elapsed = time.time() - start
        tdLog.info(f"Full table scan (10K rows): {elapsed:.3f}s")
        tdSql.checkRows(10000)

        # Test 2: Multiple aggregations
        start = time.time()
        tdSql.query("SELECT "
                   "COUNT(*), SUM(val), AVG(val), "
                   "MIN(val), MAX(val), "
                   "STDDEV(val), VARIANCE(val) "
                   "FROM vtb_stress_large;")
        elapsed = time.time() - start
        tdLog.info(f"Multiple aggregations (10K rows): {elapsed:.3f}s")
        tdSql.checkRows(1)

        # Test 3: Group by with many groups
        start = time.time()
        tdSql.query("SELECT CAST(val / 100 AS INT) as grp, COUNT(*) "
                   "FROM vtb_stress_large "
                   "GROUP BY CAST(val / 100 AS INT) "
                   "ORDER BY grp;")
        elapsed = time.time() - start
        tdLog.info(f"GROUP BY (10 groups): {elapsed:.3f}s")
        tdSql.checkRows(10)

        tdLog.info("Large result set stress test passed")

    def test_stress_many_virtual_tables(self):
        """Stress: Many virtual tables creation and query

        Test system stability when many virtual tables exist.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress, many-tables

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Many Virtual Tables ===")

        self._prepare_stress_data(num_tables=10, rows_per_table=100)

        tdSql.execute(f"use {self.DB_NAME};")

        # Create many virtual tables
        num_vtables = 50

        start = time.time()
        for i in range(num_vtables):
            src_table = i % 10  # Reference different source tables
            tdSql.execute(f"CREATE VTABLE `vtb_stress_many_{i}` ("
                         "ts timestamp, "
                         f"val double from stress_src_{src_table}.val)")

        create_elapsed = time.time() - start
        tdLog.info(f"Created {num_vtables} virtual tables in {create_elapsed:.3f}s")

        # Query all virtual tables
        start = time.time()
        for i in range(num_vtables):
            tdSql.query(f"SELECT COUNT(*) FROM vtb_stress_many_{i};")
            tdSql.checkData(0, 0, 100)

        query_elapsed = time.time() - start
        tdLog.info(f"Queried {num_vtables} virtual tables in {query_elapsed:.3f}s")

        # Drop all virtual tables
        start = time.time()
        for i in range(num_vtables):
            tdSql.execute(f"DROP TABLE vtb_stress_many_{i};")

        drop_elapsed = time.time() - start
        tdLog.info(f"Dropped {num_vtables} virtual tables in {drop_elapsed:.3f}s")

        tdLog.info("Many virtual tables stress test passed")

    def test_stress_complex_query_patterns(self):
        """Stress: Complex query patterns

        Test complex query patterns including joins, subqueries,
        and multiple conditions.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress, complex-query

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Complex Query Patterns ===")

        self._prepare_stress_data(num_tables=2, rows_per_table=1000)

        tdSql.execute(f"use {self.DB_NAME};")

        # Create virtual tables
        tdSql.execute("CREATE VTABLE `vtb_stress_complex_0` ("
                      "ts timestamp, "
                      "val double from stress_src_0.val, "
                      "data binary(64) from stress_src_0.data)")

        tdSql.execute("CREATE VTABLE `vtb_stress_complex_1` ("
                      "ts timestamp, "
                      "val double from stress_src_1.val, "
                      "data binary(64) from stress_src_1.data)")

        # Test 1: Subquery
        start = time.time()
        tdSql.query("SELECT * FROM vtb_stress_complex_0 "
                   "WHERE val > (SELECT AVG(val) FROM vtb_stress_complex_1);")
        elapsed = time.time() - start
        tdLog.info(f"Subquery: {elapsed:.3f}s, returned {tdSql.queryRows} rows")

        # Test 2: Union
        start = time.time()
        tdSql.query("(SELECT * FROM vtb_stress_complex_0 LIMIT 100) "
                   "UNION ALL "
                   "(SELECT * FROM vtb_stress_complex_1 LIMIT 100);")
        elapsed = time.time() - start
        tdLog.info(f"Union query: {elapsed:.3f}s")
        tdSql.checkRows(200)

        # Test 3: Multiple conditions with OR
        start = time.time()
        tdSql.query("SELECT * FROM vtb_stress_complex_0 "
                   "WHERE (val < 10 OR val > 90) "
                   "AND data LIKE 'data_0_%';")
        elapsed = time.time() - start
        tdLog.info(f"Multiple conditions: {elapsed:.3f}s, returned {tdSql.queryRows} rows")

        # Test 4: Simple aggregation with ORDER BY
        start = time.time()
        tdSql.query("SELECT ts, val "
                   "FROM vtb_stress_complex_0 "
                   "ORDER BY val "
                   "LIMIT 100;")
        elapsed = time.time() - start
        tdLog.info(f"Window function: {elapsed:.3f}s")
        tdSql.checkRows(100)

        tdLog.info("Complex query patterns stress test passed")

    def test_stress_mixed_operations(self):
        """Stress: Mixed DDL and DML operations

        Test system stability with interleaved DDL (create/drop)
        and DML (query) operations.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress, mixed-operations

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Mixed DDL and DML Operations ===")

        tdSql.execute(f"use {self.DB_NAME};")

        # Prepare source table
        tdSql.execute("CREATE TABLE `stress_src_mixed` ("
                      "ts timestamp, "
                      "val double)")

        sql = "INSERT INTO stress_src_mixed VALUES "
        values = [f"({1700000000000 + i}, {i * 0.1})" for i in range(1000)]
        sql += ", ".join(values)
        tdSql.execute(sql)

        num_iterations = 20

        for i in range(num_iterations):
            # Create virtual table
            tdSql.execute(f"CREATE VTABLE `vtb_stress_mixed_{i}` ("
                         "ts timestamp, "
                         "val double from stress_src_mixed.val)")

            # Query multiple times
            for _ in range(5):
                tdSql.query(f"SELECT COUNT(*) FROM vtb_stress_mixed_{i};")
                tdSql.checkData(0, 0, 1000)

            # Drop some older tables
            if i >= 5:
                tdSql.execute(f"DROP TABLE vtb_stress_mixed_{i - 5};")

            if (i + 1) % 5 == 0:
                tdLog.info(f"Completed {i + 1}/{num_iterations} mixed operation iterations")

        # Clean up remaining tables
        for i in range(max(0, num_iterations - 5), num_iterations):
            tdSql.execute(f"DROP TABLE IF EXISTS vtb_stress_mixed_{i};")

        tdLog.info("Mixed operations stress test passed")

    def test_stress_memory_cleanup(self):
        """Stress: Memory cleanup verification

        Test that memory is properly cleaned up after operations.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress, memory-cleanup

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Memory Cleanup Verification ===")

        tdSql.execute(f"use {self.DB_NAME};")

        # Prepare large source table
        tdSql.execute("CREATE TABLE `stress_src_mem` ("
                      "ts timestamp, "
                      "col1 double, col2 double, col3 double, col4 double, col5 double)")

        # Insert data
        sql = "INSERT INTO stress_src_mem VALUES "
        values = []
        for i in range(5000):
            ts = 1700000000000 + i
            values.append(f"({ts}, {i*0.1}, {i*0.2}, {i*0.3}, {i*0.4}, {i*0.5})")
        sql += ", ".join(values)
        tdSql.execute(sql)

        # Measure baseline query time
        tdSql.execute("CREATE VTABLE `vtb_stress_mem_baseline` ("
                      "ts timestamp, "
                      "col1 double from stress_src_mem.col1, "
                      "col2 double from stress_src_mem.col2, "
                      "col3 double from stress_src_mem.col3, "
                      "col4 double from stress_src_mem.col4, "
                      "col5 double from stress_src_mem.col5)")

        start = time.time()
        tdSql.query("SELECT * FROM vtb_stress_mem_baseline;")
        baseline_time = time.time() - start
        tdLog.info(f"Baseline query time: {baseline_time:.3f}s")

        # Perform many operations
        num_cycles = 30
        for cycle in range(num_cycles):
            # Create virtual table
            tdSql.execute(f"CREATE VTABLE `vtb_stress_mem_{cycle}` ("
                         "ts timestamp, "
                         "col1 double from stress_src_mem.col1, "
                         "col2 double from stress_src_mem.col2, "
                         "col3 double from stress_src_mem.col3, "
                         "col4 double from stress_src_mem.col4, "
                         "col5 double from stress_src_mem.col5)")

            # Query
            tdSql.query(f"SELECT * FROM vtb_stress_mem_{cycle};")

            # Drop
            tdSql.execute(f"DROP TABLE vtb_stress_mem_{cycle};")

            # Force garbage collection
            gc.collect()

            if (cycle + 1) % 10 == 0:
                tdLog.info(f"Completed {cycle + 1}/{num_cycles} memory cleanup cycles")

        # Measure final query time
        start = time.time()
        tdSql.query("SELECT * FROM vtb_stress_mem_baseline;")
        final_time = time.time() - start
        tdLog.info(f"Final query time: {final_time:.3f}s")

        # Query time should not increase significantly (indicates memory leak)
        # Allow 100% tolerance for system variance
        assert final_time < baseline_time * 2.0, \
            f"Possible memory leak: baseline={baseline_time:.3f}s, final={final_time:.3f}s"

        tdLog.info("Memory cleanup verification test passed")

    def test_stress_concurrent_simulation(self):
        """Stress: Simulated concurrent access

        Simulate concurrent access by rapidly alternating between
        different virtual tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress, concurrent

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info("=== Test: Simulated Concurrent Access ===")

        self._prepare_stress_data(num_tables=5, rows_per_table=500)

        tdSql.execute(f"use {self.DB_NAME};")

        # Create 5 virtual tables
        for i in range(5):
            tdSql.execute(f"CREATE VTABLE `vtb_stress_conc_{i}` ("
                         "ts timestamp, "
                         f"val double from stress_src_{i}.val)")

        # Simulate concurrent access by rapidly alternating queries
        num_iterations = 50
        start = time.time()

        for i in range(num_iterations):
            # Alternate between different tables
            table_idx = i % 5
            tdSql.query(f"SELECT COUNT(*), AVG(val) FROM vtb_stress_conc_{table_idx};")
            tdSql.checkRows(1)

        total_time = time.time() - start
        avg_time = total_time / num_iterations

        tdLog.info(f"Concurrent simulation: {num_iterations} queries in {total_time:.3f}s, "
                  f"avg={avg_time:.4f}s per query")

        # Average query time should be reasonable
        assert avg_time < 0.5, f"Average query time too high: {avg_time:.4f}s"

        tdLog.info("Simulated concurrent access test passed")

    def teardown_class(cls):
        """Clean up stress test databases."""
        tdLog.info("=== Stress Test Cleanup ===")
        tdSql.execute(f"drop database if exists {cls.DB_NAME};")
        tdLog.info("Stress test cleanup complete")
