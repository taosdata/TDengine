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


class TestVtableStress:

    DB_NAME = "test_vtable_stress"

    def setup_class(cls):
        tdLog.info(f"prepare databases for stress testing.")

        tdSql.execute(f"drop database if exists {cls.DB_NAME};")
        tdSql.execute(f"create database {cls.DB_NAME} vgroups 2;")
        tdSql.execute(f"use {cls.DB_NAME};")

        tdLog.info(f"prepare source table for create/drop cycles.")
        tdSql.execute("CREATE TABLE `stress_src_cycle` ("
                      "ts timestamp, "
                      "val double)")

        sql = "INSERT INTO stress_src_cycle VALUES "
        values = [f"({1700000000000 + i}, {i * 0.1})" for i in range(100)]
        sql += ", ".join(values)
        tdSql.execute(sql)

        tdLog.info(f"prepare source table for repeated queries (1K rows).")
        tdSql.execute("CREATE TABLE `stress_src_query` ("
                      "ts timestamp, "
                      "val double, "
                      "data binary(64))")

        sql = "INSERT INTO stress_src_query VALUES "
        values = [f"({1700000000000 + i}, {i * 0.1}, 'data_{i}')" for i in range(1000)]
        sql += ", ".join(values)
        tdSql.execute(sql)

        tdSql.execute("CREATE VTABLE `vtb_stress_query` ("
                      "ts timestamp, "
                      "val double from stress_src_query.val, "
                      "data binary(64) from stress_src_query.data)")

        tdLog.info(f"prepare source table for large result set (10K rows).")
        tdSql.execute("CREATE TABLE `stress_src_large` ("
                      "ts timestamp, "
                      "val double, "
                      "data binary(64))")

        for batch in range(0, 10000, 1000):
            sql = "INSERT INTO stress_src_large VALUES "
            values = [f"({1700000000000 + i}, {i * 0.1}, 'data_{i}')"
                      for i in range(batch, min(batch + 1000, 10000))]
            sql += ", ".join(values)
            tdSql.execute(sql)

        tdSql.execute("CREATE VTABLE `vtb_stress_large` ("
                      "ts timestamp, "
                      "val double from stress_src_large.val, "
                      "data binary(64) from stress_src_large.data)")

        tdLog.info(f"prepare 10 source tables for many-vtable test (100 rows each).")
        for t in range(10):
            tdSql.execute(f"CREATE TABLE `stress_src_{t}` ("
                          "ts timestamp, "
                          "val double, "
                          "data binary(64))")

            sql = f"INSERT INTO stress_src_{t} VALUES "
            values = [f"({1700000000000 + i}, {i * 0.1}, 'data_{t}_{i}')" for i in range(100)]
            sql += ", ".join(values)
            tdSql.execute(sql)

        tdLog.info(f"prepare source tables for complex queries (2 tables, 1K rows each).")
        for t in range(2):
            tdSql.execute(f"CREATE TABLE `stress_src_complex_{t}` ("
                          "ts timestamp, "
                          "val double, "
                          "data binary(64))")

            sql = f"INSERT INTO stress_src_complex_{t} VALUES "
            values = [f"({1700000000000 + i}, {i * 0.1}, 'data_{t}_{i}')" for i in range(1000)]
            sql += ", ".join(values)
            tdSql.execute(sql)

        tdSql.execute("CREATE VTABLE `vtb_stress_complex_0` ("
                      "ts timestamp, "
                      "val double from stress_src_complex_0.val, "
                      "data binary(64) from stress_src_complex_0.data)")

        tdSql.execute("CREATE VTABLE `vtb_stress_complex_1` ("
                      "ts timestamp, "
                      "val double from stress_src_complex_1.val, "
                      "data binary(64) from stress_src_complex_1.data)")

        tdLog.info(f"prepare source table for mixed DDL/DML (1K rows).")
        tdSql.execute("CREATE TABLE `stress_src_mixed` ("
                      "ts timestamp, "
                      "val double)")

        sql = "INSERT INTO stress_src_mixed VALUES "
        values = [f"({1700000000000 + i}, {i * 0.1})" for i in range(1000)]
        sql += ", ".join(values)
        tdSql.execute(sql)

        tdLog.info(f"prepare source table for memory cleanup (5K rows, 5 cols).")
        tdSql.execute("CREATE TABLE `stress_src_mem` ("
                      "ts timestamp, "
                      "col1 double, col2 double, col3 double, col4 double, col5 double)")

        sql = "INSERT INTO stress_src_mem VALUES "
        values = [f"({1700000000000 + i}, {i*0.1}, {i*0.2}, {i*0.3}, {i*0.4}, {i*0.5})"
                  for i in range(5000)]
        sql += ", ".join(values)
        tdSql.execute(sql)

        tdSql.execute("CREATE VTABLE `vtb_stress_mem_baseline` ("
                      "ts timestamp, "
                      "col1 double from stress_src_mem.col1, "
                      "col2 double from stress_src_mem.col2, "
                      "col3 double from stress_src_mem.col3, "
                      "col4 double from stress_src_mem.col4, "
                      "col5 double from stress_src_mem.col5)")

        tdLog.info(f"prepare source tables for concurrent simulation (5 tables, 500 rows each).")
        for i in range(5):
            tdSql.execute(f"CREATE TABLE `stress_src_conc_{i}` ("
                          "ts timestamp, "
                          "val double)")

            sql = f"INSERT INTO stress_src_conc_{i} VALUES "
            values = [f"({1700000000000 + j}, {j * 0.1})" for j in range(500)]
            sql += ", ".join(values)
            tdSql.execute(sql)

        for i in range(5):
            tdSql.execute(f"CREATE VTABLE `vtb_stress_conc_{i}` ("
                          "ts timestamp, "
                          f"val double from stress_src_conc_{i}.val)")

    def test_stress_repeated_create_drop_vtable(self):
        """Stress: repeated virtual table create/drop cycles

        Repeatedly create and drop a virtual table (50 cycles),
        verifying query correctness at each cycle.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test repeated create/drop virtual table (50 cycles).")
        tdSql.execute(f"use {self.DB_NAME};")

        num_cycles = 50
        for cycle in range(num_cycles):
            tdSql.execute("CREATE VTABLE `vtb_stress_cycle` ("
                          "ts timestamp, "
                          "val double from stress_src_cycle.val)")

            tdSql.query("SELECT COUNT(*), AVG(val) FROM vtb_stress_cycle;")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 100)

            tdSql.execute("DROP VTABLE vtb_stress_cycle;")

            if (cycle + 1) % 10 == 0:
                tdLog.info(f"completed {cycle + 1}/{num_cycles} create/drop cycles.")

        tdLog.info(f"repeated create/drop test passed.")

    def test_stress_repeated_query(self):
        """Stress: repeated query execution on same virtual table

        Execute the same aggregation query 100 times, verifying
        result consistency at every iteration.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test repeated query execution (100 iterations).")
        tdSql.execute(f"use {self.DB_NAME};")

        num_queries = 100
        for i in range(num_queries):
            tdSql.query("SELECT COUNT(*), AVG(val), MIN(val), MAX(val) FROM vtb_stress_query;")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1000)

            if (i + 1) % 20 == 0:
                tdLog.info(f"completed {i + 1}/{num_queries} queries.")

        tdLog.info(f"repeated query test passed.")

    def test_stress_large_result_set(self):
        """Stress: large result set handling

        1. full table scan on 10K rows
        2. multiple aggregations
        3. group by with many groups

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test large result set handling.")
        tdSql.execute(f"use {self.DB_NAME};")

        tdSql.query("SELECT * FROM vtb_stress_large;")
        tdSql.checkRows(10000)

        tdSql.query("SELECT "
                     "COUNT(*), SUM(val), AVG(val), "
                     "MIN(val), MAX(val), "
                     "STDDEV(val) "
                     "FROM vtb_stress_large;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10000)

        tdSql.query("SELECT CAST(val / 100 AS INT) as grp, COUNT(*) "
                     "FROM vtb_stress_large "
                     "GROUP BY CAST(val / 100 AS INT) "
                     "ORDER BY grp;")
        tdSql.checkRows(10)

        tdLog.info(f"large result set test passed.")

    def test_stress_many_virtual_tables(self):
        """Stress: create and query many virtual tables

        Create 50 virtual tables referencing 10 source tables,
        query each one, then drop them all.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test creating and querying 50 virtual tables.")
        tdSql.execute(f"use {self.DB_NAME};")

        num_vtables = 50
        for i in range(num_vtables):
            src_table = i % 10
            tdSql.execute(f"CREATE VTABLE `vtb_stress_many_{i}` ("
                          "ts timestamp, "
                          f"val double from stress_src_{src_table}.val)")

        for i in range(num_vtables):
            tdSql.query(f"SELECT COUNT(*) FROM vtb_stress_many_{i};")
            tdSql.checkData(0, 0, 100)

        for i in range(num_vtables):
            tdSql.execute(f"DROP VTABLE vtb_stress_many_{i};")

        tdLog.info(f"many virtual tables test passed.")

    def test_stress_complex_query_patterns(self):
        """Stress: complex query patterns on virtual tables

        1. subquery
        2. union all
        3. multiple conditions with OR
        4. order by with limit

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test complex query patterns.")
        tdSql.execute(f"use {self.DB_NAME};")

        tdSql.query("SELECT * FROM vtb_stress_complex_0 "
                     "WHERE val > (SELECT AVG(val) FROM vtb_stress_complex_1);")
        tdLog.info(f"subquery returned {tdSql.queryRows} rows.")

        tdSql.query("(SELECT * FROM vtb_stress_complex_0 LIMIT 100) "
                     "UNION ALL "
                     "(SELECT * FROM vtb_stress_complex_1 LIMIT 100);")
        tdSql.checkRows(200)

        tdSql.query("SELECT * FROM vtb_stress_complex_0 "
                     "WHERE (val < 10 OR val > 90) "
                     "AND data LIKE 'data_0_%';")
        tdLog.info(f"multi-condition query returned {tdSql.queryRows} rows.")

        tdSql.query("SELECT ts, val "
                     "FROM vtb_stress_complex_0 "
                     "ORDER BY val "
                     "LIMIT 100;")
        tdSql.checkRows(100)

        tdLog.info(f"complex query patterns test passed.")

    def test_stress_mixed_ddl_dml(self):
        """Stress: mixed DDL and DML operations

        Interleave create/query/drop operations across 20 iterations,
        dropping older tables as new ones are created.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test mixed DDL and DML operations.")
        tdSql.execute(f"use {self.DB_NAME};")

        num_iterations = 20
        for i in range(num_iterations):
            tdSql.execute(f"CREATE VTABLE `vtb_stress_mixed_{i}` ("
                          "ts timestamp, "
                          "val double from stress_src_mixed.val)")

            for _ in range(5):
                tdSql.query(f"SELECT COUNT(*) FROM vtb_stress_mixed_{i};")
                tdSql.checkData(0, 0, 1000)

            if i >= 5:
                tdSql.execute(f"DROP VTABLE vtb_stress_mixed_{i - 5};")

            if (i + 1) % 5 == 0:
                tdLog.info(f"completed {i + 1}/{num_iterations} mixed iterations.")

        for i in range(max(0, num_iterations - 5), num_iterations):
            tdSql.execute(f"DROP VTABLE IF EXISTS vtb_stress_mixed_{i};")

        tdLog.info(f"mixed DDL/DML test passed.")

    def test_stress_memory_cleanup(self):
        """Stress: memory cleanup verification

        Repeatedly create/query/drop virtual tables (30 cycles)
        on a 5K-row source, then verify final query still returns
        correct results.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test memory cleanup verification.")
        tdSql.execute(f"use {self.DB_NAME};")

        tdSql.query("SELECT COUNT(*) FROM vtb_stress_mem_baseline;")
        tdSql.checkData(0, 0, 5000)

        num_cycles = 30
        for cycle in range(num_cycles):
            tdSql.execute(f"CREATE VTABLE `vtb_stress_mem_{cycle}` ("
                          "ts timestamp, "
                          "col1 double from stress_src_mem.col1, "
                          "col2 double from stress_src_mem.col2, "
                          "col3 double from stress_src_mem.col3, "
                          "col4 double from stress_src_mem.col4, "
                          "col5 double from stress_src_mem.col5)")

            tdSql.query(f"SELECT COUNT(*) FROM vtb_stress_mem_{cycle};")
            tdSql.checkData(0, 0, 5000)

            tdSql.execute(f"DROP VTABLE vtb_stress_mem_{cycle};")

            if (cycle + 1) % 10 == 0:
                tdLog.info(f"completed {cycle + 1}/{num_cycles} memory cleanup cycles.")

        tdSql.query("SELECT * FROM vtb_stress_mem_baseline;")
        tdSql.checkRows(5000)

        tdLog.info(f"memory cleanup test passed.")

    def test_stress_concurrent_simulation(self):
        """Stress: simulated concurrent access

        Rapidly alternate queries across 5 virtual tables
        for 50 iterations, verifying correctness each time.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, stress

        Jira: None

        History:
            - 2026-2-25 Created
        """
        tdLog.info(f"test simulated concurrent access.")
        tdSql.execute(f"use {self.DB_NAME};")

        num_iterations = 50
        for i in range(num_iterations):
            table_idx = i % 5
            tdSql.query(f"SELECT COUNT(*), AVG(val) FROM vtb_stress_conc_{table_idx};")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 500)

        tdLog.info(f"concurrent simulation test passed.")
