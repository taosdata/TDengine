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

class TestInterp2:
    updatecfgDict = {
        "keepColumnName": "1",
        "ttlChangeOnWrite": "1",
        "querySmaOptimize": "1",
        "slowLogScope": "none",
        "queryBufferSize": 10240
    }

    def setup_class(cls):
        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute("create database test keep 36500")
        tdSql.execute("use test")
        tdSql.execute(
            f'''create table if not exists test.td32727
                (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10), c9 tinyint unsigned, c10 smallint unsigned, c11 int unsigned, c12 bigint unsigned)
                '''
        )
        tdSql.execute("create table if not exists test.td32861(ts timestamp, c1 int);")

        tdSql.execute("create stable if not exists test.ts5941(ts timestamp, c1 int, c2 int) tags (t1 varchar(30));")
        tdSql.execute("create table if not exists test.ts5941_child using test.ts5941 tags ('testts5941');")

        tdLog.printNoPrefix("==========step2:insert data")

        tdSql.execute(f"insert into test.td32727 values ('2020-02-01 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar', 5, 5, 5, 5)")
        tdSql.execute(f"insert into test.td32727 values ('2020-02-01 00:00:10', 10, 10, 10, 10, 10.0, 10.0, true, 'varchar', 'nchar', 10, 10, 10, 10)")
        tdSql.execute(f"insert into test.td32727 values ('2020-02-01 00:00:15', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar', 15, 15, 15, 15)")

        tdSql.execute(
            """insert into test.td32861 values
                ('2020-01-01 00:00:00', 0),
                ('2020-01-01 00:00:01', 1),
                ('2020-01-01 00:00:03', 3),
                ('2020-01-01 00:00:06', 6),
                ('2020-01-01 00:00:10', 10),
                ('2020-01-01 00:00:15', 15),
                ('2020-01-01 00:00:21', 21);"""
        )
        tdSql.execute(f"insert into test.ts5941_child values ('2020-02-01 00:00:05', 5, 5)")
        tdSql.execute(f"insert into test.ts5941_child values ('2020-02-01 00:00:10', 10, 10)")
        tdSql.execute(f"insert into test.ts5941_child values ('2020-02-01 00:00:15', 15, 15)")

    def test_normal_query_new(self):
        """Interp fill and psedo column

        1. Used with PARTITION BY
        2. Used with _isfilled and _irowts in both the select list and as ORDER BY columns
        3. Testing more comprehensive fill modes

        Catalog:
            - Query:Interp

        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2024-10-30 Jing Sima Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        testCase = "interp"
        # read sql from .sql file and execute
        tdLog.info("test normal query.")
        self.sqlFile = etool.curFile(__file__, f"in/{testCase}.in")
        self.ansFile = etool.curFile(__file__, f"ans/{testCase}.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_abnormal_query(self):
        """Interp abnormal query

        1. Testing abnormal query
        
        Catalog:
            - Query:Interp

        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2024-10-30 Jing Sima Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        testCase = "interp"
        tdLog.info("test abnormal query.")
        tdSql.error("select interp(c1) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', -1s) every(2s) fill(prev, 99);")
        tdSql.error("select interp(c1), interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99);")
        tdSql.error("select _irowts from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) fill(near, 2);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, c1);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', 1s, '2020-01-01 00:00:30.000') every(2s) fill(near, 99);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', '1s') every(2s) fill(near, 99);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(linear, 99);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(value, 99);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(value_f, 99);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(null, 99);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(null_f, 99);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1n) every(2s) fill(prev, 99);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1y) every(2s) fill(prev, 99);")
        tdSql.error("select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1) every(2s) fill(prev, 99);")
        tdSql.error("create stream s1 trigger force_window_close into test.s1res as select _irowts, interp(c1), interp(c2)from test.td32727 partition by tbname range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(1s) fill(near, 1, 1);")



