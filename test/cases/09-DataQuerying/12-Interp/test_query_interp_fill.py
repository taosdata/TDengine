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
from new_test_framework.utils.eutil import findTaosdLog
from new_test_framework.utils.streamUtil import StreamItem, tdStream
from new_test_framework.utils import tdLog, tdSql, etool, tdCom
from new_test_framework.utils.common import TDCom, tdCom
import os
from random import randrange
import subprocess
from tzlocal import get_localzone

import random
import queue
import threading

ROUND: int = 500

class TestInterpFill:

    updatecfgDict = {
        "keepColumnName": "1",
        "ttlChangeOnWrite": "1",
        "querySmaOptimize": "1",
        "slowLogScope": "none",
        "queryBufferSize": 10240,
        'asynclog': 0,
        'ttlUnit': 1,
        'ttlPushInterval': 5,
        'ratioOfVnodeStreamThrea': 4,
        'debugFlag': 143,
        "qDebugFlag": 135
    }
    check_failed: bool = False

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

        tdSql.execute("create table if not exists ntb (ts timestamp, c1 int)")
        tdSql.execute("create table if not exists stb (ts timestamp, c1 int) tags (gid int)")
        tdSql.execute("create table if not exists ctb1 using stb tags (1)")
        tdSql.execute("create table if not exists ctb2 using stb tags (2)")

        tdSql.execute("""
            insert into ntb values
            ("2025-12-12 12:00:00", 1)
            ("2025-12-12 12:03:00", null)
            ("2025-12-12 12:04:00", null)
            ("2025-12-12 12:05:00", null)
            ("2025-12-12 12:08:00", 2)
            ("2025-12-12 12:09:00", null)
            ("2025-12-12 12:10:00", null)
            ("2025-12-12 12:11:00", 3)""")

        tdSql.execute("""
            insert into ctb1 values
            ("2025-12-12 12:00:00", 1)
            ("2025-12-12 12:03:00", null)
            ("2025-12-12 12:04:00", null)
            ("2025-12-12 12:05:00", null)
            ("2025-12-12 12:08:00", 2)
            ("2025-12-12 12:09:00", null)
            ("2025-12-12 12:10:00", null)
            ("2025-12-12 12:11:00", 3)""")

        tdSql.execute("""
            insert into ctb2 values
            ("2025-12-12 12:13:00", null)
            ("2025-12-12 12:14:00", null)
            ("2025-12-12 12:15:00", null)
            ("2025-12-12 12:18:00", 2)
            ("2025-12-12 12:19:00", null)
            ("2025-12-12 12:20:00", 3)""")

        cmd = f"taosBenchmark -f {os.path.dirname(os.path.realpath(__file__))}/in/insert_config.json"
        tdLog.info(f"Running command: {cmd}")
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, err = process.communicate()
        assert process.returncode == 0, f"taosBenchmark failed: {err.decode()}"

        # insert data for surrounding time test
        tdSql.execute("create database test_surround keep 36500")
        tdSql.execute("use test_surround")
        tdSql.execute("create table test_surround.ntb (ts timestamp, c1 int, c2 varchar(10))")
        tdSql.execute("create table test_surround.stb (ts timestamp, c1 int) tags (gid int)")
        tdSql.execute("create table test_surround.ctb1 using test_surround.stb tags (1)")
        tdSql.execute("create table test_surround.ctb2 using test_surround.stb tags (2)")
        tdSql.execute("create table test_surround.ctb3 using test_surround.stb tags (3)")
        tdSql.execute("""
            insert into test_surround.ntb values
            ("2026-01-01 12:00:00", 1, null)
            ("2026-01-02 12:00:00", null, 'a')
            ("2026-01-03 12:00:00", null, 'b')
            ("2026-01-06 12:00:00", 2, null)
            ("2026-01-07 12:00:00", null, 'c')
            ("2026-01-08 12:00:00", null, null)
            ("2026-01-09 12:00:00", 3, null)""")
        tdSql.execute("""
            insert into test_surround.ctb1 values
            ("2026-01-01 12:00:00", 1)
            ("2026-01-02 12:00:00", null)
            ("2026-01-03 12:00:00", null)
            ("2026-01-06 12:00:00", 2)
            ("2026-01-07 12:00:00", null)
            ("2026-01-08 12:00:00", null)
            ("2026-01-09 12:00:00", 3)""")
        tdSql.execute("""
            insert into test_surround.ctb2 values
            ("2026-01-01 12:00:00", null)
            ("2026-01-02 12:00:00", null)
            ("2026-01-03 12:00:00", 1)
            ("2026-01-04 12:00:00", 2)
            ("2026-01-07 12:00:00", 3)
            ("2026-01-08 12:00:00", null)
            ("2026-01-09 12:00:00", null)""")
        tdSql.execute("""
            insert into test_surround.ctb3 values
            ("2026-01-01 12:00:00", null)
            ("2026-01-02 12:00:00", 1)
            ("2026-01-03 12:00:00", null)
            ("2026-01-05 12:00:00", 2)
            ("2026-01-07 12:00:00", null)
            ("2026-01-08 12:00:00", 3)
            ("2026-01-09 12:00:00", null)""")

    def test_normal_query_new(self):
        """Interp fill and psedo column

        1. Used with PARTITION BY
        2. Used with _isfilled and _irowts in both the select list and as ORDER BY columns
        3. Testing more comprehensive fill modes


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

    def test_interp_fill_ignore_null(self):
        """Interp query fill with non-null values

        1. testing fill(prev/next/near/linear) filling nulls
           with non-null values

        Catalog:
            - Query:Interp

        Since: v3.4.1.0

        Labels: common,ci

        History:
            - 2025-12-12 Tony Zhang created

        """
        testCase = "interp_fill_ignore_null"
        # read sql from .sql file and execute
        tdLog.info("test normal query ignoring null.")
        self.sqlFile = etool.curFile(__file__, f"in/{testCase}.in")
        self.ansFile = etool.curFile(__file__, f"ans/{testCase}.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_interp_fill_ignore_null_scan(self):
        """Interp query fill with non-null values in scan

        1. testing interp query filling nulls with non-null values
        2. testing PREV and NEXT scan stop at non-null values

        Catalog:
            - Scan:Interp

        Since: v3.4.1.0

        Labels: common,ci

        History:
            - 2025-12-22 Tony Zhang created

        """
        tdSql.execute("alter dnode 1 'qDebugFlag 141'")
        tdSql.execute("use test")
        tdSql.execute("create table test.ntb1(ts timestamp, c1 int)")
        start = 1_766_000_000_000  # 2025-12-18 03:33:20
        step = 1000                # 1 second
        size = 10000               # 10000 rows
        # insert enough rows into ntb1
        for ts in range(start, start + size * step, 1 * step):
            tdSql.execute(f"insert into ntb1 values ({ts}, {ts % 10000})")

        mid = start + size / 2 * step  # 1_766_005_000_000, 2025-12-18 04:56:40
        end = start + size * step      # 1_766_010_000_000, 2025-12-18 06:20:00
        # this query will generate only 1 NEXT scan datablock
        tdSql.execute(f"select _irowts, interp(c1, 1) from ntb1 range({start} - 500a) fill(linear)")
        # this query will generate 1 PREV scan and 1 NEXT scan datablock
        tdSql.execute(f"select _irowts, interp(c1, 1) from ntb1 range({mid} + 500a) fill(linear)")
        # this query will generate only 1 PREV scan datablock
        tdSql.execute(f"select _irowts, interp(c1, 1) from ntb1 range({end} + 500a) fill(linear)")

        tdSql.execute("alter dnode 1 'qDebugFlag 135'")
        tdSql.query("show local variables like 'queryPolicy'")
        if tdSql.getData(0, 0) == 1:
            # queryPolicy is 1, should have 4 doTimesliceNext logs
            # for other queryPolicy, it is random, so we cannot check the logs
            assert findTaosdLog("DEBUG.*doTimesliceNext") == 1+2+1, \
                "should have 4 doTimesliceNext logs"

        # insert more null rows into ntb1
        start = end  # 1_766_010_000_000, 2025-12-18 06:20:00
        for ts in range(start, start + size * step, 1 * step):
            tdSql.execute(f"insert into ntb1 values ({ts}, NULL)")

        mid = start + size / 2 * step  # 1_766_015_000_000, 2025-12-18 07:43:20
        end = start + size * step      # 1_766_020_000_000, 2025-12-18 09:06:40
        tdSql.execute(f"insert into ntb1 values ({start}, 77777)")
        tdSql.execute(f"insert into ntb1 values ({end}, 88888)")

        tdSql.query(f"""select cast(_irowts as bigint), interp(c1, 1),
                cast(_irowts_origin as bigint), _isfilled
                from ntb1 range({mid}) fill(prev)""")
        tdSql.checkData(0, 0, mid)
        tdSql.checkData(0, 1, 77777)
        tdSql.checkData(0, 2, start)
        tdSql.checkData(0, 3, True)
        tdSql.query(f"""select cast(_irowts as bigint), interp(c1, 1),
                cast(_irowts_origin as bigint), _isfilled
                from ntb1 range({mid}) fill(next)""")
        tdSql.checkData(0, 0, mid)
        tdSql.checkData(0, 1, 88888)
        tdSql.checkData(0, 2, end)
        tdSql.checkData(0, 3, True)
        tdSql.query(f"""select cast(_irowts as bigint), interp(c1, 1),
                cast(_irowts_origin as bigint), _isfilled
                from ntb1 range({mid}) fill(near)""")
        tdSql.checkData(0, 0, mid)
        tdSql.checkData(0, 1, 77777)
        tdSql.checkData(0, 2, start)
        tdSql.checkData(0, 3, True)
        tdSql.query(f"""select cast(_irowts as bigint), interp(c1, 1)
                from ntb1 range({mid}) fill(linear)""")
        tdSql.checkData(0, 0, mid)
        tdSql.checkData(0, 1, 83332)

    def test_interp_fill_ignore_null_stream_basic(self):
        """Interp query fill with non-null values in stream

        - testing fill(prev/next/near/linear) filling nulls
          with non-null values in stream
        - using simple data in ntb table to test computing results. ntb
          just has 8 rows of data

        Catalog:
            - Stream:Interp

        Since: v3.4.1.0

        Labels: common,ci

        History:
            - 2025-12-22 Tony Zhang created

        """
        tdStream.createSnode()
        tdSql.execute("use test")
        tdSql.execute("create table trigger_table(ts timestamp, c1 int)")

        streams: list[StreamItem] = []

        stream: StreamItem = StreamItem(
            id=0,
            stream="""create stream s0 state_window(c1, 1) from trigger_table
                into r0 as select _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from ntb range(_twstart+1s) fill(prev)""",
            check_func=self.check_s0
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=1,
            stream="""create stream s1 state_window(c1, 1) from trigger_table
                into r1 as select _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from ntb range(_twstart+1s) fill(next)""",
            check_func=self.check_s1
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=2,
            stream="""create stream s2 state_window(c1, 1) from trigger_table
                into r2 as select _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from ntb range(_twstart+1s) fill(near)""",
            check_func=self.check_s2
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=3,
            stream="""create stream s3 state_window(c1, 1) from trigger_table
                into r3 as select _irowts,
                interp(c1, 1) from ntb range(_twstart+1s) fill(linear)""",
            check_func=self.check_s3
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=4,
            stream="""create stream s4 state_window(c1, 1) from trigger_table
                into r4 as select _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from ntb range(_twstart, _twend) every(30s)
                fill(prev)""",
            check_func=self.check_s4
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=5,
            stream="""create stream s5 state_window(c1, 1) from trigger_table
                into r5 as select _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from ntb range(_twstart, _twend) every(30s)
                fill(next)""",
            check_func=self.check_s5
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=6,
            stream="""create stream s6 state_window(c1, 1) from trigger_table
                into r6 as select _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from ntb range(_twstart, _twend) every(30s)
                fill(near)""",
            check_func=self.check_s6
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=7,
            stream="""create stream s7 state_window(c1, 1) from trigger_table
                into r7 as select _irowts,
                interp(c1, 1) from ntb range(_twstart, _twend) every(30s)
                fill(linear)""",
            check_func=self.check_s7
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        # insert data into trigger_table
        tdSql.execute("""
            insert into trigger_table values
            ('2025-12-12 11:59:00', 1),
            ('2025-12-12 12:01:00', 1),
            ('2025-12-12 12:02:00', 2),
            ('2025-12-12 12:03:00', 1),
            ('2025-12-12 12:05:00', 1),
            ('2025-12-12 12:06:00', 2),
            ('2025-12-12 12:09:30', 2),
            ('2025-12-12 12:10:00', 1),
            ('2025-12-12 12:12:00', 1),
            ('2025-12-12 12:12:01', 2)
        """)

        # check results
        for s in streams:
            s.checkResults()

    def check_s0(self):
        tdSql.checkResultsByFunc(
            sql="select * from r0",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-12-12 12:02:01.000")
            and tdSql.compareData(0, 1, True)
            and tdSql.compareData(0, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(1, 0, "2025-12-12 12:03:01.000")
            and tdSql.compareData(1, 1, True)
            and tdSql.compareData(1, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(1, 3, 1)
            and tdSql.compareData(2, 0, "2025-12-12 12:06:01.000")
            and tdSql.compareData(2, 1, True)
            and tdSql.compareData(2, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(2, 3, 1)
            and tdSql.compareData(3, 0, "2025-12-12 12:10:01.000")
            and tdSql.compareData(3, 1, True)
            and tdSql.compareData(3, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(3, 3, 2)
        )

    def check_s1(self):
        tdSql.checkResultsByFunc(
            sql="select * from r1",
            func=lambda: tdSql.getRows() == 5
            and tdSql.compareData(0, 0, "2025-12-12 11:59:01.000")
            and tdSql.compareData(0, 1, True)
            and tdSql.compareData(0, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(1, 0, "2025-12-12 12:02:01.000")
            and tdSql.compareData(1, 1, True)
            and tdSql.compareData(1, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(1, 3, 2)
            and tdSql.compareData(2, 0, "2025-12-12 12:03:01.000")
            and tdSql.compareData(2, 1, True)
            and tdSql.compareData(2, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(2, 3, 2)
            and tdSql.compareData(3, 0, "2025-12-12 12:06:01.000")
            and tdSql.compareData(3, 1, True)
            and tdSql.compareData(3, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(3, 3, 2)
            and tdSql.compareData(4, 0, "2025-12-12 12:10:01.000")
            and tdSql.compareData(4, 1, True)
            and tdSql.compareData(4, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(4, 3, 3)
        )

    def check_s2(self):
        tdSql.checkResultsByFunc(
            sql="select * from r2",
            func=lambda: tdSql.getRows() == 5
            and tdSql.compareData(0, 0, "2025-12-12 11:59:01.000")
            and tdSql.compareData(0, 1, True)
            and tdSql.compareData(0, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(1, 0, "2025-12-12 12:02:01.000")
            and tdSql.compareData(1, 1, True)
            and tdSql.compareData(1, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(1, 3, 1)
            and tdSql.compareData(2, 0, "2025-12-12 12:03:01.000")
            and tdSql.compareData(2, 1, True)
            and tdSql.compareData(2, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(2, 3, 1)
            and tdSql.compareData(3, 0, "2025-12-12 12:06:01.000")
            and tdSql.compareData(3, 1, True)
            and tdSql.compareData(3, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(3, 3, 2)
            and tdSql.compareData(4, 0, "2025-12-12 12:10:01.000")
            and tdSql.compareData(4, 1, True)
            and tdSql.compareData(4, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(4, 3, 3)
        )

    def check_s3(self):
        tdSql.checkResultsByFunc(
            sql="select * from r3",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-12-12 12:02:01.000")
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(1, 0, "2025-12-12 12:03:01.000")
            and tdSql.compareData(1, 1, 1)
            and tdSql.compareData(2, 0, "2025-12-12 12:06:01.000")
            and tdSql.compareData(2, 1, 1)
            and tdSql.compareData(3, 0, "2025-12-12 12:10:01.000")
            and tdSql.compareData(3, 1, 2)
        )

    def check_s4(self):
        tdSql.checkResultsByFunc(
            sql="select * from r4",
            func=lambda: tdSql.getRows() == 25
            and tdSql.compareData(0, 0, "2025-12-12 12:00:00.000")
            and tdSql.compareData(0, 1, False)
            and tdSql.compareData(0, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(1, 0, "2025-12-12 12:00:30.000")
            and tdSql.compareData(1, 1, True)
            and tdSql.compareData(1, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(1, 3, 1)
            and tdSql.compareData(2, 0, "2025-12-12 12:01:00.000")
            and tdSql.compareData(2, 1, True)
            and tdSql.compareData(2, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(2, 3, 1)
            and tdSql.compareData(3, 0, "2025-12-12 12:01:30.000")
            and tdSql.compareData(3, 1, True)
            and tdSql.compareData(3, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(3, 3, 1)
            and tdSql.compareData(4, 0, "2025-12-12 12:02:00.000")
            and tdSql.compareData(4, 1, True)
            and tdSql.compareData(4, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(4, 3, 1)
            and tdSql.compareData(5, 0, "2025-12-12 12:02:30.000")
            and tdSql.compareData(5, 1, True)
            and tdSql.compareData(5, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(5, 3, 1)
            and tdSql.compareData(6, 0, "2025-12-12 12:03:00.000")
            and tdSql.compareData(6, 1, True)
            and tdSql.compareData(6, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(6, 3, 1)
            and tdSql.compareData(7, 0, "2025-12-12 12:03:30.000")
            and tdSql.compareData(7, 1, True)
            and tdSql.compareData(7, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(7, 3, 1)
            and tdSql.compareData(8, 0, "2025-12-12 12:04:00.000")
            and tdSql.compareData(8, 1, True)
            and tdSql.compareData(8, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(8, 3, 1)
            and tdSql.compareData(9, 0, "2025-12-12 12:04:30.000")
            and tdSql.compareData(9, 1, True)
            and tdSql.compareData(9, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(9, 3, 1)
            and tdSql.compareData(10, 0, "2025-12-12 12:05:00.000")
            and tdSql.compareData(10, 1, True)
            and tdSql.compareData(10, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(10, 3, 1)
            and tdSql.compareData(11, 0, "2025-12-12 12:05:30.000")
            and tdSql.compareData(11, 1, True)
            and tdSql.compareData(11, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(11, 3, 1)
            and tdSql.compareData(12, 0, "2025-12-12 12:06:00.000")
            and tdSql.compareData(12, 1, True)
            and tdSql.compareData(12, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(12, 3, 1)
            and tdSql.compareData(13, 0, "2025-12-12 12:06:30.000")
            and tdSql.compareData(13, 1, True)
            and tdSql.compareData(13, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(13, 3, 1)
            and tdSql.compareData(14, 0, "2025-12-12 12:07:00.000")
            and tdSql.compareData(14, 1, True)
            and tdSql.compareData(14, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(14, 3, 1)
            and tdSql.compareData(15, 0, "2025-12-12 12:07:30.000")
            and tdSql.compareData(15, 1, True)
            and tdSql.compareData(15, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(15, 3, 1)
            and tdSql.compareData(16, 0, "2025-12-12 12:08:00.000")
            and tdSql.compareData(16, 1, False)
            and tdSql.compareData(16, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(16, 3, 2)
            and tdSql.compareData(17, 0, "2025-12-12 12:08:30.000")
            and tdSql.compareData(17, 1, True)
            and tdSql.compareData(17, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(17, 3, 2)
            and tdSql.compareData(18, 0, "2025-12-12 12:09:00.000")
            and tdSql.compareData(18, 1, True)
            and tdSql.compareData(18, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(18, 3, 2)
            and tdSql.compareData(19, 0, "2025-12-12 12:09:30.000")
            and tdSql.compareData(19, 1, True)
            and tdSql.compareData(19, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(19, 3, 2)
            and tdSql.compareData(20, 0, "2025-12-12 12:10:00.000")
            and tdSql.compareData(20, 1, True)
            and tdSql.compareData(20, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(20, 3, 2)
            and tdSql.compareData(21, 0, "2025-12-12 12:10:30.000")
            and tdSql.compareData(21, 1, True)
            and tdSql.compareData(21, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(21, 3, 2)
            and tdSql.compareData(22, 0, "2025-12-12 12:11:00.000")
            and tdSql.compareData(22, 1, False)
            and tdSql.compareData(22, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(22, 3, 3)
            and tdSql.compareData(23, 0, "2025-12-12 12:11:30.000")
            and tdSql.compareData(23, 1, True)
            and tdSql.compareData(23, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(23, 3, 3)
            and tdSql.compareData(24, 0, "2025-12-12 12:12:00.000")
            and tdSql.compareData(24, 1, True)
            and tdSql.compareData(24, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(24, 3, 3)
        )

    def check_s5(self):
        tdSql.checkResultsByFunc(
            sql="select * from r5",
            func=lambda: tdSql.getRows() == 25
            and tdSql.compareData(0, 0, "2025-12-12 11:59:00.000")
            and tdSql.compareData(0, 1, True)
            and tdSql.compareData(0, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(1, 0, "2025-12-12 11:59:30.000")
            and tdSql.compareData(1, 1, True)
            and tdSql.compareData(1, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(1, 3, 1)
            and tdSql.compareData(2, 0, "2025-12-12 12:00:00.000")
            and tdSql.compareData(2, 1, False)
            and tdSql.compareData(2, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(2, 3, 1)
            and tdSql.compareData(3, 0, "2025-12-12 12:00:30.000")
            and tdSql.compareData(3, 1, True)
            and tdSql.compareData(3, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(3, 3, 2)
            and tdSql.compareData(4, 0, "2025-12-12 12:01:00.000")
            and tdSql.compareData(4, 1, True)
            and tdSql.compareData(4, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(4, 3, 2)
            and tdSql.compareData(5, 0, "2025-12-12 12:01:30.000")
            and tdSql.compareData(5, 1, True)
            and tdSql.compareData(5, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(5, 3, 2)
            and tdSql.compareData(6, 0, "2025-12-12 12:02:00.000")
            and tdSql.compareData(6, 1, True)
            and tdSql.compareData(6, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(6, 3, 2)
            and tdSql.compareData(7, 0, "2025-12-12 12:02:30.000")
            and tdSql.compareData(7, 1, True)
            and tdSql.compareData(7, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(7, 3, 2)
            and tdSql.compareData(8, 0, "2025-12-12 12:03:00.000")
            and tdSql.compareData(8, 1, True)
            and tdSql.compareData(8, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(8, 3, 2)
            and tdSql.compareData(9, 0, "2025-12-12 12:03:30.000")
            and tdSql.compareData(9, 1, True)
            and tdSql.compareData(9, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(9, 3, 2)
            and tdSql.compareData(10, 0, "2025-12-12 12:04:00.000")
            and tdSql.compareData(10, 1, True)
            and tdSql.compareData(10, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(10, 3, 2)
            and tdSql.compareData(11, 0, "2025-12-12 12:04:30.000")
            and tdSql.compareData(11, 1, True)
            and tdSql.compareData(11, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(11, 3, 2)
            and tdSql.compareData(12, 0, "2025-12-12 12:05:00.000")
            and tdSql.compareData(12, 1, True)
            and tdSql.compareData(12, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(12, 3, 2)
            and tdSql.compareData(13, 0, "2025-12-12 12:05:30.000")
            and tdSql.compareData(13, 1, True)
            and tdSql.compareData(13, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(13, 3, 2)
            and tdSql.compareData(14, 0, "2025-12-12 12:06:00.000")
            and tdSql.compareData(14, 1, True)
            and tdSql.compareData(14, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(14, 3, 2)
            and tdSql.compareData(15, 0, "2025-12-12 12:06:30.000")
            and tdSql.compareData(15, 1, True)
            and tdSql.compareData(15, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(15, 3, 2)
            and tdSql.compareData(16, 0, "2025-12-12 12:07:00.000")
            and tdSql.compareData(16, 1, True)
            and tdSql.compareData(16, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(16, 3, 2)
            and tdSql.compareData(17, 0, "2025-12-12 12:07:30.000")
            and tdSql.compareData(17, 1, True)
            and tdSql.compareData(17, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(17, 3, 2)
            and tdSql.compareData(18, 0, "2025-12-12 12:08:00.000")
            and tdSql.compareData(18, 1, False)
            and tdSql.compareData(18, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(18, 3, 2)
            and tdSql.compareData(19, 0, "2025-12-12 12:08:30.000")
            and tdSql.compareData(19, 1, True)
            and tdSql.compareData(19, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(19, 3, 3)
            and tdSql.compareData(20, 0, "2025-12-12 12:09:00.000")
            and tdSql.compareData(20, 1, True)
            and tdSql.compareData(20, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(20, 3, 3)
            and tdSql.compareData(21, 0, "2025-12-12 12:09:30.000")
            and tdSql.compareData(21, 1, True)
            and tdSql.compareData(21, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(21, 3, 3)
            and tdSql.compareData(22, 0, "2025-12-12 12:10:00.000")
            and tdSql.compareData(22, 1, True)
            and tdSql.compareData(22, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(22, 3, 3)
            and tdSql.compareData(23, 0, "2025-12-12 12:10:30.000")
            and tdSql.compareData(23, 1, True)
            and tdSql.compareData(23, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(23, 3, 3)
            and tdSql.compareData(24, 0, "2025-12-12 12:11:00.000")
            and tdSql.compareData(24, 1, False)
            and tdSql.compareData(24, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(24, 3, 3)
        )

    def check_s6(self):
        tdSql.checkResultsByFunc(
            sql="select * from r6",
            func=lambda: tdSql.getRows() == 27
            and tdSql.compareData(0, 0, "2025-12-12 11:59:00.000")
            and tdSql.compareData(0, 1, True)
            and tdSql.compareData(0, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(1, 0, "2025-12-12 11:59:30.000")
            and tdSql.compareData(1, 1, True)
            and tdSql.compareData(1, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(1, 3, 1)
            and tdSql.compareData(2, 0, "2025-12-12 12:00:00.000")
            and tdSql.compareData(2, 1, False)
            and tdSql.compareData(2, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(2, 3, 1)
            and tdSql.compareData(3, 0, "2025-12-12 12:00:30.000")
            and tdSql.compareData(3, 1, True)
            and tdSql.compareData(3, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(3, 3, 1)
            and tdSql.compareData(4, 0, "2025-12-12 12:01:00.000")
            and tdSql.compareData(4, 1, True)
            and tdSql.compareData(4, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(4, 3, 1)
            and tdSql.compareData(5, 0, "2025-12-12 12:01:30.000")
            and tdSql.compareData(5, 1, True)
            and tdSql.compareData(5, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(5, 3, 1)
            and tdSql.compareData(6, 0, "2025-12-12 12:02:00.000")
            and tdSql.compareData(6, 1, True)
            and tdSql.compareData(6, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(6, 3, 1)
            and tdSql.compareData(7, 0, "2025-12-12 12:02:30.000")
            and tdSql.compareData(7, 1, True)
            and tdSql.compareData(7, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(7, 3, 1)
            and tdSql.compareData(8, 0, "2025-12-12 12:03:00.000")
            and tdSql.compareData(8, 1, True)
            and tdSql.compareData(8, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(8, 3, 1)
            and tdSql.compareData(9, 0, "2025-12-12 12:03:30.000")
            and tdSql.compareData(9, 1, True)
            and tdSql.compareData(9, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(9, 3, 1)
            and tdSql.compareData(10, 0, "2025-12-12 12:04:00.000")
            and tdSql.compareData(10, 1, True)
            and tdSql.compareData(10, 2, "2025-12-12 12:00:00.000")
            and tdSql.compareData(10, 3, 1)
            and tdSql.compareData(11, 0, "2025-12-12 12:04:30.000")
            and tdSql.compareData(11, 1, True)
            and tdSql.compareData(11, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(11, 3, 2)
            and tdSql.compareData(12, 0, "2025-12-12 12:05:00.000")
            and tdSql.compareData(12, 1, True)
            and tdSql.compareData(12, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(12, 3, 2)
            and tdSql.compareData(13, 0, "2025-12-12 12:05:30.000")
            and tdSql.compareData(13, 1, True)
            and tdSql.compareData(13, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(13, 3, 2)
            and tdSql.compareData(14, 0, "2025-12-12 12:06:00.000")
            and tdSql.compareData(14, 1, True)
            and tdSql.compareData(14, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(14, 3, 2)
            and tdSql.compareData(15, 0, "2025-12-12 12:06:30.000")
            and tdSql.compareData(15, 1, True)
            and tdSql.compareData(15, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(15, 3, 2)
            and tdSql.compareData(16, 0, "2025-12-12 12:07:00.000")
            and tdSql.compareData(16, 1, True)
            and tdSql.compareData(16, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(16, 3, 2)
            and tdSql.compareData(17, 0, "2025-12-12 12:07:30.000")
            and tdSql.compareData(17, 1, True)
            and tdSql.compareData(17, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(17, 3, 2)
            and tdSql.compareData(18, 0, "2025-12-12 12:08:00.000")
            and tdSql.compareData(18, 1, False)
            and tdSql.compareData(18, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(18, 3, 2)
            and tdSql.compareData(19, 0, "2025-12-12 12:08:30.000")
            and tdSql.compareData(19, 1, True)
            and tdSql.compareData(19, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(19, 3, 2)
            and tdSql.compareData(20, 0, "2025-12-12 12:09:00.000")
            and tdSql.compareData(20, 1, True)
            and tdSql.compareData(20, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(20, 3, 2)
            and tdSql.compareData(21, 0, "2025-12-12 12:09:30.000")
            and tdSql.compareData(21, 1, True)
            and tdSql.compareData(21, 2, "2025-12-12 12:08:00.000")
            and tdSql.compareData(21, 3, 2)
            and tdSql.compareData(22, 0, "2025-12-12 12:10:00.000")
            and tdSql.compareData(22, 1, True)
            and tdSql.compareData(22, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(22, 3, 3)
            and tdSql.compareData(23, 0, "2025-12-12 12:10:30.000")
            and tdSql.compareData(23, 1, True)
            and tdSql.compareData(23, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(23, 3, 3)
            and tdSql.compareData(24, 0, "2025-12-12 12:11:00.000")
            and tdSql.compareData(24, 1, False)
            and tdSql.compareData(24, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(24, 3, 3)
            and tdSql.compareData(25, 0, "2025-12-12 12:11:30.000")
            and tdSql.compareData(25, 1, True)
            and tdSql.compareData(25, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(25, 3, 3)
            and tdSql.compareData(26, 0, "2025-12-12 12:12:00.000")
            and tdSql.compareData(26, 1, True)
            and tdSql.compareData(26, 2, "2025-12-12 12:11:00.000")
            and tdSql.compareData(26, 3, 3)
        )

    def check_s7(self):
        tdSql.checkResultsByFunc(
            sql="select * from r7",
            func=lambda: tdSql.getRows() == 23
            and tdSql.compareData(0, 0, "2025-12-12 12:00:00.000")
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(1, 0, "2025-12-12 12:00:30.000")
            and tdSql.compareData(1, 1, 1)
            and tdSql.compareData(2, 0, "2025-12-12 12:01:00.000")
            and tdSql.compareData(2, 1, 1)
            and tdSql.compareData(3, 0, "2025-12-12 12:01:30.000")
            and tdSql.compareData(3, 1, 1)
            and tdSql.compareData(4, 0, "2025-12-12 12:02:00.000")
            and tdSql.compareData(4, 1, 1)
            and tdSql.compareData(5, 0, "2025-12-12 12:02:30.000")
            and tdSql.compareData(5, 1, 1)
            and tdSql.compareData(6, 0, "2025-12-12 12:03:00.000")
            and tdSql.compareData(6, 1, 1)
            and tdSql.compareData(7, 0, "2025-12-12 12:03:30.000")
            and tdSql.compareData(7, 1, 1)
            and tdSql.compareData(8, 0, "2025-12-12 12:04:00.000")
            and tdSql.compareData(8, 1, 1)
            and tdSql.compareData(9, 0, "2025-12-12 12:04:30.000")
            and tdSql.compareData(9, 1, 1)
            and tdSql.compareData(10, 0, "2025-12-12 12:05:00.000")
            and tdSql.compareData(10, 1, 1)
            and tdSql.compareData(11, 0, "2025-12-12 12:05:30.000")
            and tdSql.compareData(11, 1, 1)
            and tdSql.compareData(12, 0, "2025-12-12 12:06:00.000")
            and tdSql.compareData(12, 1, 1)
            and tdSql.compareData(13, 0, "2025-12-12 12:06:30.000")
            and tdSql.compareData(13, 1, 1)
            and tdSql.compareData(14, 0, "2025-12-12 12:07:00.000")
            and tdSql.compareData(14, 1, 1)
            and tdSql.compareData(15, 0, "2025-12-12 12:07:30.000")
            and tdSql.compareData(15, 1, 1)
            and tdSql.compareData(16, 0, "2025-12-12 12:08:00.000")
            and tdSql.compareData(16, 1, 2)
            and tdSql.compareData(17, 0, "2025-12-12 12:08:30.000")
            and tdSql.compareData(17, 1, 2)
            and tdSql.compareData(18, 0, "2025-12-12 12:09:00.000")
            and tdSql.compareData(18, 1, 2)
            and tdSql.compareData(19, 0, "2025-12-12 12:09:30.000")
            and tdSql.compareData(19, 1, 2)
            and tdSql.compareData(20, 0, "2025-12-12 12:10:00.000")
            and tdSql.compareData(20, 1, 2)
            and tdSql.compareData(21, 0, "2025-12-12 12:10:30.000")
            and tdSql.compareData(21, 1, 2)
            and tdSql.compareData(22, 0, "2025-12-12 12:11:00.000")
            and tdSql.compareData(22, 1, 3)
        )

    def test_interp_fill_ignore_null_stream_advanced(self):
        """Interp query fill with non-null values in stream

        - testing fill(prev/next/near/linear) filling nulls
          with non-null values in stream
        - using advanced data in table ntb1 to test computing results. ntb1
          has 20000 rows of data and last 10000 rows are null, thus notify
          message is needed to notify the scan operator in stream runner

        Catalog:
            - Stream:Interp

        Since: v3.4.1.0

        Labels: common,ci

        History:
            - 2026-01-08 Tony Zhang created

        """
        tdSql.execute("use test")
        tdSql.execute("create table trigger_table1(ts timestamp, c1 int)")

        streams: list[StreamItem] = []

        stream: StreamItem = StreamItem(
            id=8,
            stream="""create stream s8 state_window(c1, 1) from trigger_table1
                into r8 as select _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from ntb1 range(_twstart+1s) fill(prev)""",
            check_func=self.check_s8
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=9,
            stream="""create stream s9 state_window(c1, 1) from trigger_table1
                into r9 as select _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from ntb1 range(_twstart+1s) fill(next)""",
            check_func=self.check_s9
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=10,
            stream="""create stream s10 state_window(c1, 0) from trigger_table1
                into r10 as select _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from ntb1 range(_twstart, _twend) every(30s)
                fill(prev)""",
            check_func=self.check_s10
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=11,
            stream="""create stream s11 state_window(c1, 0) from trigger_table1
                into r11 as select _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from ntb1 range(_twstart, _twend) every(30s)
                fill(next)""",
            check_func=self.check_s11
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=12,
            stream="""create stream s12 state_window(c1, 0) from trigger_table1
                into r12 as select _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from %%trows range(_twstart, _twend) every(30s)
                fill(next)""",
            check_func=self.check_s12
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=13,
            stream="""create stream s13 state_window(c1, 0) from trigger_table1
                into r13 as select _twstart, _irowts, _isfilled, _irowts_origin,
                interp(c1, 1) from stb range("2025-12-12 12:12:00")
                fill(near)""",
            check_func=self.check_s13
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        start_ts =          1766000000000  # 2025-12-18 03:33:20
        end_ts_valid_data = 1766010000000  # 2025-12-18 06:20:00
        end_ts =            1766020000000  # 2025-12-18 09:06:40
        step = 1000
        # insert data into trigger_table
        tdSql.execute(f"""
            insert into trigger_table1 values
            ({start_ts-1*step},          1),
            ({start_ts},                 1),

            ({start_ts+1*step},          3),
            ({start_ts+2*step},          3),

            ({end_ts_valid_data-2*step}, 2),
            ({end_ts_valid_data-1*step}, 2),

            ({end_ts_valid_data},        1),
            ({end_ts_valid_data+1*step}, 1),

            ({end_ts_valid_data+3*step}, 2),
            ({end_ts_valid_data+5*step}, 2),

            ({end_ts - 3*step},          1),
            ({end_ts - 2*step},          1),
            ({end_ts},                   1),

            ({end_ts+1*step},            2),
            ({end_ts+2*step},            2),

            ({end_ts+3*step},            1),
        """)

        # check results
        for s in streams:
            s.checkResults()

    def check_s8(self):
        tdSql.checkResultsByFunc(
            sql="select * from r8",
            func=lambda: tdSql.getRows() == 7
            # Row 0: 2025-12-18 03:33:20.000
            and tdSql.compareData(0, 0, '2025-12-18 03:33:20.000')
            and tdSql.compareData(0, 1, False)
            and tdSql.compareData(0, 2, '2025-12-18 03:33:20.000')
            and tdSql.compareData(0, 3, 0)
            # Row 1: 2025-12-18 03:33:22.000
            and tdSql.compareData(1, 0, '2025-12-18 03:33:22.000')
            and tdSql.compareData(1, 1, False)
            and tdSql.compareData(1, 2, '2025-12-18 03:33:22.000')
            and tdSql.compareData(1, 3, 2000)
            # Row 2: 2025-12-18 06:19:59.000
            and tdSql.compareData(2, 0, '2025-12-18 06:19:59.000')
            and tdSql.compareData(2, 1, False)
            and tdSql.compareData(2, 2, '2025-12-18 06:19:59.000')
            and tdSql.compareData(2, 3, 9000)
            # Row 3: 2025-12-18 06:20:01.000
            and tdSql.compareData(3, 0, '2025-12-18 06:20:01.000')
            and tdSql.compareData(3, 1, True)
            and tdSql.compareData(3, 2, '2025-12-18 06:20:00.000')
            and tdSql.compareData(3, 3, 77777)
            # Row 4: 2025-12-18 06:20:04.000
            and tdSql.compareData(4, 0, '2025-12-18 06:20:04.000')
            and tdSql.compareData(4, 1, True)
            and tdSql.compareData(4, 2, '2025-12-18 06:20:00.000')
            and tdSql.compareData(4, 3, 77777)
            # Row 5: 2025-12-18 09:06:38.000
            and tdSql.compareData(5, 0, '2025-12-18 09:06:38.000')
            and tdSql.compareData(5, 1, True)
            and tdSql.compareData(5, 2, '2025-12-18 06:20:00.000')
            and tdSql.compareData(5, 3, 77777)
            # Row 6: 2025-12-18 09:06:42.000
            and tdSql.compareData(6, 0, '2025-12-18 09:06:42.000')
            and tdSql.compareData(6, 1, True)
            and tdSql.compareData(6, 2, '2025-12-18 09:06:40.000')
            and tdSql.compareData(6, 3, 88888)
        )

    def check_s9(self):
        tdSql.checkResultsByFunc(
            sql="select * from r9",
            func=lambda: tdSql.getRows() == 6
            # Row 0
            and tdSql.compareData(0, 0, '2025-12-18 03:33:20.000')
            and tdSql.compareData(0, 1, False)
            and tdSql.compareData(0, 2, '2025-12-18 03:33:20.000')
            and tdSql.compareData(0, 3, 0)
            # Row 1
            and tdSql.compareData(1, 0, '2025-12-18 03:33:22.000')
            and tdSql.compareData(1, 1, False)
            and tdSql.compareData(1, 2, '2025-12-18 03:33:22.000')
            and tdSql.compareData(1, 3, 2000)
            # Row 2
            and tdSql.compareData(2, 0, '2025-12-18 06:19:59.000')
            and tdSql.compareData(2, 1, False)
            and tdSql.compareData(2, 2, '2025-12-18 06:19:59.000')
            and tdSql.compareData(2, 3, 9000)
            # Row 3
            and tdSql.compareData(3, 0, '2025-12-18 06:20:01.000')
            and tdSql.compareData(3, 1, True)
            and tdSql.compareData(3, 2, '2025-12-18 09:06:40.000')
            and tdSql.compareData(3, 3, 88888)
            # Row 4
            and tdSql.compareData(4, 0, '2025-12-18 06:20:04.000')
            and tdSql.compareData(4, 1, True)
            and tdSql.compareData(4, 2, '2025-12-18 09:06:40.000')
            and tdSql.compareData(4, 3, 88888)
            # Row 5
            and tdSql.compareData(5, 0, '2025-12-18 09:06:38.000')
            and tdSql.compareData(5, 1, True)
            and tdSql.compareData(5, 2, '2025-12-18 09:06:40.000')
            and tdSql.compareData(5, 3, 88888)
        )

    def check_s10(self):
        tdSql.checkResultsByFunc(
            sql="select * from r10",
            func=lambda: tdSql.getRows() == 6
            # Row 0: 2025-12-18 03:33:21.000
            and tdSql.compareData(0, 0, '2025-12-18 03:33:21.000')
            and tdSql.compareData(0, 1, False)
            and tdSql.compareData(0, 2, '2025-12-18 03:33:21.000')
            and tdSql.compareData(0, 3, 1000)
            # Row 1: 2025-12-18 06:19:58.000
            and tdSql.compareData(1, 0, '2025-12-18 06:19:58.000')
            and tdSql.compareData(1, 1, False)
            and tdSql.compareData(1, 2, '2025-12-18 06:19:58.000')
            and tdSql.compareData(1, 3, 8000)
            # Row 2: 2025-12-18 06:20:00.000
            and tdSql.compareData(2, 0, '2025-12-18 06:20:00.000')
            and tdSql.compareData(2, 1, False)
            and tdSql.compareData(2, 2, '2025-12-18 06:20:00.000')
            and tdSql.compareData(2, 3, 77777)
            # Row 3: 2025-12-18 06:20:03.000
            and tdSql.compareData(3, 0, '2025-12-18 06:20:03.000')
            and tdSql.compareData(3, 1, True)
            and tdSql.compareData(3, 2, '2025-12-18 06:20:00.000')
            and tdSql.compareData(3, 3, 77777)
            # Row 4: 2025-12-18 09:06:37.000
            and tdSql.compareData(4, 0, '2025-12-18 09:06:37.000')
            and tdSql.compareData(4, 1, True)
            and tdSql.compareData(4, 2, '2025-12-18 06:20:00.000')
            and tdSql.compareData(4, 3, 77777)
            # Row 5: 2025-12-18 09:06:41.000
            and tdSql.compareData(5, 0, '2025-12-18 09:06:41.000')
            and tdSql.compareData(5, 1, True)
            and tdSql.compareData(5, 2, '2025-12-18 09:06:40.000')
            and tdSql.compareData(5, 3, 88888)
        )

    def check_s11(self):
        tdSql.checkResultsByFunc(
            sql="select * from r11",
            func=lambda: tdSql.getRows() == 6
            # Row 0: 2025-12-18 03:33:19.000
            and tdSql.compareData(0, 0, '2025-12-18 03:33:19.000')
            and tdSql.compareData(0, 1, True)
            and tdSql.compareData(0, 2, '2025-12-18 03:33:20.000')
            and tdSql.compareData(0, 3, 0)
            # Row 1: 2025-12-18 03:33:21.000
            and tdSql.compareData(1, 0, '2025-12-18 03:33:21.000')
            and tdSql.compareData(1, 1, False)
            and tdSql.compareData(1, 2, '2025-12-18 03:33:21.000')
            and tdSql.compareData(1, 3, 1000)
            # Row 2: 2025-12-18 06:19:58.000
            and tdSql.compareData(2, 0, '2025-12-18 06:19:58.000')
            and tdSql.compareData(2, 1, False)
            and tdSql.compareData(2, 2, '2025-12-18 06:19:58.000')
            and tdSql.compareData(2, 3, 8000)
            # Row 3: 2025-12-18 06:20:00.000
            and tdSql.compareData(3, 0, '2025-12-18 06:20:00.000')
            and tdSql.compareData(3, 1, False)
            and tdSql.compareData(3, 2, '2025-12-18 06:20:00.000')
            and tdSql.compareData(3, 3, 77777)
            # Row 4: 2025-12-18 06:20:03.000
            and tdSql.compareData(4, 0, '2025-12-18 06:20:03.000')
            and tdSql.compareData(4, 1, True)
            and tdSql.compareData(4, 2, '2025-12-18 09:06:40.000')
            and tdSql.compareData(4, 3, 88888)
            # Row 5: 2025-12-18 09:06:37.000
            and tdSql.compareData(5, 0, '2025-12-18 09:06:37.000')
            and tdSql.compareData(5, 1, True)
            and tdSql.compareData(5, 2, '2025-12-18 09:06:40.000')
            and tdSql.compareData(5, 3, 88888)
        )

    def check_s12(self):
        tdSql.checkResultsByFunc(
            sql="select * from r12",
            func=lambda: (
                tdSql.getRows() == 7
                # Row 0: 2025-12-18 03:33:19.000
                and tdSql.compareData(0, 0, '2025-12-18 03:33:19.000')
                and tdSql.compareData(0, 1, False)
                and tdSql.compareData(0, 2, '2025-12-18 03:33:19.000')
                and tdSql.compareData(0, 3, 1)
                # Row 1: 2025-12-18 03:33:21.000
                and tdSql.compareData(1, 0, '2025-12-18 03:33:21.000')
                and tdSql.compareData(1, 1, False)
                and tdSql.compareData(1, 2, '2025-12-18 03:33:21.000')
                and tdSql.compareData(1, 3, 3)
                # Row 2: 2025-12-18 06:19:58.000
                and tdSql.compareData(2, 0, '2025-12-18 06:19:58.000')
                and tdSql.compareData(2, 1, False)
                and tdSql.compareData(2, 2, '2025-12-18 06:19:58.000')
                and tdSql.compareData(2, 3, 2)
                # Row 3: 2025-12-18 06:20:00.000
                and tdSql.compareData(3, 0, '2025-12-18 06:20:00.000')
                and tdSql.compareData(3, 1, False)
                and tdSql.compareData(3, 2, '2025-12-18 06:20:00.000')
                and tdSql.compareData(3, 3, 1)
                # Row 4: 2025-12-18 06:20:03.000
                and tdSql.compareData(4, 0, '2025-12-18 06:20:03.000')
                and tdSql.compareData(4, 1, False)
                and tdSql.compareData(4, 2, '2025-12-18 06:20:03.000')
                and tdSql.compareData(4, 3, 2)
                # Row 5: 2025-12-18 09:06:37.000
                and tdSql.compareData(5, 0, '2025-12-18 09:06:37.000')
                and tdSql.compareData(5, 1, False)
                and tdSql.compareData(5, 2, '2025-12-18 09:06:37.000')
                and tdSql.compareData(5, 3, 1)
                # Row 6: 2025-12-18 09:06:41.000
                and tdSql.compareData(6, 0, '2025-12-18 09:06:41.000')
                and tdSql.compareData(6, 1, False)
                and tdSql.compareData(6, 2, '2025-12-18 09:06:41.000')
                and tdSql.compareData(6, 3, 2)
            )
        )

    def check_s13(self):
        tdSql.checkResultsByFunc(
            sql="select * from r13",
            func=lambda: (
                tdSql.getRows() == 7
                # Row 0: 2025-12-18 03:33:19.000 | 2025-12-12 12:12:00.000 | true | 2025-12-12 12:11:00.000 | 3
                and tdSql.compareData(0, 0, '2025-12-18 03:33:19.000')
                and tdSql.compareData(0, 1, '2025-12-12 12:12:00.000')
                and tdSql.compareData(0, 2, True)
                and tdSql.compareData(0, 3, '2025-12-12 12:11:00.000')
                and tdSql.compareData(0, 4, 3)
                # Row 1: 2025-12-18 03:33:21.000 | 2025-12-12 12:12:00.000 | true | 2025-12-12 12:11:00.000 | 3
                and tdSql.compareData(1, 0, '2025-12-18 03:33:21.000')
                and tdSql.compareData(1, 1, '2025-12-12 12:12:00.000')
                and tdSql.compareData(1, 2, True)
                and tdSql.compareData(1, 3, '2025-12-12 12:11:00.000')
                and tdSql.compareData(1, 4, 3)
                # Row 2: 2025-12-18 06:19:58.000 | 2025-12-12 12:12:00.000 | true | 2025-12-12 12:11:00.000 | 3
                and tdSql.compareData(2, 0, '2025-12-18 06:19:58.000')
                and tdSql.compareData(2, 1, '2025-12-12 12:12:00.000')
                and tdSql.compareData(2, 2, True)
                and tdSql.compareData(2, 3, '2025-12-12 12:11:00.000')
                and tdSql.compareData(2, 4, 3)
                # Row 3: 2025-12-18 06:20:00.000 | 2025-12-12 12:12:00.000 | true | 2025-12-12 12:11:00.000 | 3
                and tdSql.compareData(3, 0, '2025-12-18 06:20:00.000')
                and tdSql.compareData(3, 1, '2025-12-12 12:12:00.000')
                and tdSql.compareData(3, 2, True)
                and tdSql.compareData(3, 3, '2025-12-12 12:11:00.000')
                and tdSql.compareData(3, 4, 3)
                # Row 4: 2025-12-18 06:20:03.000 | 2025-12-12 12:12:00.000 | true | 2025-12-12 12:11:00.000 | 3
                and tdSql.compareData(4, 0, '2025-12-18 06:20:03.000')
                and tdSql.compareData(4, 1, '2025-12-12 12:12:00.000')
                and tdSql.compareData(4, 2, True)
                and tdSql.compareData(4, 3, '2025-12-12 12:11:00.000')
                and tdSql.compareData(4, 4, 3)
                # Row 5: 2025-12-18 09:06:37.000 | 2025-12-12 12:12:00.000 | true | 2025-12-12 12:11:00.000 | 3
                and tdSql.compareData(5, 0, '2025-12-18 09:06:37.000')
                and tdSql.compareData(5, 1, '2025-12-12 12:12:00.000')
                and tdSql.compareData(5, 2, True)
                and tdSql.compareData(5, 3, '2025-12-12 12:11:00.000')
                and tdSql.compareData(5, 4, 3)
                # Row 6: 2025-12-18 09:06:41.000 | 2025-12-12 12:12:00.000 | true | 2025-12-12 12:11:00.000 | 3
                and tdSql.compareData(6, 0, '2025-12-18 09:06:41.000')
                and tdSql.compareData(6, 1, '2025-12-12 12:12:00.000')
                and tdSql.compareData(6, 2, True)
                and tdSql.compareData(6, 3, '2025-12-12 12:11:00.000')
                and tdSql.compareData(6, 4, 3)
            )
        )

    def test_notify_table_merge_scan(self):
        """Interp query on super table

        1. testing interp query on super table. It will use merge scan instead
        of table scan. Test the functionality and guarantee the correctness and
        memory safety.

        Catalog:
            - Query:Interp

        Since: v3.4.1.0

        Labels: common,ci

        History:
            - 2026-01-12 Tony Zhang created

        """
        tdSql.execute("use fill_interp_test")

        # start from 1767196800000 = 2026-01-01 00:00:00.000
        # end at 1767696799000 = 2026-01-06 18:53:19
        # 500,000 rows per child table
        # from super table
        tdSql.query("""select _irowts, interp(v, 1), _irowts_origin, _isfilled
                    from stb range ("2025-12-31 23:59:59") fill(next)""")
        tdSql.checkData(0, 0, "2025-12-31 23:59:59.000")
        tdSql.checkData(0, 2, "2026-01-01 00:00:00.000")
        tdSql.checkData(0, 3, True)

        # partition by tbname
        tdSql.query("""select _irowts, interp(v, 1), _irowts_origin, _isfilled
                    , tbname from stb partition by tbname
                    range ("2026-01-07 00:00:00") fill(prev) order by tbname""")
        tdSql.checkData(0, 0, "2026-01-07 00:00:00.000")
        tdSql.checkData(0, 2, "2026-01-06 18:53:19.000")
        tdSql.checkData(0, 3, True)
        tdSql.checkData(1, 0, "2026-01-07 00:00:00.000")
        tdSql.checkData(1, 2, "2026-01-06 18:53:19.000")
        tdSql.checkData(1, 3, True)

        # partition by tag
        tdSql.query("""select _irowts, interp(v, 1), _irowts_origin, _isfilled
                    from stb partition by zone
                    range ("2026-01-07 00:00:00") fill(prev)""")
        tdSql.checkData(0, 0, "2026-01-07 00:00:00.000")
        tdSql.checkData(0, 2, "2026-01-06 18:53:19.000")
        tdSql.checkData(0, 3, True)

    #
    # ------------------- extend ----------------
    #
    def create_database(self, tsql, dbName, dropFlag=1, vgroups=2, replica=1, duration: str = '1d'):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s" % (dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d duration %s" % (
            dbName, vgroups, replica, duration))
        tdLog.debug("complete to create database %s" % (dbName))
        tsql.execute("use %s" % (dbName))
        return

    def create_stable(self, tsql, paraDict):
        colString = tdCom.gen_column_type_str(
            colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(
            tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)" % (
            paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s" % (sqlString))
        tsql.execute(sqlString)
        return

    def create_ctable(self, tsql=None, dbName='dbx', stbName='stb', ctbPrefix='ctb', ctbNum=1, ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)" % (dbName, ctbPrefix, i+ctbStartIdx, dbName, stbName, (i+ctbStartIdx) % 5, i+ctbStartIdx + random.randint(
                1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100))
            tsql.execute(sqlString)

        tdLog.debug("complete to create %d child tables by %s.%s" %
                    (ctbNum, dbName, stbName))
        return

    def init_normal_tb(self, tsql, db_name: str, tb_name: str, rows: int, start_ts: int, ts_step: int):
        sql = 'CREATE TABLE %s.%s (ts timestamp, c1 INT, c2 INT, c3 INT, c4 double, c5 VARCHAR(255))' % (
            db_name, tb_name)
        tsql.execute(sql)
        sql = 'INSERT INTO %s.%s values' % (db_name, tb_name)
        for j in range(rows):
            sql += f'(%d, %d,%d,%d,{random.random()},"varchar_%d"),' % (start_ts + j * ts_step + randrange(500), j %
                                                     10 + randrange(200), j % 10, j % 10, j % 10 + randrange(100))
        tsql.execute(sql)

    def insert_data(self, tsql, dbName, ctbPrefix, ctbNum, rowsPerTbl, batchNum, startTs, tsStep):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" % dbName)
        pre_insert = "insert into "
        sql = pre_insert

        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s.%s%d values " % (dbName, ctbPrefix, i)
            for j in range(rowsPerTbl):
                if (i < ctbNum/2):
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d') " % (startTs + j*tsStep + randrange(
                        500), j % 10 + randrange(100), j % 10 + randrange(200), j % 10, j % 10, j % 10, j % 10, j % 10, j % 10)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,'binary%d', 'nchar%d') " % (
                        startTs + j*tsStep + randrange(500), j % 10, j % 10, j % 10, j % 10, j % 10, j % 10)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s%d values " % (dbName, ctbPrefix, i)
                    else:
                        sql = "insert into "
        if sql != pre_insert:
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def init_data(self, db: str = 'test', ctb_num: int = 10, rows_per_ctb: int = 10000, start_ts: int = 1537146000000, ts_step: int = 500):
        tdLog.printNoPrefix(
            "======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     db,
                    'dropFlag':   1,
                    'vgroups':    4,
                    'stbName':    'meters',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count': 1}, {'type': 'BIGINT', 'count': 1}, {'type': 'FLOAT', 'count': 1}, {'type': 'DOUBLE', 'count': 1}, {'type': 'smallint', 'count': 1}, {'type': 'tinyint', 'count': 1}, {'type': 'bool', 'count': 1}, {'type': 'binary', 'len': 10, 'count': 1}, {'type': 'nchar', 'len': 10, 'count': 1}],
                    'tagSchema':   [{'type': 'INT', 'count': 1}, {'type': 'nchar', 'len': 20, 'count': 1}, {'type': 'binary', 'len': 20, 'count': 1}, {'type': 'BIGINT', 'count': 1}, {'type': 'smallint', 'count': 1}, {'type': 'DOUBLE', 'count': 1}],
                    'ctbPrefix':  't',
                    'ctbStartIdx': 0,
                    'ctbNum':     ctb_num,
                    'rowsPerTbl': rows_per_ctb,
                    'batchNum':   3000,
                    'startTs':    start_ts,
                    'tsStep':     ts_step}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = ctb_num
        paraDict['rowsPerTbl'] = rows_per_ctb

        tdLog.info("create database")
        self.create_database(tsql=tdSql, dbName=paraDict["dbName"], dropFlag=paraDict["dropFlag"],
                             vgroups=paraDict["vgroups"], replica=self.replicaVar, duration=self.duraion)

        tdLog.info("create stb")
        self.create_stable(tsql=tdSql, paraDict=paraDict)

        tdLog.info("create child tables")
        self.create_ctable(tsql=tdSql, dbName=paraDict["dbName"],
                           stbName=paraDict["stbName"], ctbPrefix=paraDict["ctbPrefix"],
                           ctbNum=paraDict["ctbNum"], ctbStartIdx=paraDict["ctbStartIdx"])
        self.insert_data(tsql=tdSql, dbName=paraDict["dbName"],
                         ctbPrefix=paraDict["ctbPrefix"], ctbNum=paraDict["ctbNum"],
                         rowsPerTbl=paraDict["rowsPerTbl"], batchNum=paraDict["batchNum"],
                         startTs=paraDict["startTs"], tsStep=paraDict["tsStep"])
        self.init_normal_tb(tdSql, paraDict['dbName'], 'norm_tb',
                            paraDict['rowsPerTbl'], paraDict['startTs'], paraDict['tsStep'])

    def test_interp_extension(self):
        """Interp fill extension

        1. Query interp fill extension with large data volume
        2. Query interp fill extension with near mode
        3. Query interp fill extension with linear mode
        4. Query interp fill extension with prev mode
        5. Query interp fill extension with next mode
        6. Query interp fill extension with value mode
        7. Query interp fill extension with null mode
        8. Multi-threaded query interp fill extension

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-13 Alex Duan Migrated from uncatalog/system-test/2-query/test_interp_extension.py

        """

        self.vgroups = 4
        self.ctbNum = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'

        self.init_data()
        self.check_interp_extension()

        #tdSql.close()


    def datetime_add_tz(self, dt):
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return dt.replace(tzinfo=get_localzone())
        return dt

    def binary_search_ts(self, select_results, ts):
        mid = 0
        try:
            found: bool = False
            start = 0
            end = len(select_results) - 1
            while start <= end:
                mid = (start + end) // 2
                if self.datetime_add_tz(select_results[mid][0]) == ts:
                    found = True
                    return mid
                elif self.datetime_add_tz(select_results[mid][0]) < ts:
                    start = mid + 1
                else:
                    end = mid - 1

            if not found:
                tdLog.exit(f"cannot find ts in select results {ts} {select_results}")
            return start
        except Exception as e:
            tdLog.debug(f"{select_results[mid][0]}, {ts}, {len(select_results)}, {select_results[mid]}")
            self.check_failed = True
            tdLog.exit(f"binary_search_ts error: {e}")

    def distance(self, ts1, ts2):
        return abs(self.datetime_add_tz(ts1) - self.datetime_add_tz(ts2))

    ## TODO pass last position to avoid search from the beginning
    def is_nearest(self, select_results, irowts_origin, irowts):
        if len(select_results) <= 1:
            return True
        try:
            #tdLog.debug(f"check is_nearest for: {irowts_origin} {irowts}")
            idx = self.binary_search_ts(select_results, irowts_origin)
            if idx == 0:
                #tdLog.debug(f"prev row: null,cur row: {select_results[idx]}, next row: {select_results[idx + 1]}")
                res = self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx + 1][0])
                if not res:
                    tdLog.debug(f"prev row: null,cur row: {select_results[idx]}, next row: {select_results[idx + 1]}, irowts_origin: {irowts_origin}, irowts: {irowts}")
                return res
            if idx == len(select_results) - 1:
                #tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: null")
                res = self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx - 1][0])
                if not res:
                    tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: null, irowts_origin: {irowts_origin}, irowts: {irowts}")
                return res
            #tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: {select_results[idx + 1]}")
            res = self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx - 1][0]) and self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx + 1][0])
            if not res:
                tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: {select_results[idx + 1]}, irowts_origin: {irowts_origin}, irowts: {irowts}")
            return res
        except Exception as e:
            self.check_failed = True
            tdLog.exit(f"is_nearest error: {e}")

    ## interp_results: _irowts_origin, _irowts, ..., _isfilled
    ## select_all_results must be sorted by ts in ascending order
    def check_result_for_near(self, interp_results, select_all_results, sql, sql_select_all):
        #tdLog.info(f"check_result_for_near for sql: {sql}, sql_select_all: {sql_select_all}, interp_results: {interp_results}")
        for row in interp_results:
            #tdLog.info(f"row: {row}")
            if row[0].tzinfo is None or row[0].tzinfo.utcoffset(row[0]) is None:
                irowts_origin = row[0].replace(tzinfo=get_localzone())
                irowts = row[1].replace(tzinfo=get_localzone())
            else:
                irowts_origin = row[0]
                irowts = row[1]
            if not self.is_nearest(select_all_results, irowts_origin, irowts):
                self.check_failed = True
                tdLog.exit(f"interp result is not the nearest for row: {row}, {sql}")

    def query_routine(self, sql_queue: queue.Queue, output_queue: queue.Queue):
        try:
            tdcom = TDCom()
            cli = tdcom.newTdSql()
            while True:
                item = sql_queue.get()
                if item is None or self.check_failed:
                    output_queue.put(None)
                    break
                (sql, sql_select_all, _) = item
                cli.query(sql, queryTimes=1)
                interp_results = cli.queryResult
                if sql_select_all is not None:
                    cli.query(sql_select_all, queryTimes=1)
                output_queue.put((sql, interp_results, cli.queryResult, sql_select_all))
            cli.close()
        except Exception as e:
            self.check_failed = True
            tdLog.exit(f"query_routine error: {e}")

    def interp_check_near_routine(self, select_all_results, output_queue: queue.Queue):
        try:
            while True:
                item = output_queue.get()
                if item is None:
                    break
                (sql, interp_results, all_results, sql_select_all) = item
                if all_results is not None:
                    self.check_result_for_near(interp_results, all_results, sql, sql_select_all)
                else:
                    self.check_result_for_near(interp_results, select_all_results, sql, None)
        except Exception as e:
            self.check_failed = True
            tdLog.error(f"error sql: {sql} sql_select_all: {sql_select_all}")
            tdLog.exit(f"interp_check_near_routine error: {e}")

    def create_qt_threads(self, sql_queue: queue.Queue, output_queue: queue.Queue, num: int):
        qts = []
        for _ in range(0, num):
            qt = threading.Thread(target=self.query_routine, args=(sql_queue, output_queue))
            qt.start()
            qts.append(qt)
        return qts

    def wait_qt_threads(self, qts: list):
        for qt in qts:
            qt.join()

    ### first(ts)               | last(ts)
    ### 2018-09-17 09:00:00.047 | 2018-09-17 10:23:19.863
    def check_interp_fill_extension_near(self):
        sql = f"select last(ts), c1, c2 from test.t0"
        tdSql.query(sql, queryTimes=1)
        lastRow = tdSql.queryResult[0]
        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(61)
        for i in range(0, 61):
            tdSql.checkData(i, 0, lastRow[0])
            tdSql.checkData(i, 2, lastRow[1])
            tdSql.checkData(i, 3, lastRow[2])
            tdSql.checkData(i, 4, True)

        sql = f"select ts, c1, c2 from test.t0 where ts between '2018-09-17 08:59:59' and '2018-09-17 09:00:06' order by ts asc"
        tdSql.query(sql, queryTimes=1)
        select_all_results = tdSql.queryResult
        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range('2018-09-17 09:00:00', '2018-09-17 09:00:05') every(1s) fill(near)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(6)
        self.check_result_for_near(tdSql.queryResult, select_all_results, sql, None)

        start = 1537146000000
        end = 1537151000000

        tdSql.query("select ts, c1, c2 from test.t0 order by ts asc", queryTimes=1)
        select_all_results = tdSql.queryResult

        qt_threads_num = 4
        sql_queue = queue.Queue()
        output_queue = queue.Queue()
        qts = self.create_qt_threads(sql_queue, output_queue, qt_threads_num)
        ct = threading.Thread(target=self.interp_check_near_routine, args=(select_all_results, output_queue))
        ct.start()
        for i in range(0, ROUND):
            range_start = random.randint(start, end)
            range_end = random.randint(range_start, end)
            every = random.randint(1, 15)
            #tdLog.debug(f"range_start: {range_start}, range_end: {range_end}")
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range({range_start}, {range_end}) every({every}s) fill(near)"
            sql_queue.put((sql, None, None))

        ### no prev only, no next only, no prev and no next, have prev and have next
        for i in range(0, ROUND):
            range_point = random.randint(start, end)
            ## all data points are can be filled by near
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range({range_point}, 1h) fill(near, 1, 2)"
            sql_queue.put((sql, None, None))

        for i in range(0, ROUND):
            range_start = random.randint(start, end)
            range_end = random.randint(range_start, end)
            range_where_start = random.randint(start, end)
            range_where_end = random.randint(range_where_start, end)
            every = random.randint(1, 15)
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 where ts between {range_where_start} and {range_where_end} range({range_start}, {range_end}) every({every}s) fill(near)"
            tdSql.query(f'select to_char(cast({range_where_start} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\'), to_char(cast({range_where_end} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\')', queryTimes=1)
            where_start_str = tdSql.queryResult[0][0]
            where_end_str = tdSql.queryResult[0][1]
            sql_select_all = f"select ts, c1, c2 from test.t0 where ts between '{where_start_str}' and '{where_end_str}' order by ts asc"
            sql_queue.put((sql, sql_select_all, None))

        for i in range(0, ROUND):
            range_start = random.randint(start, end)
            range_end = random.randint(range_start, end)
            range_where_start = random.randint(start, end)
            range_where_end = random.randint(range_where_start, end)
            range_point = random.randint(start, end)
            # 检查range_point与range_where_start或range_where_end的间隔是否小于等于1小时(3600000毫秒)
            one_hour_ms = 3600000
            if ((range_point < range_where_start and abs(range_point - range_where_start) > one_hour_ms) or
                (range_point > range_where_end and abs(range_point - range_where_end) > one_hour_ms)):
                continue
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 where ts between {range_where_start} and {range_where_end} range({range_point}, 1h) fill(near, 1, 2)"
            tdSql.query(f'select to_char(cast({range_where_start} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\'), to_char(cast({range_where_end} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\')', queryTimes=1)
            where_start_str = tdSql.queryResult[0][0]
            where_end_str = tdSql.queryResult[0][1]
            sql_select_all = f"select ts, c1, c2 from test.t0 where ts between '{where_start_str}' and '{where_end_str}' order by ts asc"
            sql_queue.put((sql, sql_select_all, None))
        for i in range(0, qt_threads_num):
            sql_queue.put(None)
        self.wait_qt_threads(qts)
        ct.join()

        if self.check_failed:
            tdLog.exit("interp check near failed")

    def check_interp_extension_irowts_origin(self):
        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near)"
        tdSql.query(sql, queryTimes=1)

        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(NULL)"
        tdSql.error(sql, -2147473833)
        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(linear)"
        tdSql.error(sql, -2147473833)
        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(NULL_F)"
        tdSql.error(sql, -2147473833)

    def check_interp_fill_extension(self):
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near, 0, 0)"
        tdSql.query(sql, queryTimes=1)

        ### must specify value
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near)"
        tdSql.error(sql, -2147473915, "Too few fill values specified")
        ### num of fill value mismatch
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near, 1)"
        tdSql.error(sql, -2147473915, "Too few fill values specified")
        ### num of fill value mismatch
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near, 1, 2, 3)"
        tdSql.error(sql, -2147473915, "Too many fill values specified")

        ### missing every clause
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00', 1h) fill(near, 1, 1)"
        tdSql.error(sql, -2147473827) ## TSDB_CODE_PAR_INVALID_INTERP_CLAUSE

        ### NULL/linear cannot specify other values
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00') fill(NULL, 1, 1)"
        tdSql.error(sql, -2147473920)

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00') fill(linear, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        ### cannot have every clause with range around
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) every(1s) fill(prev, 1, 1)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)

        ### cannot specify near/prev/next values when no surrounding time or range around
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near, 1, 1)"
        tdSql.error(sql, -2147473747) ## cannot specify values

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00') every(1s) fill(near, 1, 1)"
        tdSql.error(sql, -2147473747) ## cannot specify values

        ### when range around interval is set, only prev/next/near modes are supported
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(NULL, 1, 1)"
        tdSql.error(sql, -2147473920)
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(NULL)"
        tdSql.error(sql, -2147473746)
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(linear, 1, 1)"
        tdSql.error(sql, -2147473920)
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(linear)"
        tdSql.error(sql, -2147473746)

        ### range interval cannot be 0
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 0h) fill(near, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1y) fill(near, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1n) fill(near, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters where ts between '2020-02-01 00:00:00' and '2020-02-01 00:00:00' range('2020-02-01 00:00:00', 1h) fill(near, 1, 1)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(0)

        ### first(ts)               | last(ts)
        ### 2018-09-17 09:00:00.047 | 2018-09-17 10:23:19.863
        sql = "select to_char(first(ts), 'YYYY-MM-DD HH24:MI:SS.MS') from test.meters"
        tdSql.query(sql, queryTimes=1)
        first_ts = tdSql.queryResult[0][0]
        sql = "select to_char(last(ts), 'YYYY-MM-DD HH24:MI:SS.MS') from test.meters"
        tdSql.query(sql, queryTimes=1)
        last_ts = tdSql.queryResult[0][0]
        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1d) fill(near, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, '2020-02-01 00:00:00.000')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-18 10:25:00', 1d) fill(prev, 3, 4)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, '2018-09-18 10:25:00.000')
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 4)
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-16 08:25:00', 1d) fill(next, 5, 6)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, '2018-09-16 08:25:00.000')
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 6)
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-16 09:00:01', 1d) fill(next, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, first_ts)
        tdSql.checkData(0, 1, '2018-09-16 09:00:01')
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-18 10:23:19', 1d) fill(prev, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)
        tdSql.checkData(0, 1, '2018-09-18 10:23:19')
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('{last_ts}', 1a) fill(next, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)
        tdSql.checkData(0, 1, last_ts)
        tdSql.checkData(0, 4, False)

    def check_interval_fill_extension(self):
        ## not allowed
        sql = f"select count(*) from test.meters interval(1s) fill(near)"
        tdSql.error(sql, -2147473748) ## TSDB_CODE_PAR_NOT_ALLOWED_FILL_MODE

        sql = f"select count(*) from test.meters interval(1s) fill(prev, 1)"
        tdSql.error(sql, -2147473747) ## TSDB_CODE_PAR_NOT_ALLOWED_FILL_VALUES
        sql = f"select count(*) from test.meters interval(1s) fill(next, 1)"
        tdSql.error(sql, -2147473747) ## TSDB_CODE_PAR_NOT_ALLOWED_FILL_VALUES

        sql = f"select _irowts_origin, count(*) from test.meters where ts between '2018-09-17 08:59:59' and '2018-09-17 09:00:06' interval(1s) fill(next)"
        tdSql.error(sql, -2147473918) ## invalid column name _irowts_origin

    def check_interp_fill_extension_stream(self):
        ## near is not supported
        sql = f"create stream s1 trigger force_window_close into test.s_res_tb as select _irowts, interp(c1), interp(c2)from test.meters partition by tbname every(1s) fill(near);"
        tdSql.error(sql, -2147473851) ## TSDB_CODE_PAR_INVALID_STREAM_QUERY

        ## _irowts_origin is not support
        sql = f"create stream s1 trigger force_window_close into test.s_res_tb as select _irowts_origin, interp(c1), interp(c2)from test.meters partition by tbname every(1s) fill(prev);"
        tdSql.error(sql, -2147473851) ## TSDB_CODE_PAR_INVALID_STREAM_QUERY

        sql = f"create stream s1 trigger force_window_close into test.s_res_tb as select _irowts, interp(c1), interp(c2)from test.meters partition by tbname every(1s) fill(next, 1, 1);"
        tdSql.error(sql, -2147473915) ## cannot specify values

    def check_interp_extension(self):
        self.check_interp_fill_extension_near()
        self.check_interp_extension_irowts_origin()
        self.check_interp_fill_extension()
        self.check_interval_fill_extension()

    def test_interp_fill_surround(self):
        """Interp normal query of interp filling with surround

        1. testing interp filling with surrounding time

        Catalog:
            - Query:Interp

        Since: v3.4.1.0

        Labels: common,ci

        History:
            - 2026-01-19 Tony Zhang created

        """
        tdSql.execute("use test_surround")

        testCase = "interp_fill_surround"
        # read sql from .sql file and execute
        tdLog.info("test interp fill surround.")
        self.sqlFile = etool.curFile(__file__, f"in/{testCase}.in")
        self.ansFile = etool.curFile(__file__, f"ans/{testCase}.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_interp_fill_surround_stream(self):
        """Interp normal query of interp filling with surround in stream

        1. testing interp filling with surrounding time in stream

        Catalog:
            - Query:Interp

        Since: v3.4.1.0

        Labels: common,ci

        History:
            - 2026-01-19 Tony Zhang created

        """
        tdSql.execute("use test_surround")
        tdSql.execute("create table triggertb (ts timestamp, c1 int)")
        streams: list[StreamItem] = []

        stream: StreamItem = StreamItem(
            id=14,
            stream="""create stream s14 state_window(c1) from triggertb into
                res14 as select _irowts, interp(c1, 1) from %%trows
                range(_twstart, _twend) every(12h) fill(prev) surround(1d, 100)""",
            check_func=self.check_s14,
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=15,
            stream="""create stream s15 state_window(c1) from triggertb into
                res15 as select _irowts, interp(c1, 1) from test_surround.stb
                range(_twstart, _twend) every(1d) fill(next) surround(1d, 100)""",
            check_func=self.check_s15,
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id=16,
            stream="""create stream s16 state_window(c1) from triggertb into
                res16 as select _irowts, interp(c1, 1), _irowts_origin from
                test_surround.ntb range(_twstart, _twend) every(1d) fill(near)
                surround(1d, 100)""",
            check_func=self.check_s16,
        )
        streams.append(stream)

        for stream in streams:
            stream.createStream()
        tdStream.checkStreamStatus()

        tdSql.execute("""insert into triggertb values
            ('2026-01-01 12:00:00', 1),
            ('2026-01-02 00:00:00', 1),
            ('2026-01-03 00:00:00', 2),
            ('2026-01-07 12:00:00', 2),
            ('2026-01-09 10:00:00', 1),
            ('2026-01-09 15:00:00', 1),
            ('2026-01-10 12:00:00', 3)
            """)

        for stream in streams:
            stream.checkResults()

    def check_s14(self):
        tdSql.checkResultsByFunc(
            sql="select * from res14",
            func=lambda: tdSql.getRows() == 13
            and tdSql.compareData(0, 0, "2026-01-01 12:00:00.000") and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(1, 0, "2026-01-02 00:00:00.000") and tdSql.compareData(1, 1, 1)
            and tdSql.compareData(2, 0, "2026-01-03 00:00:00.000") and tdSql.compareData(2, 1, 2)
            and tdSql.compareData(3, 0, "2026-01-03 12:00:00.000") and tdSql.compareData(3, 1, 2)
            and tdSql.compareData(4, 0, "2026-01-04 00:00:00.000") and tdSql.compareData(4, 1, 2)
            and tdSql.compareData(5, 0, "2026-01-04 12:00:00.000") and tdSql.compareData(5, 1, 100)
            and tdSql.compareData(6, 0, "2026-01-05 00:00:00.000") and tdSql.compareData(6, 1, 100)
            and tdSql.compareData(7, 0, "2026-01-05 12:00:00.000") and tdSql.compareData(7, 1, 100)
            and tdSql.compareData(8, 0, "2026-01-06 00:00:00.000") and tdSql.compareData(8, 1, 100)
            and tdSql.compareData(9, 0, "2026-01-06 12:00:00.000") and tdSql.compareData(9, 1, 100)
            and tdSql.compareData(10, 0, "2026-01-07 00:00:00.000") and tdSql.compareData(10, 1, 100)
            and tdSql.compareData(11, 0, "2026-01-07 12:00:00.000") and tdSql.compareData(11, 1, 2)
            and tdSql.compareData(12, 0, "2026-01-09 10:00:00.000") and tdSql.compareData(12, 1, 1)
        )

    def check_s15(self):
        tdSql.checkResultsByFunc(
            sql="select * from res15",
            func=lambda: tdSql.getRows() == 7
            and tdSql.compareData(0, 0, "2026-01-01 12:00:00.000") and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(1, 0, "2026-01-03 00:00:00.000") and tdSql.compareData(1, 1, 1)
            and tdSql.compareData(2, 0, "2026-01-04 00:00:00.000") and tdSql.compareData(2, 1, 2)
            and tdSql.compareData(3, 0, "2026-01-05 00:00:00.000") and tdSql.compareData(3, 1, 2)
            and tdSql.compareData(4, 0, "2026-01-06 00:00:00.000") and tdSql.compareData(4, 1, 2)
            and tdSql.compareData(5, 0, "2026-01-07 00:00:00.000") and tdSql.compareData(5, 1, 3)
            and tdSql.compareData(6, 0, "2026-01-09 10:00:00.000") and tdSql.compareData(6, 1, 3)
        )

    def check_s16(self):
        tdSql.checkResultsByFunc(
            sql="select * from res16",
            func=lambda: tdSql.getRows() == 7
            and tdSql.compareData(0, 0, "2026-01-01 12:00:00.000") and tdSql.compareData(0, 1, 1) and tdSql.compareData(0, 2, "2026-01-01 12:00:00.000")
            and tdSql.compareData(1, 0, "2026-01-03 00:00:00.000") and tdSql.compareData(1, 1, 100) and tdSql.compareData(1, 2, None)
            and tdSql.compareData(2, 0, "2026-01-04 00:00:00.000") and tdSql.compareData(2, 1, 100) and tdSql.compareData(2, 2, None)
            and tdSql.compareData(3, 0, "2026-01-05 00:00:00.000") and tdSql.compareData(3, 1, 100) and tdSql.compareData(3, 2, None)
            and tdSql.compareData(4, 0, "2026-01-06 00:00:00.000") and tdSql.compareData(4, 1, 2) and tdSql.compareData(4, 2, "2026-01-06 12:00:00.000")
            and tdSql.compareData(5, 0, "2026-01-07 00:00:00.000") and tdSql.compareData(5, 1, 2) and tdSql.compareData(5, 2, "2026-01-06 12:00:00.000")
            and tdSql.compareData(6, 0, "2026-01-09 10:00:00.000") and tdSql.compareData(6, 1, 3) and tdSql.compareData(6, 2, "2026-01-09 12:00:00.000")
        )

    def test_interp_fill_surround_abnormal(self):
        """Interp abnormal query of interp filling with surround

        1. testing abnormal parameters of interp filling with surrounding time

        Catalog:
            - Query:Interp

        Since: v3.4.1.0

        Labels: common,ci

        History:
            - 2026-01-19 Tony Zhang created

        """
        tdSql.execute("use test_surround")
        for mode in ["prev", "next", "near"]:
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(1h)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(1h, 1, 2)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}, 1) surround(1h, 2)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}, 1) surround(1h)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00', 1h) fill({mode}) surround(1h)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00', 1h) fill({mode}, 1) surround(1h)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00', 1h) fill({mode}, 1) surround(1h, 2)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00', 1h) fill({mode})")

            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround('1s', 1)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(1, 2)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(1d+1d, 2)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(1h, (select avg(c1) from ntb))")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(0h, 2)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(-1s, 2)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(1b, 2)")  # smaller than database precision
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(1y, 2)")  # smaller than database precision
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(1n, 2)")  # smaller than database precision
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(1s, a)")
            tdSql.error(f"select interp(c1) from ntb range('2026-01-01 12:00:00') fill({mode}) surround(1s, !)")

        tdSql.error("select interp(c1) from ntb range('2026-01-01 12:00:00') surround(1h, 2)")
        tdSql.error("select interp(c1) from ntb range('2026-01-01 12:00:00') fill(linear) surround(1h, 1)")
        tdSql.error("select interp(c1) from ntb range('2026-01-01 12:00:00') fill(none) surround(1h, 1)")
        tdSql.error("select interp(c1) from ntb range('2026-01-01 12:00:00') fill(null) surround(1h, 1)")
        tdSql.error("select interp(c1) from ntb range('2026-01-01 12:00:00') fill(null_f) surround(1h, 1)")
        tdSql.error("select interp(c1) from ntb range('2026-01-01 12:00:00') fill(value, 1) surround(1h, 1)")
        tdSql.error("select interp(c1) from ntb range('2026-01-01 12:00:00') fill(value_f, 1) surround(1h, 1)")
        tdSql.error("select interp(c2) from ntb range('2026-01-01 12:00:00') fill(prev) surround(1h, 'd')")
