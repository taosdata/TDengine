import math
import numpy as np
import platform
import random
import re
import subprocess
import sys
import time
import os

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck,tdDnodes
from wsgiref.headers import tspecials

msec_per_min=60*1000


class TestKeepOrderFunc:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    #
    # ------------------ main ------------------
    #
    def test_win_keep_order_func(self):
        """ Fun: KEEP_ORDER_FUNC
        
        1. Functionality test for KEEP_ORDER_FUNC
        2. Query on super/child table
        3. Query with interval/state_window/session_window/count_window
        4. Query with max/min/statecount/stateduration/mavg/tail/mode
        5. Query with top/bottom/sample

        Since: v3.4.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
        History:
            - 2026-01-19 xsRen Create the KEEP_ORDER_FUNC function test case

        """
        
        self.prepare_data()

        self.unstable_result_func()

        testCases = [
            "test_win_keep_order_func_baisic",
            "test_win_keep_order_func_subquery",
        ]
        for testCase in testCases:
            tdLog.info(f"test {testCase} case")
            self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
            self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

            tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)
     
    def prepare_data(self):
        ts = 1741757485230
        tdSql.execute("drop database if exists keeporderdb")
        tdSql.execute("create database keeporderdb vgroups 2 replica 1")
        tdSql.execute("use keeporderdb")
        tdSql.execute("CREATE STABLE keeporderdb.`meters` (`ts` TIMESTAMP, `ts2` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` VARCHAR(24))")
        for tableIndex in range(10):
            tdSql.execute(f"CREATE TABLE keeporderdb.t{tableIndex} USING keeporderdb.meters TAGS ({tableIndex}, 'tb{tableIndex}')")
            for num in range(700):
                if(num >= 10 and num < 11) or (num >= 30 and num < 32) or (num >= 50 and num < 53) or (num >= 80 and num < 84) or \
                    (num >= 120 and num < 125) or (num >= 170 and num < 176) or (num >= 230 and num < 237) or (num >= 300 and num < 308) or \
                    (num >= 380 and num < 389) or (num >= 470 and num < 480) or (num >= 570 and num < 581) or (num >= 680 and num < 692):
                    continue
                tdSql.execute(f"INSERT INTO keeporderdb.t{tableIndex} VALUES({ts + num * 1000}, {ts + (num % 13) * 1000}, {num * 1.0}, {215 + num/15}, 0.0)")

    def check_query_rows_and_prefix(self, sql, expected_rows, expected_prefix):
        tdSql.query(sql)
        tdSql.checkRows(expected_rows)
        for row_idx, row in enumerate(expected_prefix):
            for col_idx, expected in enumerate(row):
                tdSql.checkData(row_idx, col_idx, expected)

    def check_long_result_sqls(self):
        checks = [
            (
                "select _wstart, _wend, _wduration, statecount(voltage, 'LE', 223) "
                "from keeporderdb.meters session(ts, 1s);",
                6220,
                [
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:34.230", 9000, 1],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:34.230", 9000, 2],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:34.230", 9000, 3],
                ],
            ),
            (
                "select _wstart, _wend, _wduration, mavg(voltage, 4) "
                "from keeporderdb.meters session(ts, 1s);",
                6181,
                [
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:34.230", 9000, 215],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:34.230", 9000, 215],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:34.230", 9000, 215],
                ],
            ),
            (
                "select _wstart, _wend, _wduration, count(*) from keeporderdb.meters count_window(3);",
                2074,
                [
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 3],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 3],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 3],
                ],
            ),
            (
                "select _wstart, _wend, _wduration, max(current) from keeporderdb.meters count_window(3);",
                2074,
                [
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 0],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 0],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 0],
                ],
            ),
            (
                "select _wstart, _wend, _wduration, min(current) from keeporderdb.meters count_window(3);",
                2074,
                [
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 0],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 0],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 0],
                ],
            ),
            (
                "select _wstart, _wend, _wduration, top(voltage, 1) "
                "from keeporderdb.meters count_window(3);",
                2074,
                [
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                ],
            ),
            (
                "select _wstart, _wend, _wduration, top(voltage, 2) "
                "from keeporderdb.meters count_window(3);",
                4148,
                [
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                ],
            ),
            (
                "select _wstart, _wend, _wduration, bottom(voltage, 1) "
                "from keeporderdb.meters count_window(3);",
                2074,
                [
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                ],
            ),
            (
                "select _wstart, _wend, _wduration, bottom(voltage, 2) "
                "from keeporderdb.meters count_window(3);",
                4148,
                [
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 215],
                ],
            ),
            (
                "select _wstart, _wend, _wduration, statecount(voltage, 'LE', 223) "
                "from keeporderdb.meters count_window(3);",
                6220,
                [
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 1],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 2],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 3],
                ],
            ),
            (
                "select _wstart, _wend, _wduration, mode(current) from keeporderdb.meters count_window(3);",
                2074,
                [
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 0],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 0],
                    ["2025-03-12 13:31:25.230", "2025-03-12 13:31:25.230", 0, 0],
                ],
            ),
            ("select statecount(voltage, 'LE', 223) from keeporderdb.meters;", 6220, [[1], [2], [3]]),
            ("select mavg(voltage, 4) from keeporderdb.meters;", 6217, [[215], [215], [215]]),
            (
                "select statecount(voltage, 'LE', 223) from "
                "(select ts, current, voltage from keeporderdb.meters order by ts desc) interval(30s);",
                6220,
                [[-1], [-1], [-1]],
            ),
            (
                "select mavg(voltage, 4) from "
                "(select ts, current, voltage from keeporderdb.meters order by ts desc) interval(30s);",
                6145,
                [[262], [262], [262]],
            ),
        ]

        for sql, expected_rows, expected_prefix in checks:
            self.check_query_rows_and_prefix(sql, expected_rows, expected_prefix)

    def unstable_result_func(self):
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.meters session(ts, 1s) order by _wstart limit 2;"
        tdSql.query(sql)
        tdSql.checkRows(2)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.meters session(ts, 1s) order by _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(13)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.meters session(ts, 3s) order by _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(11)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.meters session(ts, 4s) order by _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(10)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.meters session(ts, 10s) order by _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(4)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.meters partition by tbname count_window(300) order by tbname, _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(30)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.t1 partition by tbname count_window(300) order by tbname;"
        tdSql.query(sql)
        tdSql.checkRows(3)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.meters where ts > '2025-03-12 13:30:18.230' and ts < '2025-03-12 13:32:03.230' partition by tbname state_window(voltage) order by tbname, _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(30)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.t1 where ts > '2025-03-12 13:30:18.230' and ts < '2025-03-12 13:32:03.230' partition by tbname state_window(voltage) order by tbname;"
        tdSql.query(sql)
        tdSql.checkRows(3)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.meters partition by tbname  event_window start with (voltage >= 215 and voltage < 217) end with voltage >= 217 order by tbname, _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(10)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.t1 event_window start with (voltage >= 215 and voltage < 217) end with voltage >= 217;"
        tdSql.query(sql)
        tdSql.checkRows(1)

        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 2) from keeporderdb.meters session(ts, 1s) order by _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(26)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 2) from keeporderdb.meters session(ts, 3s) order by _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(22)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 2) from keeporderdb.meters session(ts, 4s) order by _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(20)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 2) from keeporderdb.meters session(ts, 10s) order by _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(8)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 2) from keeporderdb.meters partition by tbname count_window(300) order by tbname, _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(60)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 2) from keeporderdb.t1 partition by tbname count_window(300) order by tbname;"
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 2) from keeporderdb.meters where ts > '2025-03-12 13:30:18.230' and ts < '2025-03-12 13:32:03.230' partition by tbname state_window(voltage) order by tbname, _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(60)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 2) from keeporderdb.t1 where ts > '2025-03-12 13:30:18.230' and ts < '2025-03-12 13:32:03.230' partition by tbname state_window(voltage) order by tbname;"
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 2) from keeporderdb.meters partition by tbname  event_window start with (voltage >= 215 and voltage < 217) end with voltage >= 217 order by tbname, _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(20)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 2) from keeporderdb.t1 event_window start with (voltage >= 215 and voltage < 217) end with voltage >= 217;"
        tdSql.query(sql)
        tdSql.checkRows(2)

        # System primary timeline keeps legacy behavior for duplicate timestamps.
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.meters state_window(voltage);"
        tdSql.query(sql)
        tdSql.checkRows(48)
        sql = f"select _wstart, _wend, _wduration, SAMPLE(current, 1) from keeporderdb.meters count_window(3);"
        tdSql.query(sql)
        tdSql.checkRows(2074)
        self.check_long_result_sqls()
