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
import time


class TestIntervalMore:

    def setup_class(cls):
        tdLog.info("insert interval test data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "interval.json")
        etool.benchMark(json=json)

    def create_streams(self):
        tdSql.execute("use test;")
        streams = [
            "create stream stream1 fill_history 1 into sta as select _wstart, _wend, _wduration, count(*) from test.st where ts < '2020-10-01 00:07:19' interval(1m, auto);",
            "create stream stream2 fill_history 1 into stb as select _wstart, _wend, _wduration, count(*) from test.st where ts = '2020-11-01 23:45:00' interval(1h, auto) sliding(27m);",
            "create stream stream3 fill_history 1 into stc as select _wstart, _wend, _wduration, count(*) from test.st where ts in ('2020-11-12 23:32:00') interval(1n, auto) sliding(13d);",
            "create stream stream4 fill_history 1 into std as select _wstart, _wend, _wduration, count(*) from test.st where ts in ('2020-10-09 01:23:00', '2020-11-09 01:23:00', '2020-12-09 01:23:00') interval(1s, auto);",
            "create stream stream5 fill_history 1 into ste as select _wstart, _wend, _wduration, count(*) from test.st where ts > '2020-12-09 01:23:00' interval(1d, auto) sliding(17h);",
            "create stream stream6 fill_history 1 into stf as select _wstart, _wend, _wduration, count(*) from test.st where ts >= '2020-10-09 01:23:00' interval(1n, auto);",
            "create stream stream7 fill_history 1 into stg as select _wstart, _wend, _wduration, count(*) from test.st where ts >= '2020-11-09 01:23:00' interval(1n, auto) sliding(13d);",
        ]
        for sql in streams:
            tdSql.execute(sql)
        for i in range(50):
            rows = tdSql.query(
                "select * from information_schema.ins_stream_tasks where history_task_status is not null;"
            )
            if rows == 0:
                break
            tdLog.info(f"i={i} wait for history data calculation finish ...")
            time.sleep(1)

    def test_auto_offset_empty_ts_range(self):
        """Interval auto offset: an empty ts range must not hang the client

        Regression for a client-side infinite loop. When the WHERE clause
        collapses the query time range to empty (skey=INT64_MAX,
        ekey=INT64_MIN, i.e. TSWINDOW_DESC_INITIALIZER) and the interval uses
        AUTO offset with a sub-day unit (computed on the client),
        calcIntervalAutoOffset used to spin forever calling
        taosTimeAdd(INT64_MAX, ...), flooding the log with "time overflow".
        The query must instead translate normally and return an empty result.

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.3.0.0

        Labels: interval

        History:
            - 2026-07-09 Tony Zhang Created
        """
        tdLog.info("test auto-offset with an empty ts range (must not hang).")
        tdSql.execute("drop database if exists auto_off")
        tdSql.execute("create database auto_off keep 3650")
        tdSql.execute("use auto_off")
        tdSql.execute(
            "create stable stb (ts timestamp, col int) tags (t1 int)"
        )
        tdSql.execute("create table ct1 using stb tags (1)")
        tdSql.execute("insert into ct1 values ('2026-07-08 08:00:00', 1)")
        tdSql.execute("insert into ct1 values ('2026-07-08 12:00:00', 2)")

        # Empty/contradictory ts range (upper bound earlier than lower bound)
        # plus a sub-day-unit interval and auto offset. Before the fix each of
        # these hung the client in an infinite loop; they must now return an
        # empty result set.
        empty_range_sqls = [
            (
                "select _wstart, avg(col) from stb "
                "where ts > '2026-07-09 06:00:00' "
                "and ts < '2026-07-08 06:00:00' interval(28800s, auto)"
            ),
            (
                "select _wstart, avg(col) from ct1 "
                "where ts > '2026-07-09 06:00:00' "
                "and ts < '2026-07-08 06:00:00' "
                "interval(28800s, auto) sliding(3600s)"
            ),
            (
                "select _wstart, count(*) from stb "
                "where ts >= 1678901803783 and ts <= 1678901803782 "
                "interval(8h, auto)"
            ),
        ]
        for sql in empty_range_sqls:
            tdSql.query(sql)
            tdSql.checkRows(0)

        # Sanity: a normal (non-empty) range with auto offset still computes
        # aligned windows and returns rows.
        rows = tdSql.query(
            "select _wstart, avg(col) from ct1 "
            "where ts >= '2026-07-08 06:00:00' "
            "and ts < '2026-07-09 06:00:00' interval(28800s, auto)"
        )
        assert rows > 0, "auto-offset over non-empty range must return windows"

        tdSql.execute("drop database if exists auto_off")

    def test_query_interval(self):
        """Interval: auto

        1. Testing the mixed use of interval and auto
        2. Combined with LIMIT, ts filtering conditions
        3. Combined with sliding

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.3.0.0

        Labels: interval,integration,functional
        History:
            - 2024-9-14 Jinqing Kuang Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        tdLog.info("test normal query.")
        # self.create_streams()
        # read sql from .sql file and execute
        tdLog.info("test normal query.")
        self.sqlFile = etool.curFile(__file__, f"in/interval.in")
        self.ansFile = etool.curFile(__file__, f"ans/interval.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "interval")
