###################################################################
#           Copyright (c) 2025 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import os
from new_test_framework.utils import tdSql, tdLog, tdCom, tdStream
from new_test_framework.utils import etool

class TestStateWindowNullBlock:
    # init
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.info(f"start to excute {__file__}")
    
    def test_state_window_null_block(self):
        """summary: test state window with null-state data block

        description:
            1. test state_window with all non-null data block
            2. test state_window with nulls in the middle of data block
            3. test state_window with nulls at the border of middle data block
            4. test state_window with nulls at the border of all data block
            5. test state_window with mixed null and non-null data block
            6. test state_window with all null data block, and nulls at border
            7. test state_window with partition and data block containing nulls

        Since: v3.3.8.5

        Labels: state window, null block

        Jira: TS-7129

        Catalog:
            - TimeSeriesExt:StateWindow

        History:
            - 2025-11-03 Tony Zhang: Created this test
            - 2025-12-01 Tony Zhang: add check_partition_and_null

        """
        self.prepare_data()
        self.check_end_with_null_block()
        self.check_all_non_null()
        self.check_inner_null()
        self.check_border_null1()
        self.check_border_null1()
        self.check_all_null_block1()
        self.check_all_null_block2()
        self.check_partition_and_null()
        self.check_multi_col_null_block()

    def prepare_data(self):
        tdSql.execute("drop database if exists test_state_window_null_block")
        tdSql.execute("create database test_state_window_null_block")
        tdSql.execute("use test_state_window_null_block")

        tdSql.execute("create table t1 (ts timestamp, s bool, v int)")
        tdSql.execute("create table t2 (ts timestamp, s bool, v int)")
        tdSql.execute("create table t3 (ts timestamp, s bool, v int)")
        tdSql.execute("create table t4 (ts timestamp, s bool, v int)")
        tdSql.execute("create table t5 (ts timestamp, s bool, v int)")
        tdSql.execute("create table t6 (ts timestamp, s bool, v int)")

        datafile = etool.getFilePath(__file__, "data", "data1.csv")
        tdSql.execute(f"insert into t1 file '{datafile}'")
        datafile = etool.getFilePath(__file__, "data", "data2.csv")
        tdSql.execute(f"insert into t2 file '{datafile}'")
        datafile = etool.getFilePath(__file__, "data", "data3.csv")
        tdSql.execute(f"insert into t3 file '{datafile}'")
        datafile = etool.getFilePath(__file__, "data", "data4.csv")
        tdSql.execute(f"insert into t4 file '{datafile}'")
        datafile = etool.getFilePath(__file__, "data", "data5.csv")
        tdSql.execute(f"insert into t5 file '{datafile}'")
        datafile = etool.getFilePath(__file__, "data", "data6.csv")
        tdSql.execute(f"insert into t6 file '{datafile}'")

        # prepare with taosBenchmark
        # insert 5k rows with all nulls
        json_file = os.path.join(os.path.dirname(__file__), "json/all_null_5k.json")
        etool.benchMark(json=json_file)
        # insert to create a window
        tdSql.execute("use test_end_null")
        tdSql.execute("insert into d0 values('2025-10-01 01:00:00', 1);")

    # test data pattern:
    # | true ... true | true ... false | true ... true |
    def check_all_non_null(self):
        tdSql.execute("use test_state_window_null_block")
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t1 state_window(s)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:40:00.000")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.000")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:20.000")
        tdSql.checkData(4, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)
        tdSql.checkData(4, 4, 2000)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t1 state_window(s) extend(1)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.999")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.999")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.999")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:40:00.000")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.999")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:20.000")
        tdSql.checkData(4, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)
        tdSql.checkData(4, 4, 2000)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t1 state_window(s) extend(2)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:19.001")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:39.001")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:39:59.001")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.000")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:19.001")
        tdSql.checkData(4, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)
        tdSql.checkData(4, 4, 2000)

    # test data pattern:
    # | true ... null ... true | true ... null ... false | true ... null ... true |
    def check_inner_null(self):
        tdSql.execute("use test_state_window_null_block")
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t2 state_window(s)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:40:00.000")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.000")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:20.000")
        tdSql.checkData(4, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)
        tdSql.checkData(4, 4, 2000)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t2 state_window(s) extend(1)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.999")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.999")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.999")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:40:00.000")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.999")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:20.000")
        tdSql.checkData(4, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)
        tdSql.checkData(4, 4, 2000)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t2 state_window(s) extend(2)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:19.001")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:39.001")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:39:59.001")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.000")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:19.001")
        tdSql.checkData(4, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)
        tdSql.checkData(4, 4, 2000)

    # test data pattern:
    # | true ... null ... false | null ... true ... null | true ... null ... true |
    def check_border_null1(self):
        tdSql.execute("use test_state_window_null_block")
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t3 state_window(s)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:40:00.000")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.000")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:20.000")
        tdSql.checkData(4, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)
        tdSql.checkData(4, 4, 2000)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t3 state_window(s) extend(1)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.999")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.999")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.999")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:40:00.000")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.999")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:20.000")
        tdSql.checkData(4, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)
        tdSql.checkData(4, 4, 2000)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t3 state_window(s) extend(2)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:19.001")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:39.001")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:39:59.001")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.000")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:19.001")
        tdSql.checkData(4, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)
        tdSql.checkData(4, 4, 2000)

    # test data pattern:
    # | null ... true ... null | null ... true ... null | null ... false ... null |
    def check_border_null2(self):
        tdSql.execute("use test_state_window_null_block")
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t4 state_window(s)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:04.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 1996)
        tdSql.checkData(0, 4, 1996)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:40:00.000")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.000")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:20.000")
        tdSql.checkData(4, 1, "2025-10-31 02:46:34.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 1995)
        tdSql.checkData(4, 4, 1995)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t4 state_window(s) extend(1)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:04.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.999")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 1996)
        tdSql.checkData(0, 4, 1996)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.999")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.999")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:40:00.000")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.999")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:20.000")
        tdSql.checkData(4, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)
        tdSql.checkData(4, 4, 2000)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t4 state_window(s) extend(2)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:19.001")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:39.001")
        tdSql.checkData(2, 1, "2025-10-31 01:39:59.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(2, 4, 2000)
        tdSql.checkData(3, 0, "2025-10-31 01:39:59.001")
        tdSql.checkData(3, 1, "2025-10-31 02:13:19.000")
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(4, 0, "2025-10-31 02:13:19.001")
        tdSql.checkData(4, 1, "2025-10-31 02:46:34.000")
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 1995)
        tdSql.checkData(4, 4, 1995)

    # test data pattern:
    # | false ... null ... true | all null | true ... null ... true |
    def check_all_null_block1(self):
        tdSql.execute("use test_state_window_null_block")
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t5 state_window(s)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 6000)
        tdSql.checkData(2, 4, 4000)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t5 state_window(s) extend(1)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.999")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.999")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 6000)
        tdSql.checkData(2, 4, 4000)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t5 state_window(s) extend(2)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:19.001")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:39.001")
        tdSql.checkData(2, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 6000)
        tdSql.checkData(2, 4, 4000)

    # test data pattern:
    # | null ... false ... null | all null | null ... false ... null |
    def check_all_null_block2(self):
        tdSql.execute("use test_state_window_null_block")
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t6 state_window(s)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-31 00:00:05.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 1995)
        tdSql.checkData(0, 4, 1995)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 02:46:34.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 5995)
        tdSql.checkData(2, 4, 3995)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t6 state_window(s) extend(1)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-31 00:00:05.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.999")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 1995)
        tdSql.checkData(0, 4, 1995)
        tdSql.checkData(1, 0, "2025-10-31 00:33:20.000")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.999")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:40.000")
        tdSql.checkData(2, 1, "2025-10-31 02:46:39.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 6000)
        tdSql.checkData(2, 4, 4000)

        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t6 state_window(s) extend(2)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-31 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-31 00:33:19.000")
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(0, 4, 2000)
        tdSql.checkData(1, 0, "2025-10-31 00:33:19.001")
        tdSql.checkData(1, 1, "2025-10-31 01:06:39.000")
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 0, "2025-10-31 01:06:39.001")
        tdSql.checkData(2, 1, "2025-10-31 02:46:34.000")
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 5995)
        tdSql.checkData(2, 4, 3995)

    def check_end_with_null_block(self):
        tdSql.execute("use test_end_null")
        tdSql.query("select _wstart, _wend, count(*), sum(v) from d0 state_window(v)", show=True)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2025-10-01 01:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 01:00:00.000")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.query("select _wstart, _wend, count(*), sum(v) from d0 state_window(v) extend(1)", show=True)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2025-10-01 01:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 01:23:19.000")
        tdSql.checkData(0, 2, 1400)
        tdSql.checkData(0, 3, 1)
        tdSql.query("select _wstart, _wend, count(*), sum(v) from d0 state_window(v) extend(2)", show=True)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2025-10-01 00:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 01:00:00.000")
        tdSql.checkData(0, 2, 3601)
        tdSql.checkData(0, 3, 1)

    def check_partition_and_null(self):
        tdLog.info(">>>>>>>>>>>>> check_partition_and_null >>>>>>>>>>>>>>>")
        tdSql.execute("create database if not exists test_state_window_null_block_partition", show=True)
        tdSql.execute("use test_state_window_null_block_partition")
        tdSql.execute("drop table if exists tt")
        tdSql.execute("create table tt (ts timestamp, v int, s varchar(10))", show=True)
        tdSql.execute('''
            insert into tt values 
              ("2025-12-01 11:59:57", 1, null)
              ("2025-12-01 11:59:58", 2, null)
              ("2025-12-01 11:59:59", 3, null)
              ("2025-12-01 12:00:00", 1, 'a')
              ("2025-12-01 12:00:01", 2, 'b')
              ("2025-12-01 12:00:02", 3, 'a')
              ("2025-12-01 12:00:03", 1, 'b')
              ("2025-12-01 12:00:04", 2, 'a')
              ("2025-12-01 12:00:05", 3, 'a')
              ("2025-12-01 12:00:06", 1, null)
              ("2025-12-01 12:00:07", 2, null)
        ''', show=True)

        tdSql.query("select _wstart, _wend, count(*), v, s from tt partition by v state_window(s) order by _wstart", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-12-01 12:00:00.000")
        tdSql.checkData(0, 1, "2025-12-01 12:00:00.000")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, "a")
        tdSql.checkData(1, 0, "2025-12-01 12:00:01.000")
        tdSql.checkData(1, 1, "2025-12-01 12:00:01.000")
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, "b")
        tdSql.checkData(2, 0, "2025-12-01 12:00:02.000")
        tdSql.checkData(2, 1, "2025-12-01 12:00:05.000")
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 3)
        tdSql.checkData(2, 4, "a")
        tdSql.checkData(3, 0, "2025-12-01 12:00:03.000")
        tdSql.checkData(3, 1, "2025-12-01 12:00:03.000")
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(3, 3, 1)
        tdSql.checkData(3, 4, "b")
        tdSql.checkData(4, 0, "2025-12-01 12:00:04.000")
        tdSql.checkData(4, 1, "2025-12-01 12:00:04.000")
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(4, 3, 2)
        tdSql.checkData(4, 4, "a")

        tdSql.query("select _wstart, _wend, count(*), v, s from tt partition by v state_window(s) extend(1) order by _wstart", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-12-01 12:00:00.000")
        tdSql.checkData(0, 1, "2025-12-01 12:00:02.999")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, "a")
        tdSql.checkData(1, 0, "2025-12-01 12:00:01.000")
        tdSql.checkData(1, 1, "2025-12-01 12:00:03.999")
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, "b")
        tdSql.checkData(2, 0, "2025-12-01 12:00:02.000")
        tdSql.checkData(2, 1, "2025-12-01 12:00:05.000")
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 3)
        tdSql.checkData(2, 4, "a")
        tdSql.checkData(3, 0, "2025-12-01 12:00:03.000")
        tdSql.checkData(3, 1, "2025-12-01 12:00:06.000")
        tdSql.checkData(3, 2, 2)
        tdSql.checkData(3, 3, 1)
        tdSql.checkData(3, 4, "b")
        tdSql.checkData(4, 0, "2025-12-01 12:00:04.000")
        tdSql.checkData(4, 1, "2025-12-01 12:00:07.000")
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(4, 3, 2)
        tdSql.checkData(4, 4, "a")

        tdSql.query("select _wstart, _wend, count(*), v, s from tt partition by v state_window(s) extend(2) order by _wstart", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-12-01 11:59:57.000")
        tdSql.checkData(0, 1, "2025-12-01 12:00:00.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, "a")
        tdSql.checkData(1, 0, "2025-12-01 11:59:58.000")
        tdSql.checkData(1, 1, "2025-12-01 12:00:01.000")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, "b")
        tdSql.checkData(2, 0, "2025-12-01 11:59:59.000")
        tdSql.checkData(2, 1, "2025-12-01 12:00:05.000")
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 3)
        tdSql.checkData(2, 4, "a")
        tdSql.checkData(3, 0, "2025-12-01 12:00:00.001")
        tdSql.checkData(3, 1, "2025-12-01 12:00:03.000")
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(3, 3, 1)
        tdSql.checkData(3, 4, "b")
        tdSql.checkData(4, 0, "2025-12-01 12:00:01.001")
        tdSql.checkData(4, 1, "2025-12-01 12:00:04.000")
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(4, 3, 2)
        tdSql.checkData(4, 4, "a")

    def _insert_block(self, table, base_ms, row_start, count, s1, s2):
        """Insert a contiguous block of rows into table (ts timestamp, s1 int, s2 int).

        Args:
            table:     target table name
            base_ms:   base timestamp in milliseconds (row 0's ts)
            row_start: starting row offset (ts = base_ms + (row_start + i) * 1000)
            count:     number of rows to insert
            s1:        value for s1 column, None inserts SQL NULL
            s2:        value for s2 column, None inserts SQL NULL
        """
        values = []
        for i in range(count):
            ts = base_ms + (row_start + i) * 1000
            v1 = 'null' if s1 is None else str(s1)
            v2 = 'null' if s2 is None else str(s2)
            values.append(f"({ts}, {v1}, {v2})")
        tdSql.execute(f"insert into {table} values {' '.join(values)}")

    def check_multi_col_null_block(self):
        tdLog.info(">>>>> check_multi_col_null_block >>>>>")

        # --- Part 1: reuse existing tables t1/t5/t6 with state_window(s, v) ---
        # In data CSVs, non-NULL rows have s and v correlated (true=1, false=0),
        # and NULL rows have s=null but v non-null. So state_window(s, v) should
        # produce same results as state_window(s) because any row with s=null is
        # skipped by stateWindowRowHasNull, and non-null rows have (s,v) 1:1 mapped.
        tdSql.execute("use test_state_window_null_block")

        # t1: all non-null, 5 windows expected (same as single-col)
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t1 state_window(s, v)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)

        # t2: inner nulls, s and v correlated for non-null rows, 5 windows
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t2 state_window(s, v)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)

        # t3: border nulls between blocks, 5 windows
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t3 state_window(s, v)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)

        # t4: nulls at head/tail borders
        # With "undefined -> defined does not cut", leading (null,1) rows
        # are absorbed into the first (true,1) window; trailing partial-NULL
        # rows are absorbed into the last (true,1) window.
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t4 state_window(s, v)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)

        # t5: large null blocks (rows 6001-10000 are s=null, v changes 0->1)
        # Under current semantics:
        # - rows 6001-8000 form (None,0) with count=2000
        # - rows 8001-10000 form one window with count=2000 (s initialized to True later)
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t5 state_window(s, v)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)

        # t6: border nulls
        # With current semantics, leading partial-NULL rows are absorbed by
        # the first non-NULL state window; middle null block behaves like t5.
        tdSql.query("select _wstart, _wend, s, count(*), sum(v) from t6 state_window(s, v)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, 2000)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, 2000)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(2, 3, 2000)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(3, 3, 2000)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(4, 3, 2000)

        print("Part 1: reuse t1/t5/t6 with multi-col .... [passed]")

        # --- Part 2: independent columns with large null blocks ---
        # 9 blocks of 500 rows, s1 and s2 change independently
        tdSql.execute("drop database if exists test_mc_null_block")
        tdSql.execute("create database test_mc_null_block")
        tdSql.execute("use test_mc_null_block")
        tdSql.execute("create table mc1 (ts timestamp, s1 int, s2 int)")

        base_ms = 1730332800000  # 2024-10-31 00:00:00.000
        self._insert_block("mc1", base_ms, 0,    500, 1,    10)    # Block 1: (1,10)
        self._insert_block("mc1", base_ms, 500,  500, None, 10)    # Block 2: s1=NULL
        self._insert_block("mc1", base_ms, 1000, 500, 1,    10)    # Block 3: (1,10)
        self._insert_block("mc1", base_ms, 1500, 500, 1,    20)    # Block 4: s2 changes
        self._insert_block("mc1", base_ms, 2000, 500, None, None)  # Block 5: both NULL
        self._insert_block("mc1", base_ms, 2500, 500, 2,    20)    # Block 6: s1 changes
        self._insert_block("mc1", base_ms, 3000, 500, 2,    None)  # Block 7: s2=NULL
        self._insert_block("mc1", base_ms, 3500, 500, 2,    20)    # Block 8: (2,20)
        self._insert_block("mc1", base_ms, 4000, 500, 1,    10)    # Block 9: both change

        # state_window(s1, s2): 4 windows
        # W1: blocks 1+2+3, (1,10), null rows in block 2 are absorbed (same state wraps)
        # W2: block 4, (1,20)
        # W3: blocks 6+7+8, (2,20), null rows in block 7 absorbed
        # W4: block 9, (1,10)
        tdSql.query("select _wstart, _wend, count(*), s1, s2 from mc1 state_window(s1, s2)", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 1500)  # (1,10) blocks 1+2+3
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 10)
        tdSql.checkData(1, 2, 500)   # (1,20) block 4
        tdSql.checkData(1, 3, 1)
        tdSql.checkData(1, 4, 20)
        tdSql.checkData(2, 2, 1500)  # (2,20) blocks 6+7+8
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, 20)
        tdSql.checkData(3, 2, 500)   # (1,10) block 9
        tdSql.checkData(3, 3, 1)
        tdSql.checkData(3, 4, 10)

        # cross-validate: state_window(s1) → 3 windows
        # W1: blocks 1+2+3+4 (s1=1,null,1,1), null block 2 absorbed, count=2000
        # W2: blocks 6+7+8 (s1=2,2,2), block 7 s1=2 non-null, count=1500
        # W3: block 9, count=500
        tdSql.query("select _wstart, _wend, count(*), s1 from mc1 state_window(s1)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 2000)  # s1=1
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 2, 1500)  # s1=2
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(2, 2, 500)   # s1=1
        tdSql.checkData(2, 3, 1)

        # cross-validate: state_window(s2) → 3 windows
        # W1: blocks 1+2+3 (s2=10,10,10), count=1500
        # W2: blocks 4+5+6+7+8 (s2=20,null,20,null,20), nulls absorbed, count=2500
        # W3: block 9 (s2=10), count=500
        tdSql.query("select _wstart, _wend, count(*), s2 from mc1 state_window(s2)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 1500)  # s2=10
        tdSql.checkData(0, 3, 10)
        tdSql.checkData(1, 2, 2500)  # s2=20
        tdSql.checkData(1, 3, 20)
        tdSql.checkData(2, 2, 500)   # s2=10
        tdSql.checkData(2, 3, 10)

        print("Part 2: independent columns mc1 .......... [passed]")

        # --- Part 3: multi-col + EXTEND on mc1 ---
        # EXTEND(1): tail extends to next window start - 1ms, null rows absorbed into count
        tdSql.query("select _wstart, _wend, count(*), s1, s2 from mc1 state_window(s1, s2) extend(1)", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 1500)
        tdSql.checkData(1, 2, 1000)
        tdSql.checkData(2, 2, 1500)
        tdSql.checkData(3, 2, 500)

        # EXTEND(2): head also extends backward, absorbing null blocks
        tdSql.query("select _wstart, _wend, count(*), s1, s2 from mc1 state_window(s1, s2) extend(2)", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 1500)
        tdSql.checkData(1, 2, 500)
        tdSql.checkData(2, 2, 2000)
        tdSql.checkData(3, 2, 500)

        print("Part 3: multi-col + extend ............... [passed]")