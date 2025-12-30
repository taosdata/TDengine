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

from new_test_framework.utils import tdLog, tdSql, etool
import os
import time

class TestStreamVtable:
    updatecfgDict = {
        "checkpointInterval": 60,
    }

    def create_tables(self):
        tdLog.info("create tables")

        tdSql.execute("drop database if exists test_stream_vtable;")
        tdSql.execute("create database test_stream_vtable vgroups 8;")
        tdSql.execute("use test_stream_vtable;")

        tdLog.info(f"create org super table.")
        tdSql.execute("select database();")
        tdSql.execute(f"CREATE STABLE `vtb_org_stb` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned, "
                      "u_smallint_col smallint unsigned, "
                      "u_int_col int unsigned, "
                      "u_bigint_col bigint unsigned, "
                      "tinyint_col tinyint, "
                      "smallint_col smallint, "
                      "int_col int, "
                      "bigint_col bigint, "
                      "float_col float, "
                      "double_col double, "
                      "bool_col bool, "
                      "binary_16_col binary(16),"
                      "binary_32_col binary(32),"
                      "nchar_16_col nchar(16),"
                      "nchar_32_col nchar(32)"
                      ") TAGS ("
                      "int_tag int,"
                      "bool_tag bool,"
                      "float_tag float,"
                      "double_tag double)")

        tdLog.info(f"create org child table.")
        for i in range(3):
            tdSql.execute(f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` TAGS ({i}, false, {i}, {i});")

        tdLog.info(f"create virtual normal table.")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_full` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from vtb_org_child_0.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from vtb_org_child_1.u_smallint_col, "
                      "u_int_col int unsigned from vtb_org_child_2.u_int_col, "
                      "u_bigint_col bigint unsigned from vtb_org_child_0.u_bigint_col, "
                      "tinyint_col tinyint from vtb_org_child_1.tinyint_col, "
                      "smallint_col smallint from vtb_org_child_2.smallint_col, "
                      "int_col int from vtb_org_child_0.int_col, "
                      "bigint_col bigint from vtb_org_child_1.bigint_col, "
                      "float_col float from vtb_org_child_2.float_col, "
                      "double_col double from vtb_org_child_0.double_col, "
                      "bool_col bool from vtb_org_child_1.bool_col, "
                      "binary_16_col binary(16) from vtb_org_child_2.binary_16_col,"
                      "binary_32_col binary(32) from vtb_org_child_0.binary_32_col,"
                      "nchar_16_col nchar(16) from vtb_org_child_1.nchar_16_col,"
                      "nchar_32_col nchar(32) from vtb_org_child_2.nchar_32_col)")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_half_full` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from vtb_org_child_0.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from vtb_org_child_1.u_smallint_col, "
                      "u_int_col int unsigned from vtb_org_child_2.u_int_col, "
                      "u_bigint_col bigint unsigned, "
                      "tinyint_col tinyint, "
                      "smallint_col smallint, "
                      "int_col int from vtb_org_child_0.int_col, "
                      "bigint_col bigint from vtb_org_child_1.bigint_col, "
                      "float_col float from vtb_org_child_2.float_col, "
                      "double_col double, "
                      "bool_col bool, "
                      "binary_16_col binary(16),"
                      "binary_32_col binary(32) from vtb_org_child_0.binary_32_col,"
                      "nchar_16_col nchar(16) from vtb_org_child_1.nchar_16_col,"
                      "nchar_32_col nchar(32) from vtb_org_child_2.nchar_32_col)")

        tdSql.execute(f"CREATE STABLE `vtb_virtual_stb` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned, "
                      "u_smallint_col smallint unsigned, "
                      "u_int_col int unsigned, "
                      "u_bigint_col bigint unsigned, "
                      "tinyint_col tinyint, "
                      "smallint_col smallint, "
                      "int_col int, "
                      "bigint_col bigint, "
                      "float_col float, "
                      "double_col double, "
                      "bool_col bool, "
                      "binary_16_col binary(16),"
                      "binary_32_col binary(32),"
                      "nchar_16_col nchar(16),"
                      "nchar_32_col nchar(32)"
                      ") TAGS ("
                      "int_tag int,"
                      "bool_tag bool,"
                      "float_tag float,"
                      "double_tag double)"
                      "VIRTUAL 1")

        tdLog.info(f"create virtual child table.")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_full` ("
                      "u_tinyint_col from vtb_org_child_0.u_tinyint_col, "
                      "u_smallint_col from vtb_org_child_1.u_smallint_col, "
                      "u_int_col from vtb_org_child_2.u_int_col, "
                      "u_bigint_col from vtb_org_child_0.u_bigint_col, "
                      "tinyint_col from vtb_org_child_1.tinyint_col, "
                      "smallint_col from vtb_org_child_2.smallint_col, "
                      "int_col from vtb_org_child_0.int_col, "
                      "bigint_col from vtb_org_child_1.bigint_col, "
                      "float_col from vtb_org_child_2.float_col, "
                      "double_col from vtb_org_child_0.double_col, "
                      "bool_col from vtb_org_child_1.bool_col, "
                      "binary_16_col from vtb_org_child_2.binary_16_col,"
                      "binary_32_col from vtb_org_child_0.binary_32_col,"
                      "nchar_16_col from vtb_org_child_1.nchar_16_col,"
                      "nchar_32_col from vtb_org_child_2.nchar_32_col)"
                      "USING `vtb_virtual_stb` TAGS (0, false, 0, 0)")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_half_full` ("
                      "u_tinyint_col from vtb_org_child_0.u_tinyint_col, "
                      "u_smallint_col from vtb_org_child_1.u_smallint_col, "
                      "u_int_col from vtb_org_child_2.u_int_col, "
                      "int_col from vtb_org_child_0.int_col, "
                      "bigint_col from vtb_org_child_1.bigint_col, "
                      "float_col from vtb_org_child_2.float_col, "
                      "binary_32_col from vtb_org_child_0.binary_32_col,"
                      "nchar_16_col from vtb_org_child_1.nchar_16_col,"
                      "nchar_32_col from vtb_org_child_2.nchar_32_col)"
                      "USING `vtb_virtual_stb` TAGS (1, false, 1, 1)")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_empty` "
                      "USING `vtb_virtual_stb` TAGS (2, false, 2, 2)")

    def create_proj_streams(self):
        tdSql.execute(f"CREATE STREAM s_proj_1 TRIGGER AT_ONCE INTO dst_proj_1 AS "
                        "select * from test_stream_vtable.vtb_virtual_ntb_full;")
        tdSql.execute(f"CREATE STREAM s_proj_2 TRIGGER AT_ONCE INTO dst_proj_2 AS "
                        "select * from test_stream_vtable.vtb_virtual_ntb_half_full;")
        tdSql.execute(f"CREATE STREAM s_proj_3 TRIGGER AT_ONCE INTO dst_proj_3 AS "
                        "select * from test_stream_vtable.vtb_virtual_stb PARTITION BY tbname;")
        tdSql.execute(f"CREATE STREAM s_proj_4 TRIGGER AT_ONCE INTO dst_proj_4 AS "
                        "select * from test_stream_vtable.vtb_virtual_ctb_full;")
        tdSql.execute(f"CREATE STREAM s_proj_5 TRIGGER AT_ONCE INTO dst_proj_5 AS "
                        "select * from test_stream_vtable.vtb_virtual_ctb_half_full;")

        tdSql.execute(f"CREATE STREAM s_proj_6 TRIGGER AT_ONCE INTO dst_proj_6 AS "
                        "select * from test_stream_vtable.vtb_virtual_ntb_full WHERE u_tinyint_col = 1;")
        tdSql.execute(f"CREATE STREAM s_proj_7 TRIGGER AT_ONCE INTO dst_proj_7 AS "
                        "select * from test_stream_vtable.vtb_virtual_ntb_half_full WHERE bool_col = true;")
        tdSql.execute(f"CREATE STREAM s_proj_8 TRIGGER AT_ONCE INTO dst_proj_8 AS "
                        "select * from test_stream_vtable.vtb_virtual_stb WHERE bool_col = true PARTITION BY tbname;")
        tdSql.execute(f"CREATE STREAM s_proj_9 TRIGGER AT_ONCE INTO dst_proj_9 AS "
                        "select * from test_stream_vtable.vtb_virtual_ctb_full WHERE u_tinyint_col = 1;")
        tdSql.execute(f"CREATE STREAM s_proj_10 TRIGGER AT_ONCE INTO dst_proj_10 AS "
                        "select * from test_stream_vtable.vtb_virtual_ctb_half_full WHERE bool_col = true;")

        tdSql.execute(f"CREATE STREAM s_proj_11 TRIGGER AT_ONCE INTO dst_proj_11 AS "
                        "select ts, cos(u_tinyint_col), u_smallint_col, u_int_col, u_bigint_col from test_stream_vtable.vtb_virtual_ntb_full;")
        tdSql.execute(f"CREATE STREAM s_proj_12 TRIGGER AT_ONCE INTO dst_proj_12 AS "
                        "select ts, cos(u_tinyint_col), u_smallint_col, u_int_col, u_bigint_col from test_stream_vtable.vtb_virtual_ntb_half_full;")
        tdSql.execute(f"CREATE STREAM s_proj_13 TRIGGER AT_ONCE INTO dst_proj_13 AS "
                        "select ts, cos(u_tinyint_col), u_smallint_col, u_int_col, u_bigint_col from test_stream_vtable.vtb_virtual_stb PARTITION BY tbname;")
        tdSql.execute(f"CREATE STREAM s_proj_14 TRIGGER AT_ONCE INTO dst_proj_14 AS "
                        "select ts, cos(u_tinyint_col), u_smallint_col, u_int_col, u_bigint_col from test_stream_vtable.vtb_virtual_ctb_full;")
        tdSql.execute(f"CREATE STREAM s_proj_15 TRIGGER AT_ONCE INTO dst_proj_15 AS "
                        "select ts, cos(u_tinyint_col), u_smallint_col, u_int_col, u_bigint_col from test_stream_vtable.vtb_virtual_ctb_half_full;")

    def create_window_streams(self):
        tdSql.execute(f"CREATE STREAM s_interval_1 INTO dst_interval_1 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full interval(1s);")
        tdSql.execute(f"CREATE STREAM s_interval_2 INTO dst_interval_2 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_half_full interval(1s) sliding(100a);")
        tdSql.execute(f"CREATE STREAM s_interval_3 INTO dst_interval_3 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_stb partition by tbname interval(1s) sliding(200a);")
        tdSql.execute(f"CREATE STREAM s_interval_4 INTO dst_interval_4 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_full interval(1s) sliding(100a);")
        tdSql.execute(f"CREATE STREAM s_interval_5 INTO dst_interval_5 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_half_full interval(1s);")

        tdSql.execute(f"CREATE STREAM s_state_1 INTO dst_state_1 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full state_window(bool_col);")
        tdSql.execute(f"CREATE STREAM s_state_2 INTO dst_state_2 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_half_full state_window(bool_col);")
        tdSql.execute(f"CREATE STREAM s_state_3 INTO dst_state_3 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_stb partition by tbname state_window(bool_col);")
        tdSql.execute(f"CREATE STREAM s_state_4 INTO dst_state_4 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_full state_window(bool_col);")
        tdSql.execute(f"CREATE STREAM s_state_5 INTO dst_state_5 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_half_full state_window(bool_col);")

        tdSql.execute(f"CREATE STREAM s_session_1 INTO dst_session_1 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full session(ts, 10a);")
        tdSql.execute(f"CREATE STREAM s_session_2 INTO dst_session_2 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_half_full session(ts, 10a);")
        tdSql.execute(f"CREATE STREAM s_session_3 INTO dst_session_3 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_stb partition by tbname session(ts, 10a);")
        tdSql.execute(f"CREATE STREAM s_session_4 INTO dst_session_4 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_full session(ts, 10a);")
        tdSql.execute(f"CREATE STREAM s_session_5 INTO dst_session_5 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_half_full session(ts, 10a);")

        tdSql.execute(f"CREATE STREAM s_event_1 INTO dst_event_1 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full event_window start with u_tinyint_col > 50 end with u_smallint_col > 10000;")
        tdSql.execute(f"CREATE STREAM s_event_2 INTO dst_event_2 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_half_full event_window start with u_tinyint_col > 50 end with u_smallint_col > 10000;")
        tdSql.execute(f"CREATE STREAM s_event_3 INTO dst_event_3 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_stb partition by tbname event_window start with u_tinyint_col > 50 end with u_smallint_col > 10000;")
        tdSql.execute(f"CREATE STREAM s_event_4 INTO dst_event_4 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_full event_window start with u_tinyint_col > 50 end with u_smallint_col > 10000;")
        tdSql.execute(f"CREATE STREAM s_event_5 INTO dst_event_5 AS "
                        "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_half_full event_window start with u_tinyint_col > 50 end with u_smallint_col > 10000;")

        # tdSql.execute(f"CREATE STREAM s_count_1 INTO dst_count_1 AS "
        #                 "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full count_window(20);")
        # tdSql.execute(f"CREATE STREAM s_count_1 INTO dst_count_1 AS "
        #                 "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_half_full count_window(20);")
        # tdSql.execute(f"CREATE STREAM s_count_1 INTO dst_count_1 AS "
        #                 "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_stb partition by tbname count_window(20);")
        # tdSql.execute(f"CREATE STREAM s_count_1 INTO dst_count_1 AS "
        #                 "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_full count_window(20);")
        # tdSql.execute(f"CREATE STREAM s_count_1 INTO dst_count_1 AS "
        #                 "select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_half_full count_window(20);")

    def wait_streams_ready(self):
        for i in range(60):
            tdLog.info(f"i={i} wait for stream tasks ready ...")
            time.sleep(1)
            rows = tdSql.query("select * from information_schema.ins_stream_tasks where status <> 'ready';")
            if rows == 0:
                break

    def wait_streams_done(self):
        # The entire test runs for a while. Wait briefly, and if no exceptions occur, it's sufficient.
        for i in range(30):
            tdLog.info(f"i={i} wait for stream tasks done ...")
            time.sleep(1)
            rows = tdSql.query("select * from information_schema.ins_stream_tasks where status <> 'ready';")
            if rows != 0:
                raise Exception("stream task status is wrong, please check it!")


    def test_stream_vtable(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        tdLog.debug(f"start to excute {__file__}")

        self.create_tables()
        self.create_proj_streams()
        self.wait_streams_ready()
        json = etool.curFile(__file__, "vtable_insert.json")
        etool.benchMark(json=json)
        self.wait_streams_done()

        tdLog.success(f"{__file__} successfully executed")


