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
import os


class TestVtableQueryEliminateVirtualScan:
    updatecfgDict = {
        "supportVnodes":"1000",
    }
    def setup_class(cls):
        tdLog.info(f"prepare org tables.")
        for i in range(4):
            tdSql.execute(f"drop database if exists test_vtable_select_{i};")
            tdSql.execute(f"create database test_vtable_select_{i};")
            tdSql.execute(f"use test_vtable_select_{i};")

            tdLog.info(f"prepare org super table.")
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
                          "double_tag double,"
                          "nchar_32_tag nchar(32),"
                          "binary_32_tag binary(32))")

            tdLog.info(f"prepare org child table.")
            for j in range(15):
                tdSql.execute(f"CREATE TABLE `vtb_org_child_{j}` USING `vtb_org_stb` TAGS ({j}, false, {j}, {j}, 'child{j}', 'child{j}');")

            tdLog.info(f"prepare org normal table.")
            for j in range(15):
                tdSql.execute(f"CREATE TABLE `vtb_org_normal_{j}` (ts timestamp, u_tinyint_col tinyint unsigned, u_smallint_col smallint unsigned, u_int_col int unsigned, u_bigint_col bigint unsigned, tinyint_col tinyint, smallint_col smallint, int_col int, bigint_col bigint, float_col float, double_col double, bool_col bool, binary_16_col binary(16), binary_32_col binary(32), nchar_16_col nchar(16), nchar_32_col nchar(32)) SMA(u_tinyint_col)")

            tdLog.info(f"insert data into org tables.")
            datafile = etool.getFilePath(__file__, "data", "data1.csv")
            tdSql.execute("insert into vtb_org_normal_0 file '%s';" % datafile)
            tdSql.execute("insert into vtb_org_child_0 file '%s';" % datafile)

            datafile = etool.getFilePath(__file__, "data", "data2.csv")
            tdSql.execute("insert into vtb_org_normal_0 file '%s';" % datafile)
            tdSql.execute("insert into vtb_org_child_0 file '%s';" % datafile)

            datafile = etool.getFilePath(__file__, "data", "data3.csv")
            tdSql.execute("insert into vtb_org_normal_0 file '%s';" % datafile)
            tdSql.execute("insert into vtb_org_child_0 file '%s';" % datafile)

        tdLog.info(f"prepare virtual normal table.")

        tdSql.execute("drop database if exists test_vtable_select;")
        tdSql.execute("create database test_vtable_select;")
        tdSql.execute("use test_vtable_select;")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_full` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from test_vtable_select_0.vtb_org_normal_0.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from test_vtable_select_0.vtb_org_normal_0.u_smallint_col, "
                      "u_int_col int unsigned from test_vtable_select_0.vtb_org_normal_0.u_int_col, "
                      "u_bigint_col bigint unsigned from test_vtable_select_0.vtb_org_normal_0.u_bigint_col, "
                      "tinyint_col tinyint from test_vtable_select_0.vtb_org_normal_0.tinyint_col, "
                      "smallint_col smallint from test_vtable_select_0.vtb_org_normal_0.smallint_col, "
                      "int_col int from test_vtable_select_0.vtb_org_normal_0.int_col, "
                      "bigint_col bigint from test_vtable_select_0.vtb_org_normal_0.bigint_col, "
                      "float_col float from test_vtable_select_0.vtb_org_normal_0.float_col, "
                      "double_col double from test_vtable_select_0.vtb_org_normal_0.double_col, "
                      "bool_col bool from test_vtable_select_0.vtb_org_normal_0.bool_col, "
                      "binary_16_col binary(16) from test_vtable_select_0.vtb_org_normal_0.binary_16_col,"
                      "binary_32_col binary(32) from test_vtable_select_0.vtb_org_normal_0.binary_32_col,"
                      "nchar_16_col nchar(16) from test_vtable_select_0.vtb_org_normal_0.nchar_16_col,"
                      "nchar_32_col nchar(32) from test_vtable_select_0.vtb_org_normal_0.nchar_32_col)")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_half_full` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from test_vtable_select_0.vtb_org_normal_0.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from test_vtable_select_0.vtb_org_normal_0.u_smallint_col, "
                      "u_int_col int unsigned from test_vtable_select_0.vtb_org_normal_0.u_int_col, "
                      "u_bigint_col bigint unsigned, "
                      "tinyint_col tinyint, "
                      "smallint_col smallint, "
                      "int_col int from test_vtable_select_0.vtb_org_normal_0.int_col, "
                      "bigint_col bigint from test_vtable_select_0.vtb_org_normal_0.bigint_col, "
                      "float_col float from test_vtable_select_0.vtb_org_normal_0.float_col, "
                      "double_col double, "
                      "bool_col bool, "
                      "binary_16_col binary(16),"
                      "binary_32_col binary(32) from test_vtable_select_0.vtb_org_normal_0.binary_32_col,"
                      "nchar_16_col nchar(16) from test_vtable_select_0.vtb_org_normal_0.nchar_16_col,"
                      "nchar_32_col nchar(32) from test_vtable_select_0.vtb_org_normal_0.nchar_32_col)")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_empty` ("
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
                      "nchar_32_col nchar(32))")

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
                      "double_tag double,"
                      "nchar_32_tag nchar(32),"
                      "binary_32_tag binary(32))"
                      "VIRTUAL 1")

        tdLog.info(f"prepare virtual child table.")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_full` ("
                      "u_tinyint_col from test_vtable_select_0.vtb_org_normal_0.u_tinyint_col, "
                      "u_smallint_col from test_vtable_select_0.vtb_org_normal_0.u_smallint_col, "
                      "u_int_col from test_vtable_select_0.vtb_org_normal_0.u_int_col, "
                      "u_bigint_col from test_vtable_select_0.vtb_org_normal_0.u_bigint_col, "
                      "tinyint_col from test_vtable_select_0.vtb_org_normal_0.tinyint_col, "
                      "smallint_col from test_vtable_select_0.vtb_org_normal_0.smallint_col, "
                      "int_col from test_vtable_select_0.vtb_org_normal_0.int_col, "
                      "bigint_col from test_vtable_select_0.vtb_org_normal_0.bigint_col, "
                      "float_col from test_vtable_select_0.vtb_org_normal_0.float_col, "
                      "double_col from test_vtable_select_0.vtb_org_normal_0.double_col, "
                      "bool_col from test_vtable_select_0.vtb_org_normal_0.bool_col, "
                      "binary_16_col from test_vtable_select_0.vtb_org_normal_0.binary_16_col,"
                      "binary_32_col from test_vtable_select_0.vtb_org_normal_0.binary_32_col,"
                      "nchar_16_col from test_vtable_select_0.vtb_org_normal_0.nchar_16_col,"
                      "nchar_32_col from test_vtable_select_0.vtb_org_normal_0.nchar_32_col)"
                      "USING `vtb_virtual_stb` TAGS (0, false, 0, 0, 'child0', 'child0')")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_half_full` ("
                      "u_tinyint_col from test_vtable_select_0.vtb_org_normal_0.u_tinyint_col, "
                      "u_smallint_col from test_vtable_select_0.vtb_org_normal_0.u_smallint_col, "
                      "u_int_col from test_vtable_select_0.vtb_org_normal_0.u_int_col, "
                      "int_col from test_vtable_select_0.vtb_org_normal_0.int_col, "
                      "bigint_col from test_vtable_select_0.vtb_org_normal_0.bigint_col, "
                      "float_col from test_vtable_select_0.vtb_org_normal_0.float_col, "
                      "binary_32_col from test_vtable_select_0.vtb_org_normal_0.binary_32_col,"
                      "nchar_16_col from test_vtable_select_0.vtb_org_normal_0.nchar_16_col,"
                      "nchar_32_col from test_vtable_select_0.vtb_org_normal_0.nchar_32_col)"
                      "USING `vtb_virtual_stb` TAGS (1, false, 1, 1, 'child1', 'child1')")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_empty` "
                      "USING `vtb_virtual_stb` TAGS (2, false, 2, 2, 'child2', 'child2')")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_mix` ("
                      f"u_tinyint_col from test_vtable_select_0.vtb_org_child_0.u_tinyint_col, "
                      f"u_smallint_col from test_vtable_select_0.vtb_org_child_0.u_smallint_col, "
                      f"u_int_col from test_vtable_select_0.vtb_org_child_0.u_int_col, "
                      f"u_bigint_col from test_vtable_select_0.vtb_org_child_0.u_bigint_col, "
                      f"tinyint_col from test_vtable_select_0.vtb_org_child_0.tinyint_col, "
                      f"smallint_col from test_vtable_select_0.vtb_org_child_0.smallint_col, "
                      f"int_col from test_vtable_select_0.vtb_org_child_0.int_col, "
                      f"bigint_col from test_vtable_select_0.vtb_org_child_0.bigint_col, "
                      f"float_col from test_vtable_select_0.vtb_org_child_0.float_col, "
                      f"double_col from test_vtable_select_0.vtb_org_child_0.double_col, "
                      f"bool_col from test_vtable_select_0.vtb_org_child_0.bool_col, "
                      f"binary_16_col from test_vtable_select_0.vtb_org_child_0.binary_16_col,"
                      f"binary_32_col from test_vtable_select_0.vtb_org_child_0.binary_32_col,"
                      f"nchar_16_col from test_vtable_select_0.vtb_org_child_0.nchar_16_col,"
                      f"nchar_32_col from test_vtable_select_0.vtb_org_child_0.nchar_32_col)"
                      f"USING `vtb_virtual_stb` TAGS (3, false, 3, 3, 'child3', 'child3')")

    def test_virtual_table_query(self):
        """Query: v-table query

        1. test vtable query when eliminate virtual table scan operator

        Since: v3.3.8.0

        Labels: virtual

        Jira: None

        History:
            - 2026-1-27 Jing Sima Created

        """

        queries = [
            "select * from test_vtable_select.vtb_virtual_ntb_full",
            "select count(*) from test_vtable_select.vtb_virtual_ntb_full where _c0 >= \"2020-01-01 00:00:00\" and _c0 < \"2020-01-02 00:00:00\"",
            "select * from test_vtable_select.vtb_virtual_ntb_full where _c0 >= \"2020-01-01 00:00:00\" and _c0 < \"2020-01-02 00:00:00\" partition by tbname",
            "select count(*) from test_vtable_select.vtb_virtual_ntb_full",
            "select first(*) from test_vtable_select.vtb_virtual_ntb_full",
            "select last(*) from test_vtable_select.vtb_virtual_ntb_full",
            "select avg(u_tinyint_col), avg(u_smallint_col), avg(u_int_col) from test_vtable_select.vtb_virtual_ntb_full",
            "select * from test_vtable_select.vtb_virtual_ntb_full partition by tbname",
            "select * from test_vtable_select.vtb_virtual_ntb_full partition by u_tinyint_col",
            "select avg(u_tinyint_col), avg(u_smallint_col), avg(u_int_col) from test_vtable_select.vtb_virtual_ntb_full interval(1s)",
            "select avg(u_tinyint_col), avg(u_smallint_col), avg(u_int_col) from test_vtable_select.vtb_virtual_ntb_full state_window(bool_col)",
            "select _wstart, _wend, first(*), last(*), count(*) from test_vtable_select.vtb_virtual_ntb_full event_window start with u_tinyint_col > 50 end with u_tinyint_col > 200",
            "select _wstart, _wend, first(*), last(*), count(*) from test_vtable_select.vtb_virtual_ntb_full session(ts, 1a)",
            "select cols(last(u_tinyint_col), u_tinyint_col as `u_tinyint_col`,  _c0 as `ts`), cols(last(u_smallint_col), u_smallint_col as `u_smallint_col`, _c0 as `ts`), cols(last(u_int_col), u_int_col as `u_int_col`, _c0 as `ts`) from test_vtable_select.vtb_virtual_ntb_full",
        ]

        for sql in queries:
            exp_sql = sql.replace(
                "test_vtable_select.vtb_virtual_ntb_full",
                "test_vtable_select_0.vtb_org_normal_0",
            )
            tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)

