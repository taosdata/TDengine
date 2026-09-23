# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, etool, tdCom
import os

class TestVTableQuery:

    def setup_class(cls):
        tdLog.info(f"prepare org tables.")

        tdSql.execute("drop database if exists test_vtable_select_same_reference_col;")
        tdSql.execute("create database test_vtable_select_same_reference_col;")
        tdSql.execute("use test_vtable_select_same_reference_col;")

        tdLog.info(f"prepare org super table.")
        tdSql.execute("select database();")
        tdSql.execute(f"CREATE STABLE `vtb_org_stb` (ts timestamp, int_col int) TAGS (int_tag int)")

        tdLog.info(f"prepare org child table.")
        for i in range(15):
            tdSql.execute(f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` TAGS ({i});")

        tdLog.info(f"prepare org normal table.")
        for i in range(15):
            tdSql.execute(f"CREATE TABLE `vtb_org_normal_{i}` (ts timestamp, int_col int)")

        tdLog.info(f"insert data into org tables.")
        for table_idx in range(2):
            for i in range(10):
                ts = 1696000000000 + i * 1000
                tdSql.execute(f"insert into vtb_org_normal_{table_idx} values ({ts}, {i})")
                tdSql.execute(f"insert into vtb_org_child_{table_idx} values ({ts}, {i})")

        tdLog.info(f"prepare virtual super table.")
        tdSql.execute(f"CREATE STABLE `vtb_virtual_stb` (ts timestamp, ts_col timestamp, ts_col_2 timestamp, ts_col_3 timestamp, int_col int, int_col_2 int, int_col_3 int, int_col_4 int) TAGS (int_tag int) VIRTUAL 1")

        tdLog.info(f"prepare virtual normal table.")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_1` ("
                      "ts        timestamp, "
                      "ts_col    timestamp from vtb_org_normal_0.ts, "
                      "ts_col_2  timestamp from vtb_org_normal_0.ts, "
                      "ts_col_3  timestamp from vtb_org_normal_0.ts, "
                      "int_col   int       from vtb_org_normal_0.int_col,"
                      "int_col_2 int       from vtb_org_normal_0.int_col,"
                      "int_col_3 int       from vtb_org_normal_0.int_col,"
                      "int_col_4 int       from vtb_org_normal_0.int_col)")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_2` ("
                      "ts        timestamp, "
                      "ts_col    timestamp from vtb_org_normal_0.ts, "
                      "ts_col_2  timestamp from vtb_org_normal_1.ts, "
                      "ts_col_3  timestamp from vtb_org_normal_1.ts, "
                      "int_col   int       from vtb_org_normal_0.int_col,"
                      "int_col_2 int       from vtb_org_normal_0.int_col,"
                      "int_col_3 int       from vtb_org_normal_1.int_col,"
                      "int_col_4 int       from vtb_org_normal_1.int_col)")


        tdLog.info(f"prepare virtual child table.")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_1` ("
                      "ts_col     from vtb_org_normal_0.ts,"
                      "ts_col_2   from vtb_org_normal_0.ts,"
                      "ts_col_3   from vtb_org_normal_0.ts,"
                      "int_col    from vtb_org_normal_0.int_col,"
                      "int_col_2  from vtb_org_normal_0.int_col,"
                      "int_col_3  from vtb_org_normal_0.int_col,"
                      "int_col_4  from vtb_org_normal_0.int_col) "
                      "USING `vtb_virtual_stb` TAGS (1);")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_2` ("
                      "ts_col     from vtb_org_normal_0.ts,"
                      "ts_col_2   from vtb_org_normal_0.ts,"
                      "ts_col_3   from vtb_org_normal_0.ts,"
                      "int_col    from vtb_org_normal_0.int_col,"
                      "int_col_2  from vtb_org_normal_0.int_col,"
                      "int_col_3  from vtb_org_normal_0.int_col,"
                      "int_col_4  from vtb_org_child_0.int_col) "
                      "USING `vtb_virtual_stb` TAGS (2);")


        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_3` ("
                      "ts_col     from vtb_org_normal_1.ts,"
                      "ts_col_2   from vtb_org_normal_1.ts,"
                      "ts_col_3   from vtb_org_normal_1.ts,"
                      "int_col    from vtb_org_normal_1.int_col,"
                      "int_col_2  from vtb_org_normal_1.int_col,"
                      "int_col_3  from vtb_org_normal_1.int_col,"
                      "int_col_4  from vtb_org_normal_1.int_col) "
                      "USING `vtb_virtual_stb` TAGS (3);")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_4` ("
                      "ts_col     from vtb_org_normal_0.ts,"
                      "ts_col_2   from vtb_org_normal_1.ts,"
                      "ts_col_3   from vtb_org_normal_1.ts,"
                      "int_col    from vtb_org_normal_0.int_col,"
                      "int_col_2  from vtb_org_normal_0.int_col,"
                      "int_col_3  from vtb_org_normal_1.int_col,"
                      "int_col_4  from vtb_org_normal_1.int_col) "
                      "USING `vtb_virtual_stb` TAGS (4);")
    def test_select_virtualtable_same_reference_col(self):
        """Query: virtual table with same reference column

        1. test vtable select normal table projection with same reference column
        2. test vtable select child table projection with same reference column
        3. test vtable select super table projection with same reference column


        Catalog:
            - VirtualTable

        Since: v3.3.8.0

        Labels: virtual,integration,functional
        Jira: None

        History:
            - 2025-12-2 Jing Sima Created

        """

        for tbname in ["vtb_virtual_ntb_1", "vtb_virtual_ntb_2"]:
            tdSql.query(f"select * from test_vtable_select_same_reference_col.{tbname};")
            tdSql.checkRows(10)
            for i in range(10):
                ts = 1696000000000 + i * 1000
                for j in range(4):
                    tdSql.checkData(i, j, ts)
                for j in range(4, 8):
                    tdSql.checkData(i, j, i)

        for tbname in ["vtb_virtual_ctb_1", "vtb_virtual_ctb_2", "vtb_virtual_ctb_3", "vtb_virtual_ctb_4"]:
            tdSql.query(f"select * from test_vtable_select_same_reference_col.{tbname};")
            tdSql.checkRows(10)
            for i in range(10):
                ts = 1696000000000 + i * 1000
                for j in range(4):
                    tdSql.checkData(i, j, ts)
                for j in range(4, 8):
                    tdSql.checkData(i, j, i)

        tdSql.query(f"select tbname, * from test_vtable_select_same_reference_col.vtb_virtual_stb order by tbname, ts;")
        tdSql.checkRows(40)
        row_idx = 0
        for table_idx in range(1,5):
            for i in range(10):
                ts = 1696000000000 + i * 1000
                tdSql.checkData(row_idx, 0, f"vtb_virtual_ctb_{table_idx}")
                for j in range(1,5):
                    tdSql.checkData(row_idx, j, ts)
                for j in range(5,9):
                    tdSql.checkData(row_idx, j, i)
                row_idx +=1