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

class TestVtableQueryAfterAlterOriginTable:

    def setup_class(cls):
        tdLog.info(f"prepare org tables.")

        tdSql.execute("drop database if exists test_vtable_query_after_alter_origin_table;")
        tdSql.execute("create database test_vtable_query_after_alter_origin_table;")
        tdSql.execute("use test_vtable_query_after_alter_origin_table;")

        tdSql.execute("create table ntb_0(ts timestamp, c1 int, c2 int, c3 int);")

        tdLog.info(f"prepare child tables.")

        tdSql.execute("create stable stb (ts timestamp, c1 int, c2 int, c3 int) tags (t1 int);")
        tdSql.execute("create table ctb_0 using stb tags (1);")

        tdLog.info(f"prepare insert data.")

        tdSql.execute("insert into ntb_0 values('2017-07-14 10:40:00.000', 1, 1, 1);")
        tdSql.execute("insert into ntb_0 values('2017-07-14 10:40:00.001', 2, 2, 2);")
        tdSql.execute("insert into ntb_0 values('2017-07-14 10:40:00.002', 3, 3, 3);")

        tdSql.execute("insert into ctb_0 values('2017-07-14 10:40:00.000', 1, 1, 1);")
        tdSql.execute("insert into ctb_0 values('2017-07-14 10:40:00.001', 2, 2, 2);")
        tdSql.execute("insert into ctb_0 values('2017-07-14 10:40:00.002', 3, 3, 3);")

        tdLog.info(f"prepare virtual tables.")
        tdSql.execute("create stable vstb (ts timestamp, c1 int, c2 int, c3 int) tags (t1 int) virtual 1;")
        tdSql.execute("create vtable vtb_0 (c1 from ntb_0.c1, c2 from ntb_0.c2, c3 from ntb_0.c3) using vstb tags (1);")
        tdSql.execute("create vtable vtb_1 (c1 from ctb_0.c1, c2 from ctb_0.c2, c3 from ctb_0.c3) using vstb tags (2);")

    def test_query_after_alter(self):
        """Query: after alter

        test query after alter origin tables

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: alter,virtual,integration,functional

        Jira: None

        History:
            - 2025-11-17 Jing Sima Created

        """

        tdSql.execute("use test_vtable_query_after_alter_origin_table;")
        tdSql.execute("select database();")
        # 0. query vstable before alter origin table

        tdSql.query("select * from vstb;")
        tdSql.checkRows(6)

        # 1. alter origin table (normal table), drop column and add new column with same name, then query vstable

        tdSql.execute("alter table ntb_0 drop column c3;")
        tdSql.execute("alter table ntb_0 add column c3 int;")

        tdSql.query("select * from vstb;")
        tdSql.checkRows(6)

        # 2. alter origin table (child table), drop column and add new column with same name, then query vstable
        tdSql.execute("alter table stb drop column c3;")
        tdSql.execute("alter table stb add column c3 int;")

        tdSql.query("select * from vstb;")
        tdSql.checkRows(6)
