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
from new_test_framework.utils import tdLog, tdSql


class TestVtableChildColrefAfterAlter:

    def setup_class(cls):
        tdLog.info("prepare virtual table column reference alter case.")

        tdSql.execute("drop database if exists test_vtable_child_colref_after_alter;")
        tdSql.execute("create database test_vtable_child_colref_after_alter;")
        tdSql.execute("use test_vtable_child_colref_after_alter;")

        tdSql.execute("create table src (ts timestamp, c2 int, c3 int);")
        tdSql.execute("insert into src values ('2026-06-11 11:15:00', 20, 30);")

        tdSql.execute("create stable vstb (ts timestamp, c1 int, c2 int) tags (t int) virtual 1;")
        tdSql.execute("alter stable vstb drop column c1;")
        tdSql.execute("alter stable vstb add column c3 int;")

        tdSql.execute("create vtable vctb_pos (src.c2, src.c3) using vstb tags (1);")
        tdSql.execute("create vtable vctb_empty using vstb tags (2);")

    def check_query_result(self, table_name, rows, cols):
        tdSql.query(f"select * from {table_name};")
        tdSql.checkRows(rows)
        tdSql.checkCols(cols)

    def test_virtual_child_colref_uses_real_stable_col_id(self):
        """Alter: virtual child table keeps column references after stable column drop

        Create virtual child tables after their virtual stable has non-contiguous
        column ids, then drop a later stable column and query the children.

        Catalog:
            - VirtualTable

        Since: v3.3.8.0

        Labels: alter,virtual,integration,functional
        Jira: None

        History:
            - 2026-06-22 Jing Sima Created

        """
        tdSql.execute("use test_vtable_child_colref_after_alter;")

        self.check_query_result("vctb_pos", 1, 3)
        self.check_query_result("vctb_empty", 0, 3)

        tdSql.execute("alter stable vstb drop column c3;")

        self.check_query_result("vctb_pos", 1, 2)
        self.check_query_result("vctb_empty", 0, 2)
