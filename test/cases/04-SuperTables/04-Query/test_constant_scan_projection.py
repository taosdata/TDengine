# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import tdLog, tdSql, etool


class TestConstantScanProjection:
    DB = "constant_scan_projection"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def check_system_constant_rows(self, source, reference_column, suffix=""):
        tdSql.query(f"select {reference_column} from {source} {suffix}")
        expected_rows = tdSql.queryRows
        tdSql.query(f"select 1 from {source} {suffix}")
        tdSql.checkRows(expected_rows)
        for row in range(expected_rows):
            tdSql.checkData(row, 0, 1)

    def prepare_data(self):
        tdSql.execute(f"drop database if exists {self.DB}")
        tdSql.execute(f"create database {self.DB} vgroups 1")
        tdSql.execute(f"use {self.DB}")
        tdSql.execute("create stable st (ts timestamp, v int) tags (device int)")
        tdSql.execute(
            "create table t0 using st tags (0) "
            "t1 using st tags (1) "
            "t2 using st tags (2) "
            "t3 using st tags (3) "
            "t4 using st tags (4)"
        )
        tdSql.execute("insert into t1 values ('2026-10-20 00:00:00', 1)")
        print("constant scan data ......................... [ passed ]")

    def check_tag_scan(self):
        cases = [
            ("select tags 1 from st", 5),
            ("select tags 1 from st where device < 3", 3),
            ("select tags 1 from st where device < 0", 0),
            ("select tags 1 from st limit 2 offset 1", 2),
        ]
        for sql, count in cases:
            tdSql.query(sql)
            tdSql.checkRows(count)
            for row in range(count):
                tdSql.checkData(row, 0, 1)
        tdSql.query("select tags 1 in (1, null) from t0 order by 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, True)
        tdSql.query("select tags 1 in (select v from t0) from t0 order by 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, False)

        tdSql.query("select tags 1 from st order by 1")
        tdSql.checkRows(5)
        for row in range(5):
            tdSql.checkData(row, 0, 1)
        tdSql.query("select distinct tags 1 from st")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.query("select tags 1 in (1, null) from st order by 1")
        tdSql.checkRows(5)
        for row in range(5):
            tdSql.checkData(row, 0, True)
        tdSql.query("select distinct tags (select 1 from st limit 1) + 1 from st")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.query("select tags 1 in (select device from st) from st order by 1")
        tdSql.checkRows(5)
        for row in range(5):
            tdSql.checkData(row, 0, True)
        print("tag constant projection ......................... [ passed ]")

    def check_system_scan(self):
        self.check_system_constant_rows("information_schema.ins_tables", "table_name")
        self.check_system_constant_rows(
            "information_schema.ins_tables", "table_name",
            f"where db_name = '{self.DB}'"
        )
        self.check_system_constant_rows(
            "information_schema.ins_tables", "table_name",
            "where table_name = 't0'"
        )
        self.check_system_constant_rows(
            "information_schema.ins_tables", "table_name",
            f"where db_name = '{self.DB}' and table_name = 't0'"
        )
        self.check_system_constant_rows(
            "information_schema.ins_tables", "table_name",
            f"where db_name = '{self.DB}' order by table_name limit 2 offset 1"
        )
        self.check_system_constant_rows("information_schema.ins_users", "name")
        self.check_system_constant_rows(
            "information_schema.ins_users", "name", "where name = 'root'"
        )
        self.check_system_constant_rows(
            "information_schema.ins_users", "name", "where name = 'missing_user'"
        )
        self.check_system_constant_rows("information_schema.ins_databases", "name")
        self.check_system_constant_rows(
            "information_schema.ins_databases", "name", f"where name = '{self.DB}'"
        )
        self.check_system_constant_rows(
            "information_schema.ins_databases", "name", "order by name limit 1 offset 1"
        )
        tdSql.query(
            "select 1 from information_schema.ins_tables "
            f"where db_name = '{self.DB}'"
        )
        tdSql.checkRows(5)
        for row in range(5):
            tdSql.checkData(row, 0, 1)
        tdSql.query(
            "select 1 from information_schema.ins_tables "
            f"where db_name = '{self.DB}' limit 2 offset 1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.query(
            "select 1 from information_schema.ins_tables "
            "where db_name = 'constant_scan_projection_missing'"
        )
        tdSql.checkRows(0)
        tdSql.query(
            "select 1 from information_schema.ins_tables "
            f"where db_name = '{self.DB}' and table_name = 't0'"
        )
        tdSql.checkRows(1)
        tdSql.execute("create stable st2 (ts timestamp, v int) tags (device int)")
        tdSql.execute("create table t5 using st2 tags (5)")
        tdSql.execute("create table t6 (ts timestamp, v int)")
        tdSql.query(
            "select 1 from information_schema.ins_tables "
            f"where db_name = '{self.DB}'"
        )
        tdSql.checkRows(7)
        tdSql.query(
            "select 1 from information_schema.ins_tables "
            f"where db_name = '{self.DB}' order by table_name limit 2"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        print("system constant projection ......................... [ passed ]")

    def check_constant_aggregate_having(self):
        source = (
            "from information_schema.ins_tables "
            f"where db_name = '{self.DB}'"
        )
        tdSql.query(f"select sum(1) {source} having sum(1) > 0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 7)
        tdSql.query(f"select sum(1) {source} having sum(1) > 7")
        tdSql.checkRows(0)
        tdSql.query(f"select 1 {source} partition by 1")
        tdSql.checkRows(7)

    def check_virtual_stable_scan(self):
        tdSql.execute("create stable vst (ts timestamp, v int) tags (device int) virtual 1")
        tdSql.execute("create vtable vt0(v from t0.v) using vst tags (0)")
        tdSql.execute("insert into t0 values ('2026-10-21 00:00:00', 7)")
        tdSql.query("select v from vst where tbname = 'vt0'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 7)
        tdSql.query("select tags 1 from vst")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

    def test_constant_scan_projection(self):
        """Keep constant projection results correct for system and tag scans.

        1. Check tag scan row counts, filtering, pagination, sort, distinct, and subqueries.
        2. Check system table constant rows and pagination.
        3. Check virtual stable scans that depend on internal system and tag scans.

        Catalog:
            - SuperTable
            - Query

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-21 Codex Add constant projection scan regression coverage.
            - 2026-10-23 Codex Cover sorted and distinct TAGS expressions on supertables.

        """
        self.prepare_data()
        self.check_tag_scan()
        self.check_system_scan()
        self.check_constant_aggregate_having()
        self.check_virtual_stable_scan()
