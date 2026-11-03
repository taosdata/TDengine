# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

import ctypes

from taos import TaosBind
from taos.error import StatementError

from new_test_framework.utils import tdSql


class TestLimitZeroEmptyResult:
    """LIMIT 0 takes the client-side empty-result path, like WHERE 0=1."""

    def setup_class(cls):
        cls.db = "limit_zero_empty"
        cls.row_count = 1000
        cls.base_ts = 1750000000000
        tdSql.execute(f"drop database if exists {cls.db}")
        tdSql.execute(f"create database {cls.db} vgroups 2 keep 36500")
        tdSql.execute(f"use {cls.db}")
        tdSql.execute("create table src (ts timestamp, v int)")
        tdSql.execute("create table aux (ts timestamp, v int)")
        for start in range(0, cls.row_count, 500):
            src_vals = " ".join(
                f"({cls.base_ts + i * 10},{i})" for i in range(start, start + 500)
            )
            aux_vals = " ".join(
                f"({cls.base_ts + i * 10},{i * 10})" for i in range(start, start + 500)
            )
            tdSql.execute(f"insert into src values {src_vals}")
            tdSql.execute(f"insert into aux values {aux_vals}")
        tdSql.execute(
            f"create vtable vn (ts timestamp, x int from {cls.db}.src.v, "
            f"y int from {cls.db}.aux.v)"
        )
        tdSql.execute("create stable vst (ts timestamp, x int, y int) tags (id int) virtual 1")
        tdSql.execute(
            f"create vtable vc (x from {cls.db}.src.v, y from {cls.db}.aux.v) "
            f"using vst tags (0)"
        )

    @staticmethod
    def plan(sql):
        tdSql.query(f"explain verbose true {sql}")
        return "\n".join(str(row[0]) for row in tdSql.queryResult)

    def check_empty(self, sql, cols):
        tdSql.query(sql)
        tdSql.checkRows(0)
        assert tdSql.queryCols == cols, (sql, tdSql.queryCols)

    def test_limit_zero_rows(self):
        """LIMIT 0 returns no rows for normal and virtual tables.

        1. Cover plain table, vtable, virtual stable and virtual child table
        2. Cover OFFSET and the comma form (offset, count)
        3. Verify LIMIT 0,5 is still five rows, not zero

        Since: v3.4.4.0

        Labels: common,ci,integration,functional
        """
        self.check_empty(f"select * from {self.db}.src limit 0", 2)
        self.check_empty(f"select * from {self.db}.src order by ts desc limit 0", 2)
        self.check_empty(f"select * from {self.db}.vn limit 0", 3)
        self.check_empty(f"select * from {self.db}.vn order by ts desc limit 0", 3)
        self.check_empty(f"select * from {self.db}.vst order by ts desc limit 0", 4)
        # vc is a child of the virtual stable: the same shortcut must hold for
        # the child-table path, which resolves its columns through the parent.
        self.check_empty(f"select * from {self.db}.vc limit 0", 3)
        self.check_empty(f"select * from {self.db}.vc order by ts desc limit 0", 3)

        self.check_empty(f"select * from {self.db}.src limit 0 offset 5", 2)
        self.check_empty(f"select * from {self.db}.src limit 5, 0", 2)
        self.check_empty(f"select * from {self.db}.vn limit 0 offset 5", 3)
        self.check_empty(f"select * from {self.db}.vc limit 0 offset 5", 3)

        tdSql.query(f"select * from {self.db}.src limit 0, 5")
        tdSql.checkRows(5)

        self.check_empty("select 1 limit 0", 1)

    def test_limit_zero_keeps_metadata(self):
        """Connectors still get result columns for LIMIT 0.

        1. Compare the field list with the WHERE 0=1 empty-result path
        2. Check an expression alias and COUNT result type
        3. Check a system table used by schema snapshots

        Since: v3.4.4.0

        Labels: common,ci,integration,functional
        """
        tdSql.query(f"select ts, x, y from {self.db}.vn where 0=1")
        expected = list(tdSql.cursor.description)
        tdSql.query(f"select ts, x, y from {self.db}.vn limit 0")
        assert list(tdSql.cursor.description) == expected
        assert [col[0] for col in expected] == ["ts", "x", "y"]

        # A virtual child table resolves its data columns through the parent
        # stable but does not carry the parent's tag column, so it exposes the
        # timestamp plus the two bound columns.
        tdSql.query(f"select * from {self.db}.vc limit 0")
        assert tdSql.queryCols == 3, tdSql.queryCols
        assert [col[0] for col in tdSql.cursor.description] == ["ts", "x", "y"]

        tdSql.query(f"select cast(x as bigint) as cx, y from {self.db}.vn limit 0")
        assert tdSql.queryCols == 2
        assert tdSql.cursor.description[0][0] == "cx"
        assert tdSql.cursor.description[1][0] == "y"

        tdSql.query(f"select count(*) from {self.db}.vn limit 0")
        tdSql.checkRows(0)
        assert tdSql.queryCols == 1
        assert tdSql.cursor.description[0][0] == "count(*)"

        tdSql.query("select * from information_schema.ins_databases limit 0")
        tdSql.checkRows(0)
        assert tdSql.queryCols > 0

    def test_limit_zero_keeps_validation(self):
        """Invalid statements still fail when LIMIT 0 is present.

        1. Unknown column
        2. Unknown table
        3. Invalid ORDER BY expression

        Since: v3.4.4.0

        Labels: common,ci,integration,functional
        """
        tdSql.error(f"select no_such_col from {self.db}.src limit 0")
        tdSql.error(f"select * from {self.db}.no_such_table limit 0")
        tdSql.error(f"select * from {self.db}.src order by no_such_col limit 0")

    def test_limit_zero_explain_still_plans(self):
        """EXPLAIN keeps the plan for LIMIT 0 and the empty-plan shortcut for predicates.

        1. LIMIT 0 on a plain table still shows its scan plan
        2. LIMIT 0 on virtual tables still shows Virtual Table Scan and Sort
        3. A constant-false predicate keeps the existing empty-plan behavior
        4. EXPLAIN of a non-empty LIMIT still shows its plan

        Since: v3.4.4.0

        Labels: common,ci,integration,functional
        """
        plan = self.plan(f"select * from {self.db}.src limit 0")
        assert "Table Scan" in plan, plan

        plan = self.plan(f"select * from {self.db}.vst order by ts desc limit 0")
        assert "Virtual Table Scan" in plan, plan
        assert "Sort" in plan, plan

        plan = self.plan(f"select * from {self.db}.vc order by ts desc limit 0")
        assert "Virtual Table Scan on vc" in plan, plan

        self.assert_no_plan(f"select * from {self.db}.src where 0=1")

        plan = self.plan(f"select * from {self.db}.src limit 1")
        assert "Table Scan" in plan, plan

    def assert_no_plan(self, sql):
        tdSql.query(f"explain verbose true {sql}")
        tdSql.checkRows(0)

    def test_limit_zero_union_and_subquery(self):
        """LIMIT 0 applies to set operators and derived tables.

        1. Outer LIMIT 0 on a UNION ALL returns no rows
        2. An aggregate over a LIMIT 0 derived table still returns one row of 0
        3. A non-constant empty predicate still returns one COUNT row

        Since: v3.4.4.0

        Labels: common,ci,integration,functional
        """
        tdSql.query(
            f"(select * from {self.db}.src limit 1) union all "
            f"(select * from {self.db}.src limit 1) limit 0"
        )
        tdSql.checkRows(0)

        tdSql.query(f"select * from (select * from {self.db}.src limit 0) limit 1")
        tdSql.checkRows(0)

        # An aggregate over an empty input still owes the client its one row of
        # 0.  An inner LIMIT 0 stays an ordinary empty input, so the outer
        # COUNT(*) must run and report 0 rather than inherit "empty result".
        tdSql.query(f"select count(*) from (select * from {self.db}.src limit 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        # The same holds for a constant-false derived table: the empty-input
        # signal must not suppress the aggregate's own output row.
        tdSql.query(f"select count(*) from (select * from {self.db}.src where 0=1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select count(*) from (select * from {self.db}.src where v = -1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

    @staticmethod
    def run_prepared(sql, value):
        stmt = tdSql.cursor._connection.statement(sql)
        bind = TaosBind()
        bind.bigint(value)
        params = (TaosBind * 1)(bind)
        try:
            stmt.bind_param(ctypes.cast(params, ctypes.c_void_p), add_batch=False)
            stmt.execute()
            result = stmt.use_result()
            rows = result.fetch_all()
            fields = result.field_count
            result.close()
            return rows, fields
        finally:
            stmt.close()

    def test_limit_parameter(self):
        """Prepared LIMIT parameters are zero-empty and reject negatives.

        1. LIMIT ? bound to 0 takes the empty-result path
        2. LIMIT ? bound to 1 still returns one row
        3. LIMIT ? bound to a negative value reports limit < 0

        Since: v3.4.4.0

        Labels: common,ci,integration,functional
        """
        sql = f"select * from {self.db}.src limit ?"
        rows, fields = self.run_prepared(sql, 0)
        assert rows == [] and fields == 2
        rows, fields = self.run_prepared(sql, 1)
        assert len(rows) == 1 and fields == 2

        for value in (-1, -2):
            try:
                self.run_prepared(sql, value)
                assert False, f"negative LIMIT {value} was accepted"
            except StatementError as err:
                assert "limit cannot be less than 0" in str(err), (value, err)
                assert (err.errno & 0xFFFF) == 0x26E9, (value, err.errno)

        union_sql = (
            f"(select * from {self.db}.src limit 1) union all "
            f"(select * from {self.db}.src limit 1) limit ?"
        )
        rows, fields = self.run_prepared(union_sql, 0)
        assert rows == [] and fields == 2
        try:
            self.run_prepared(union_sql, -1)
            assert False, "negative UNION LIMIT was accepted"
        except StatementError as err:
            assert "limit cannot be less than 0" in str(err), err
            assert (err.errno & 0xFFFF) == 0x26E9, err.errno
