# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdLog, tdSql, etool


class TestQueryHavingImplicitPartition:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def _query_rows(self, sql, normalize):
        tdSql.query(sql)
        rows = [tuple(row) for row in tdSql.queryResult]
        return sorted(rows, key=repr) if normalize else rows

    def _check_equivalent(self, implicit_sql, explicit_sql, expected_rows=None):
        normalize = "order by" not in implicit_sql.lower()
        tdLog.info("explicit PARTITION SQL: %s" % explicit_sql)
        explicit_rows = self._query_rows(explicit_sql, normalize)
        if expected_rows is not None:
            tdSql.checkAssert(len(explicit_rows) == expected_rows)
        tdLog.info("implicit HAVING SQL: %s" % implicit_sql)
        implicit_rows = self._query_rows(implicit_sql, normalize)
        if expected_rows is not None:
            tdSql.checkAssert(len(implicit_rows) == expected_rows)
        tdSql.checkAssert(implicit_rows == explicit_rows)
        return implicit_rows

    def _check_both_error(self, implicit_sql, explicit_sql):
        tdSql.error(explicit_sql)
        explicit_errno = tdSql.errno
        explicit_error = tdSql.error_info
        tdSql.error(
            implicit_sql,
            expectedErrno=explicit_errno,
            expectErrInfo=explicit_error,
        )

    def _check_plan_contains(self, sql, expected):
        tdSql.query("explain verbose true " + sql)
        plan = "\n".join(str(row) for row in tdSql.queryResult)
        tdSql.checkAssert(expected.lower() in plan.lower())

    def do_prepare(self):
        tdSql.execute("drop database if exists db_having_implicit")
        tdSql.execute("create database db_having_implicit vgroups 4")
        tdSql.execute("use db_having_implicit")
        tdSql.execute(
            "create table nt ("
            "ts timestamp, c1 int, c2 int, c3 double, "
            "group_marker int, note varchar(16))"
        )
        tdSql.execute(
            "insert into nt values "
            "(1700000000000, 1, 10, 1.5, 1, 'a') "
            "(1700000001000, 2, 20, 2.5, 1, 'b') "
            "(1700000002000, 3, null, 3.5, 1, 'c') "
            "(1700000003000, 4, 40, null, 1, 'd') "
            "(1700000004000, 5, 50, 5.5, 1, 'e') "
            "(1700000005000, null, 60, 6.5, 1, 'f') "
            "(1700000006000, 2, 20, 2.5, 1, 'b') "
            "(1700000007000, -1, -10, -1.5, 1, 'neg')"
        )
        tdSql.execute(
            "create table nt_window ("
            "ts timestamp, state_col int, value_col int)"
        )
        tdSql.execute(
            "insert into nt_window values "
            "(1700000000000, 1, 10) "
            "(1700000001000, 1, 20) "
            "(1700000002000, 2, 30) "
            "(1700000003000, 2, 40) "
            "(1700000004000, 2, 50)"
        )
        tdSql.execute(
            "create table nt_empty ("
            "ts timestamp, c1 int, c2 int, group_marker int)"
        )
        tdSql.execute(
            "create table nt_last_null (ts timestamp, c1 int, c2 int)"
        )
        tdSql.execute(
            "insert into nt_last_null values "
            "(1700000000000, null, 1) "
            "(1700000001000, null, 1)"
        )
        tdSql.execute(
            "create table nt_join (ts timestamp, v int, group_marker int)"
        )
        tdSql.execute(
            "insert into nt_join values "
            "(1700000000000, 100, 1) "
            "(1700000001000, 200, 1) "
            "(1700000002000, 300, 1) "
            "(1700000003000, 400, 1)"
        )
        tdSql.execute(
            "create stable meters ("
            "ts timestamp, c1 int, c2 int, group_marker int) "
            "tags(device int, location varchar(16))"
        )
        for table_id in range(8):
            location = "north" if table_id % 2 == 0 else "south"
            tdSql.execute(
                "create table d%d using meters tags(%d, '%s')"
                % (table_id, table_id, location)
            )
            base_ts = 1710000000000 + table_id * 100000
            tdSql.execute(
                "insert into d%d values "
                "(%d, %d, %d, 1) "
                "(%d, %d, %d, 1) "
                "(%d, %d, %d, 1)"
                % (
                    table_id,
                    base_ts,
                    table_id,
                    table_id * 10,
                    base_ts + 1000,
                    table_id + 1,
                    table_id * 10 + 1,
                    base_ts + 2000,
                    table_id + 2,
                    table_id * 10 + 2,
                )
            )
        tdSql.execute("create table d_empty using meters tags(100, 'empty')")

    def do_ordinary_column_having(self):
        pairs = [
            (
                "select c1, c2 from nt having c1 > 2 order by c1, c2",
                "select c1, c2 from nt partition by group_marker "
                "having c1 > 2 order by c1, c2",
                3,
            ),
            (
                "select c1 from nt having c2 is null order by c1",
                "select c1 from nt partition by group_marker "
                "having c2 is null order by c1",
                1,
            ),
            (
                "select c1 from nt having abs(c1) >= 3 order by c1",
                "select c1 from nt partition by group_marker "
                "having abs(c1) >= 3 order by c1",
                3,
            ),
            (
                "select c1 from nt having true order by c1",
                "select c1 from nt partition by group_marker "
                "having true order by c1",
                8,
            ),
            (
                "select c1 from nt having false",
                "select c1 from nt partition by group_marker having false",
                0,
            ),
            (
                "select c1 from nt having c1 > null",
                "select c1 from nt partition by group_marker having c1 > null",
                0,
            ),
            (
                "select note from nt having note like 'b%' order by note",
                "select note from nt partition by group_marker "
                "having note like 'b%' order by note",
                2,
            ),
            (
                "select ts from nt having ts >= 1700000006000 order by ts",
                "select ts from nt partition by group_marker "
                "having ts >= 1700000006000 order by ts",
                2,
            ),
        ]
        for implicit_sql, explicit_sql, expected_rows in pairs:
            self._check_equivalent(
                implicit_sql, explicit_sql, expected_rows=expected_rows
            )

    def do_aggregate_having(self):
        pairs = [
            (
                "select count(*), sum(c1), avg(c1), min(c1), max(c1) "
                "from nt having count(*) = 8",
                "select count(*), sum(c1), avg(c1), min(c1), max(c1) "
                "from nt partition by group_marker having count(*) = 8",
                1,
            ),
            (
                "select count(*) from nt having count(*) > 100",
                "select count(*) from nt partition by group_marker "
                "having count(*) > 100",
                0,
            ),
            (
                "select count(*), sum(c1) from nt where c1 >= 2 "
                "having count(*) = 5",
                "select count(*), sum(c1) from nt where c1 >= 2 "
                "partition by group_marker having count(*) = 5",
                1,
            ),
            (
                "select count(distinct c1) from nt "
                "having count(distinct c1) = 6",
                "select count(distinct c1) from nt partition by group_marker "
                "having count(distinct c1) = 6",
                1,
            ),
            (
                "select sum(c1 + 1) from nt having sum(c1 + 1) > 0",
                "select sum(c1 + 1) from nt partition by group_marker "
                "having sum(c1 + 1) > 0",
                1,
            ),
        ]
        for implicit_sql, explicit_sql, expected_rows in pairs:
            self._check_equivalent(
                implicit_sql, explicit_sql, expected_rows=expected_rows
            )
        self._check_both_error(
            "select c1 from nt having count(*) > 5",
            "select c1 from nt partition by group_marker having count(*) > 5",
        )
        self._check_both_error(
            "select count(*) from nt having count(*) > 5 and c1 > 0",
            "select count(*) from nt partition by group_marker "
            "having count(*) > 5 and c1 > 0",
        )

    def do_last_all_null_partition_equivalence(self):
        rows = self._check_equivalent(
            "select last(c1) from nt_last_null where c2 > 0 "
            "having last(c1) is null",
            "select last(c1) from nt_last_null where c2 > 0 "
            "partition by c2 having last(c1) is null",
            expected_rows=1,
        )
        tdSql.checkAssert(rows == [(None,)])

        tdLog.info("LAST all-NULL partition equivalence ...... [ passed ]")

    def do_last_row_all_null_partition_equivalence(self):
        rows = self._check_equivalent(
            "select last_row(c1) from nt_last_null where c2 > 0 "
            "having last_row(c1) is null",
            "select last_row(c1) from nt_last_null where c2 > 0 "
            "partition by c2 having last_row(c1) is null",
            expected_rows=1,
        )
        tdSql.checkAssert(rows == [(None,)])

        tdLog.info("LAST_ROW all-NULL partition equivalence .. [ passed ]")

    def do_first_all_null_partition_equivalence(self):
        rows = self._check_equivalent(
            "select first(c1) from nt_last_null where c2 > 0 "
            "having first(c1) is null",
            "select first(c1) from nt_last_null where c2 > 0 "
            "partition by c2 having first(c1) is null",
            expected_rows=1,
        )
        tdSql.checkAssert(rows == [(None,)])

        tdLog.info("FIRST all-NULL partition equivalence ..... [ passed ]")

    def do_having_column_restrictions(self):
        tdSql.query(
            "select c1 from nt partition by group_marker "
            "having c2 is null order by c1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query(
            "select group_marker, count(*) from nt group by group_marker "
            "having group_marker = 1 and count(*) = 8"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 8)
        tdSql.error(
            "select group_marker, count(*) from nt group by group_marker "
            "having c1 > 0 and count(*) > 5"
        )

        tdSql.query(
            "select group_marker, count(*) from nt "
            "partition by group_marker "
            "having group_marker = 1 and count(*) = 8"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 8)
        tdSql.error(
            "select group_marker, count(*) from nt "
            "partition by group_marker "
            "having c1 > 0 and count(*) > 5"
        )

        tdSql.query(
            "select _wstart, state_col, count(*) from nt_window "
            "state_window(state_col) "
            "having state_col = 2 and count(*) = 3"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 3)
        tdSql.error(
            "select _wstart, state_col, count(*) from nt_window "
            "state_window(state_col) "
            "having value_col > 0 and count(*) > 0"
        )

        tdLog.info("HAVING column restrictions ............... [ passed ]")

    def do_empty_and_null_input(self):
        rows = self._check_equivalent(
            "select c1 from nt_empty having true",
            "select c1 from nt_empty partition by group_marker having true",
        )
        tdSql.checkAssert(len(rows) == 0)
        rows = self._check_equivalent(
            "select count(*) from nt_empty having count(*) = 0",
            "select count(*) from nt_empty partition by group_marker "
            "having count(*) = 0",
        )
        tdSql.checkAssert(len(rows) == 0)
        rows = self._check_equivalent(
            "select count(*) from nt where false having count(*) = 0",
            "select count(*) from nt where false partition by group_marker "
            "having count(*) = 0",
        )
        tdSql.checkAssert(len(rows) == 0)

    def do_empty_child_table_single_group_identity(self):
        partition_by_clauses = [
            "partition by group_marker",
            "partition by device",
            "partition by tbname",
        ]
        for partition_by in partition_by_clauses:
            rows = self._check_equivalent(
                "select count(*) from d_empty having count(*) = 0",
                "select count(*) from d_empty " + partition_by
                + " having count(*) = 0",
                expected_rows=1,
            )
            tdSql.checkAssert(rows[0] == (0,))

    def do_no_from(self):
        tdSql.query("select 1 having true")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.query("select 1 having false")
        tdSql.checkRows(0)
        tdSql.query("select 1 having null")
        tdSql.checkRows(0)
        tdSql.query("select 1 where true having true")
        tdSql.checkRows(1)
        tdSql.query("select 1 where false having true")
        tdSql.checkRows(0)
        tdSql.query("select pi() having pi() > 3")
        tdSql.checkRows(1)
        tdSql.query("select count(1) having count(1) = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count(1) order by c1")
        tdSql.checkRows(1)
        tdSql.query(
            "select count(1) having count(1) = 1 order by c1"
        )
        tdSql.checkRows(1)
        tdSql.query("select 1 where false having count(1) = 0")
        tdSql.checkRows(0)
        tdSql.error("select count(*) having count(*) = 1")

    def do_limit_alias_and_hints(self):
        pairs = [
            (
                "select c1 from nt having c1 > 0 order by c1 limit 2 offset 1",
                "select c1 from nt partition by group_marker having c1 > 0 "
                "order by c1 limit 2 offset 1",
                2,
            ),
            (
                "select c1 from nt having c1 > 0 order by c1 "
                "slimit 1 soffset 0 limit 2 offset 0",
                "select c1 from nt partition by group_marker having c1 > 0 "
                "order by c1 slimit 1 soffset 0 limit 2 offset 0",
                2,
            ),
            (
                "select sum(c1) as total from nt having sum(c1) > 0 "
                "order by total",
                "select sum(c1) as total from nt partition by group_marker "
                "having sum(c1) > 0 order by total",
                1,
            ),
            (
                "select /*+ sort_for_group() */ c1 from nt having c1 > 0",
                "select /*+ sort_for_group() */ c1 from nt "
                "partition by group_marker having c1 > 0",
                6,
            ),
            (
                "select /*+ partition_first() */ c1 from nt having c1 > 0",
                "select /*+ partition_first() */ c1 from nt "
                "partition by group_marker having c1 > 0",
                6,
            ),
        ]
        for implicit_sql, explicit_sql, expected_rows in pairs:
            self._check_equivalent(
                implicit_sql, explicit_sql, expected_rows=expected_rows
            )
        self._check_both_error(
            "select sum(c1) as total from nt having total > 0",
            "select sum(c1) as total from nt partition by group_marker "
            "having total > 0",
        )

    def do_query_shapes(self):
        pairs = [
            (
                "select count(*), sum(c1) from "
                "(select c1, c2, group_marker from nt where c1 is not null) q "
                "having sum(c1) > 0",
                "select count(*), sum(c1) from "
                "(select c1, c2, group_marker from nt where c1 is not null) q "
                "partition by group_marker having sum(c1) > 0",
                1,
            ),
            (
                "select a.c1, b.v from nt a join nt_join b on a.ts = b.ts "
                "having a.c1 > 0 order by a.c1, b.v",
                "select a.c1, b.v from nt a join nt_join b on a.ts = b.ts "
                "partition by a.group_marker having a.c1 > 0 "
                "order by a.c1, b.v",
                4,
            ),
            (
                "select count(*), sum(c1) from meters having count(*) = 24",
                "select count(*), sum(c1) from meters "
                "partition by group_marker having count(*) = 24",
                1,
            ),
            (
                "select c1 from meters having c1 >= 7 order by c1",
                "select c1 from meters partition by group_marker "
                "having c1 >= 7 order by c1",
                6,
            ),
            (
                "select count(*), sum(c1) from meters where location = 'north' "
                "having count(*) = 12",
                "select count(*), sum(c1) from meters where location = 'north' "
                "partition by group_marker having count(*) = 12",
                1,
            ),
        ]
        for implicit_sql, explicit_sql, expected_rows in pairs:
            self._check_equivalent(
                implicit_sql, explicit_sql, expected_rows=expected_rows
            )

    def do_function_boundaries(self):
        pairs = [
            (
                "select sum(avg(c1)) from nt having sum(avg(c1)) > 0",
                "select sum(avg(c1)) from nt partition by group_marker "
                "having sum(avg(c1)) > 0",
            ),
            (
                "select diff(c1) from nt having count(*) > 0",
                "select diff(c1) from nt partition by group_marker "
                "having count(*) > 0",
            ),
            (
                "select top(c1, 2) from nt having count(*) > 0",
                "select top(c1, 2) from nt partition by group_marker "
                "having count(*) > 0",
            ),
            (
                "select percentile(c1, 50) from nt "
                "having percentile(c1, 50) > 0",
                "select percentile(c1, 50) from nt partition by group_marker "
                "having percentile(c1, 50) > 0",
            ),
            (
                "select c1 from nt having missing_column > 0",
                "select c1 from nt partition by group_marker "
                "having missing_column > 0",
            ),
        ]
        for implicit_sql, explicit_sql in pairs:
            self._check_both_error(implicit_sql, explicit_sql)

    def do_plan_shape(self):
        self._check_plan_contains(
            "select /*+ partition_first() */ c1 from nt having c1 > 0",
            "Partition Key",
        )
        self._check_plan_contains(
            "select count(*) from nt having count(*) > 0",
            "Aggregate",
        )

    def do_existing_having_regression(self):
        tdSql.query(
            "select group_marker, count(*) from nt partition by group_marker "
            "having count(*) = 8"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 8)
        tdSql.query(
            "select c1, count(*) from nt group by c1 "
            "having count(*) >= 1 order by c1"
        )
        tdSql.checkRows(7)
        tdSql.query(
            "select _wstart, count(*) from nt interval(2s) "
            "having count(*) = 2 order by _wstart"
        )
        tdSql.checkRows(4)

    def test_query_having_implicit_partition(self):
        """Implicit-partition HAVING semantics

        Verify that HAVING without explicit grouping has the same result and
        error boundaries as PARTITION BY a column whose value is identical for
        every input row.

        Catalog:
            - Query:Having

        Since: v3.4.2.0

        Labels: common,ci
        Jira: None

        History:
            - 2026-08-12 Tony Zhang Created
            - 2026-08-13 Tony Zhang Added column restriction matrix
            - 2026-08-26 Tony Zhang Added empty child table equivalence checks
            - 2026-08-26 Tony Zhang Added all-NULL LAST equivalence check
            - 2026-08-26 Tony Zhang Added FIRST and LAST_ROW equivalence checks

        """
        self.do_prepare()
        self.do_ordinary_column_having()
        self.do_aggregate_having()
        self.do_last_all_null_partition_equivalence()
        self.do_last_row_all_null_partition_equivalence()
        self.do_first_all_null_partition_equivalence()
        self.do_having_column_restrictions()
        self.do_empty_and_null_input()
        self.do_empty_child_table_single_group_identity()
        self.do_no_from()
        self.do_limit_alias_and_hints()
        self.do_query_shapes()
        self.do_function_boundaries()
        self.do_plan_shape()
        self.do_existing_having_regression()
