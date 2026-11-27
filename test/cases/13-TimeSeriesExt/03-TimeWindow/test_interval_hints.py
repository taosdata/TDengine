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

from math import isclose

from new_test_framework.utils import tdLog, tdSql


class TestIntervalHints:
    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def _rows(self, sql):
        tdSql.query(sql)
        return [
            [tdSql.getData(row, col) for col in range(len(tdSql.cursor.description))]
            for row in range(tdSql.getRows())
        ]

    def _equal(self, expected, actual, ordered=True):
        if not ordered:
            expected = sorted(expected, key=repr)
            actual = sorted(actual, key=repr)
        tdSql.checkAssert(len(expected) == len(actual))
        for expected_row, actual_row in zip(expected, actual):
            tdSql.checkAssert(len(expected_row) == len(actual_row))
            for expected_value, actual_value in zip(expected_row, actual_row):
                if isinstance(expected_value, float):
                    tdSql.checkAssert(actual_value is not None and isclose(actual_value, expected_value, rel_tol=1e-6))
                else:
                    tdSql.checkAssert(expected_value == actual_value)

    def _hinted_sql(self, sql, hint):
        return sql.replace("select", f"select /*+ {hint} */", 1)

    def _assert_hint_results(self, sql, ordered=True, repeat=1):
        expected = self._rows(sql)
        for hint in ("FAST_INTERVAL()", "EX_INTERVAL()"):
            hinted = self._hinted_sql(sql, hint)
            for _ in range(repeat):
                self._equal(expected, self._rows(hinted), ordered)

    def _assert_plan_mode(self, sql, hint, fast_interval, ex_interval, merge_fast=None):
        tdSql.query(f"explain verbose true {self._hinted_sql(sql, hint)}", queryTimes=1)
        plan = "\n".join(str(row) for row in tdSql.queryResult)
        modes = {mode for mode in ("fast", "ex", "normal") if f"mode={mode}" in plan}
        if not modes:
            modes = {"normal"}
        expected_mode = "ex" if ex_interval else ("fast" if fast_interval else "normal")
        assert modes == {expected_mode}, (
            f"hint={hint}, expected_mode={expected_mode}, modes={modes}, plan={plan}"
        )
        if merge_fast is not None:
            merge_lines = [str(row) for row in tdSql.queryResult if "Merge Aligned Interval" in str(row)]
            assert merge_lines and all(("mode=fast" in row) == merge_fast for row in merge_lines), plan

    # --- impl ---
    def do_prepare_data(self):
        tdSql.execute("drop database if exists interval_hints")
        tdSql.execute("create database interval_hints vgroups 2")
        tdSql.execute("create stable interval_hints.st(ts timestamp, f float, d double, i bigint) tags(t int)")
        for table in range(3):
            tdSql.execute(f"create table interval_hints.ct{table} using interval_hints.st tags({table})")
            tdSql.execute(
                f"insert into interval_hints.ct{table} values "
                f"('2024-01-01 00:00:00.000', {table + 1.5}, {table + 10.0}, {table + 1}) "
                f"('2024-01-01 00:00:05.000', {table + 2.5}, {table + 20.0}, {table + 2}) "
                f"('2024-01-01 00:00:10.000', {table + 3.5}, {table + 30.0}, {table + 3})"
            )
        tdSql.execute("create table interval_hints.ct_null using interval_hints.st tags(99)")
        tdSql.execute(
            "insert into interval_hints.ct_null values "
            "('2024-01-01 00:00:20.000', NULL, NULL, 7) "
            "('2024-01-01 00:00:25.000', 8.0, 80.0, 8)"
        )
        print("prepare interval hint data .......... [ passed ]")

    def do_verify_supported_results(self):
        query = (
            "select count(*), sum(f), avg(f), sum(d), avg(d) "
            "from interval_hints.st where ts >= '2024-01-01 00:00:00' "
            "and ts < '2024-01-01 00:00:20' interval(10s)"
        )
        tdSql.query(query)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 18.0)
        tdSql.checkData(0, 2, 3.0)
        tdSql.checkData(0, 3, 96.0)
        tdSql.checkData(0, 4, 16.0)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, 13.5)
        tdSql.checkData(1, 2, 4.5)
        tdSql.checkData(1, 3, 93.0)
        tdSql.checkData(1, 4, 31.0)

        full_query = (
            "select _wstart, _wend, _wduration, count(*), sum(f), avg(f), sum(d), avg(d) "
            "from interval_hints.st where ts >= '2024-01-01 00:00:00' "
            "and ts < '2024-01-01 00:00:20' interval(10s)"
        )
        self._assert_plan_mode(full_query, "FAST_INTERVAL()", True, False)
        self._assert_plan_mode(full_query, "EX_INTERVAL()", False, True)
        self._assert_hint_results(full_query, repeat=4)
        print("supported fast and ex results ....... [ passed ]")

    def do_verify_null_and_empty_results(self):
        null_query = (
            "select count(*), count(f), sum(f), avg(f), sum(d), avg(d) "
            "from interval_hints.st where ts >= '2024-01-01 00:00:20' "
            "and ts < '2024-01-01 00:00:30' interval(10s)"
        )
        tdSql.query(null_query)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 8.0)
        tdSql.checkData(0, 3, 8.0)
        tdSql.checkData(0, 4, 80.0)
        tdSql.checkData(0, 5, 80.0)
        self._assert_hint_results(null_query, repeat=3)

        empty_query = (
            "select _wstart, count(*), sum(f), avg(f) "
            "from interval_hints.st where ts >= '2024-01-01 00:00:40' "
            "and ts < '2024-01-01 00:00:50' interval(10s)"
        )
        tdSql.query(empty_query)
        tdSql.checkRows(0)
        self._assert_hint_results(empty_query, repeat=5)
        print("null and empty interval results ..... [ passed ]")

    def do_verify_sparse_windows(self):
        # Keep the window count near the optimizer limit while only materializing
        # a few windows. This covers EX/FAST sparse-window result traversal.
        tdSql.execute("create table interval_hints.ct_sparse using interval_hints.st tags(100)")
        tdSql.execute(
            "insert into interval_hints.ct_sparse values "
            "('2024-01-01 00:00:00.000', 1.0, 10.0, 1) "
            "('2024-01-01 13:53:18.000', 2.0, 20.0, 2) "
            "('2024-01-02 03:46:37.000', 3.0, 30.0, 3)"
        )
        sparse_query = (
            "select _wstart, count(*), sum(f), avg(d) from interval_hints.st "
            "where ts >= '2024-01-01 00:00:00' and ts < '2024-01-02 03:46:38' interval(1s)"
        )
        tdSql.query(sparse_query)
        tdSql.checkRows(7)
        self._assert_plan_mode(sparse_query, "FAST_INTERVAL()", True, False)
        self._assert_plan_mode(sparse_query, "EX_INTERVAL()", False, True)
        self._assert_hint_results(sparse_query, repeat=3)
        sparse_batch_query = (
            "select count(*), sum(f), avg(d) from interval_hints.st "
            "where ts >= '2024-01-01 00:00:00' and ts < '2024-01-02 03:46:38' interval(1s)"
        )
        self._assert_plan_mode(sparse_batch_query, "FAST_INTERVAL()", True, False)
        self._assert_plan_mode(sparse_batch_query, "EX_INTERVAL()", False, True)
        self._assert_hint_results(sparse_batch_query, repeat=3)
        print("sparse interval window results ...... [ passed ]")

    def do_verify_dense_windows(self):
        tdSql.execute("create table interval_hints.ct_dense using interval_hints.st tags(101)")
        start = 1704167300000
        values = " ".join(
            f"({start + index * 1000}, {index + 1}.0, {index + 10}.0, {index + 1})"
            for index in range(20)
        )
        tdSql.execute(f"insert into interval_hints.ct_dense values {values}")
        dense_query = (
            "select _wstart, count(*), sum(f), avg(d) from interval_hints.st "
            f"where ts >= {start} and ts < {start + 20000} interval(1s)"
        )
        tdSql.query(dense_query)
        tdSql.checkRows(20)
        self._assert_plan_mode(dense_query, "FAST_INTERVAL()", True, False)
        self._assert_plan_mode(dense_query, "EX_INTERVAL()", False, True)
        self._assert_hint_results(dense_query, repeat=3)
        print("dense interval window results ....... [ passed ]")

    def do_verify_tbname_partition_results(self):
        query = (
            "select _wstart, _wend, _wduration, count(*), sum(f), avg(f), sum(d), avg(d) "
            "from interval_hints.st where ts >= '2024-01-01 00:00:00' "
            "and ts < '2024-01-01 00:00:20' partition by tbname interval(10s)"
        )
        self._assert_plan_mode(query, "FAST_INTERVAL()", True, False)
        self._assert_plan_mode(query, "EX_INTERVAL()", False, False)
        self._assert_hint_results(query, ordered=False, repeat=2)
        print("tbname partition interval results ... [ passed ]")

    def do_verify_tbname_tag_partition_results(self):
        tdSql.execute(
            "create stable interval_hints.st_tags(ts timestamp, f double) "
            "tags(t int, label varchar(20))"
        )
        tables = (
            ("tag_ct0", "1, 'same'", ("1", "3", "5")),
            ("tag_ct1", "1, 'same'", ("11", "13", "15")),
            ("tag_ct_null", "NULL, NULL", ("NULL", "7", "NULL")),
        )
        for table, tags, values in tables:
            tdSql.execute(f"create table interval_hints.{table} using interval_hints.st_tags tags({tags})")
            first, second, third = values
            tdSql.execute(
                f"insert into interval_hints.{table} values "
                f"('2024-01-01 00:00:00', {first}) "
                f"('2024-01-01 00:00:05', {second}) "
                f"('2024-01-01 00:00:10', {third})"
            )
        tdSql.execute(
            "create table interval_hints.tag_ct_empty using interval_hints.st_tags tags(NULL, 'empty')"
        )
        expected_values = [
            ["tag_ct0", 1, "same", 2, 2.0],
            ["tag_ct0", 1, "same", 1, 5.0],
            ["tag_ct1", 1, "same", 2, 12.0],
            ["tag_ct1", 1, "same", 1, 15.0],
            ["tag_ct_null", None, None, 2, 7.0],
            ["tag_ct_null", None, None, 1, None],
        ]
        query = (
            "select tbname, t, label, count(*), avg(f), _wstart "
            "from interval_hints.st_tags where ts >= '2024-01-01 00:00:00' "
            "and ts < '2024-01-01 00:00:20' partition by {keys} interval(10s)"
        )
        for keys in ("tbname, t", "tbname, t, label", "t, tbname, label"):
            sql = query.format(keys=keys)
            expected = self._rows(sql)
            self._equal(expected_values, [row[:5] for row in expected], ordered=False)
            for hint in ("FAST_INTERVAL()", "SPLIT_SCAN() FAST_INTERVAL()"):
                self._assert_plan_mode(sql, hint, True, False)
                for _ in range(2):
                    self._equal(expected, self._rows(self._hinted_sql(sql, hint)), ordered=False)

        fallback = query.format(keys="tbname, t, label, f")
        self._assert_plan_mode(fallback, "FAST_INTERVAL()", False, False)
        self._assert_hint_results(fallback, ordered=False)

        # Four tables across two vgroups require at least 120000 slots in one
        # vgroup for these 60000 windows, exceeding the former 100000 limit.
        large_slots = query.format(keys="tbname, t, label").replace(
            "00:00:20", "00:01:00"
        ).replace("interval(10s)", "interval(1a)")
        self._assert_plan_mode(large_slots, "FAST_INTERVAL()", True, False)
        self._assert_hint_results(large_slots, ordered=False, repeat=2)
        print("tbname and tag partition results .... [ passed ]")

    def do_verify_tag_partition_results(self):
        tdSql.query(
            "select vgroup_id from information_schema.ins_tables "
            "where db_name = 'interval_hints' and table_name = 'tag_ct0'"
        )
        target_vgroup = tdSql.getData(0, 0)
        cross_table = None
        for index in range(32):
            table = f"tag_cross{index}"
            tdSql.execute(f"create table interval_hints.{table} using interval_hints.st_tags tags(1, 'same')")
            tdSql.query(
                "select vgroup_id from information_schema.ins_tables "
                f"where db_name = 'interval_hints' and table_name = '{table}'"
            )
            if tdSql.getData(0, 0) != target_vgroup:
                cross_table = table
                break
        tdSql.checkAssert(cross_table is not None)
        tdSql.execute(
            f"insert into interval_hints.{cross_table} values "
            "('2024-01-01 00:00:00', 100) ('2024-01-01 00:00:10', 20)"
        )
        query = (
            "select t, label, _wstart, count(*), count(f), sum(f), avg(f) "
            "from interval_hints.st_tags where ts >= '2024-01-01 00:00:00' "
            "and ts < '2024-01-01 00:00:20' partition by t, label interval(10s)"
        )
        expected = self._rows(query)
        self._equal(
            [[1, "same", 5, 5, 128.0, 25.6], [1, "same", 3, 3, 40.0, 40.0 / 3],
             [None, None, 2, 1, 7.0, 7.0], [None, None, 1, 0, None, None]],
            [row[:2] + row[3:] for row in expected], ordered=False,
        )
        queries = (
            query,
            query.replace("partition by t, label", "partition by label, t"),
            query.replace("t, label, _wstart", "t, _wstart").replace("partition by t, label", "partition by t"),
        )
        for sql in queries:
            expected = self._rows(sql)
            for hint in ("FAST_INTERVAL()", "SPLIT_SCAN() FAST_INTERVAL()"):
                self._assert_plan_mode(sql, hint, True, False, merge_fast=True)
                self._equal(expected, self._rows(self._hinted_sql(sql, hint)), ordered=False)
        filtered = query.replace("where ts >=", "where f > 2 and ts >=")
        self._assert_plan_mode(filtered, "FAST_INTERVAL()", True, False, merge_fast=True)
        self._assert_hint_results(filtered, ordered=False)

        sparse = query.replace("00:00:20", "00:01:00").replace("interval(10s)", "interval(1a)")
        self._assert_plan_mode(sparse, "FAST_INTERVAL()", True, False, merge_fast=True)
        self._assert_hint_results(sparse, ordered=False)
        # At least one vgroup has 18 distinct tag groups and 60000 windows,
        # forcing the optional FAST index to fall back above one million slots.
        for index in range(36):
            tdSql.execute(
                f"create table interval_hints.tag_budget{index} using interval_hints.st_tags "
                f"tags({index + 100}, 'empty')"
            )
        self._assert_hint_results(sparse, ordered=False)
        nested = (
            "select ts, t, label, diff(mv) from ("
            "select _wstart as ts, t, label, cast(cast(floor(avg(f) / 5) as bool) as int) as mv "
            "from interval_hints.st_tags where ts > '2024-01-01 00:00:00' "
            "and ts < '2024-01-01 00:00:20' partition by t, label interval(10s)) partition by t, label"
        )
        self._assert_hint_results(nested, ordered=False)
        inner_fast = nested.replace("select _wstart", "select /*+ FAST_INTERVAL() */ _wstart", 1)
        self._equal(self._rows(nested), self._rows(inner_fast), ordered=False)
        self._assert_plan_mode(inner_fast, "FAST_INTERVAL()", True, False, merge_fast=True)
        self._assert_plan_mode(query, "EX_INTERVAL()", False, False)
        # More windows than the merge result batch, and unequal source counts.
        values = " ".join(f"({1704153600000 + i * 1000}, {i % 11})" for i in range(6000))
        tdSql.execute(f"insert into interval_hints.tag_ct0 values {values}")
        values = " ".join(f"({1704153600000 + i * 1000}, {i % 7})" for i in range(0, 6000, 3))
        tdSql.execute(f"insert into interval_hints.{cross_table} values {values}")
        dense = (
            "select t, label, _wstart, count(*), count(f), sum(f), avg(f), min(f), max(f) "
            "from interval_hints.st_tags where ts >= 1704153600000 "
            "and ts < 1704159600000 partition by t, label interval(1s)"
        )
        self._assert_plan_mode(dense, "FAST_INTERVAL()", True, False, merge_fast=True)
        self._assert_hint_results(dense, ordered=False, repeat=2)
        self._assert_hint_results(dense + " having count(*) < 0", ordered=False)
        tdSql.query("explain analyze verbose true " + self._hinted_sql(dense, "FAST_INTERVAL()"))
        merge_lines = [str(row) for row in tdSql.queryResult if "Merge Aligned Interval" in str(row)]
        assert len(merge_lines) == 1 and "rows=6000 " in merge_lines[0], merge_lines
        print("tag partition merge FAST results .. [ passed ]")

    def do_verify_fallback_results(self):
        query = (
            "select _wstart, _wend, _wduration, count(*), sum(f), avg(f), sum(d), avg(d) "
            "from interval_hints.st where ts >= '2024-01-01 00:00:00' "
            "and ts < '2024-01-01 00:00:20' interval(10s)"
        )
        fallback_queries = [
            (query + " limit 1", True, False, False),
            (query.replace("interval(10s)", "interval(10s) sliding(5s)"), True, False, False),
            (query.replace("interval(10s)", "interval(10s, 1s)"), True, False, False),
            (query.replace("interval(10s)", "interval(1d)"), True, False, False),
            (query.replace("avg(f)", "avg(f + 1)", 1), True, True, False),
            (query.replace("interval(10s)", "partition by t + 1 interval(10s)"), False, False, False),
        ]
        for fallback_query, ordered, fast_interval, ex_interval in fallback_queries:
            self._assert_plan_mode(fallback_query, "FAST_INTERVAL()", fast_interval, False)
            self._assert_plan_mode(fallback_query, "EX_INTERVAL()", False, ex_interval)
            self._assert_hint_results(fallback_query, ordered=ordered, repeat=2 if not ordered else 1)
        print("interval hint fallback results ...... [ passed ]")

    def do_verify_error_and_cleanup(self):
        # Unsupported shapes must remain executable through the legacy path.
        fallback_queries = [
            (
                "select _wstart, count(*) from interval_hints.st "
                "where ts >= '2024-01-01 00:00:00' and ts < '2024-01-01 00:03:20' "
                "interval(1a)",
                False,
            ),
            (
                "select _wstart, max(f) from interval_hints.st "
                "where ts >= '2024-01-01 00:00:00' and ts < '2024-01-01 00:00:20' "
                "interval(10s)",
                True,
            ),
            (
                "select _wstart, avg(f + 1) from interval_hints.st "
                "where ts >= '2024-01-01 00:00:00' and ts < '2024-01-01 00:00:20' "
                "interval(10s)",
                True,
            ),
        ]
        for query, fast_interval in fallback_queries:
            self._assert_plan_mode(query, "FAST_INTERVAL()", fast_interval, False)
            self._assert_plan_mode(query, "EX_INTERVAL()", False, False)
            self._assert_hint_results(query, repeat=2)

        # Planning/execution errors must not leave an EX_INTERVAL task behind.
        error_queries = [
            "select /*+ EX_INTERVAL() */ count(*) from interval_hints.missing interval(10s)",
        ]
        for query in error_queries:
            tdSql.error(query)

        # Repeated completion and database teardown exercise hash remove/release.
        stable_query = (
            "select /*+ EX_INTERVAL() */ _wstart, count(*), sum(f), avg(d) "
            "from interval_hints.st where ts >= '2024-01-01 00:00:00' "
            "and ts < '2024-01-01 00:00:20' interval(10s)"
        )
        for _ in range(20):
            self._rows(stable_query)
        tdSql.execute("drop database interval_hints")
        tdSql.execute("create database interval_hints vgroups 2")
        tdSql.execute("create stable interval_hints.st(ts timestamp, f float, d double) tags(t int)")
        for table in range(3):
            tdSql.execute(f"create table interval_hints.ct{table} using interval_hints.st tags({table})")
            tdSql.execute(
                f"insert into interval_hints.ct{table} values "
                f"('2024-01-01 00:00:00.000', {table + 1.0}, {table + 2.0}) "
                f"('2024-01-01 00:00:05.000', {table + 3.0}, {table + 4.0})"
            )
        self._assert_hint_results(
            "select _wstart, count(*), sum(f), avg(d) from interval_hints.st "
            "where ts >= '2024-01-01 00:00:00' and ts < '2024-01-01 00:00:10' interval(10s)",
            repeat=3,
        )
        print("interval hint error and cleanup results .... [ passed ]")

    # --- main ---
    def test_ex_interval(self):
        """FAST_INTERVAL and EX_INTERVAL correctness and fallback behavior

        1. Verify deterministic multi-vgroup aggregates against exact expected values
        2. Compare FAST_INTERVAL and EX_INTERVAL results with the normal executor path
        3. Cover NULL inputs, empty windows, repeated execution, and unsupported shapes
        4. Verify tbname with multiple tags keeps child tables separate
        5. Cover grouped FAST indexes exceeding 100000 slots
        6. Verify tag-only partial and merge FAST with shared tags across vgroups and capacity fallback
        7. Cover grouped merge batches, unequal source counts, empty filtered output, and EXPLAIN row counts

        Catalog: Timeseries:TimeWindow

        Since: v3.3.6.35

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-27 wpan Add FAST_INTERVAL and EX_INTERVAL coverage
        """
        self.do_prepare_data()
        self.do_verify_supported_results()
        self.do_verify_null_and_empty_results()
        self.do_verify_sparse_windows()
        self.do_verify_dense_windows()
        self.do_verify_tbname_partition_results()
        self.do_verify_tbname_tag_partition_results()
        self.do_verify_tag_partition_results()
        self.do_verify_fallback_results()
        self.do_verify_error_and_cleanup()
