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

from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils import tdDnodes

import os
import sys

import pytest

_VTABLE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _VTABLE_DIR)
sys.path.insert(0, os.path.join(_VTABLE_DIR, "01-DDL", "03-ExtSource"))

from vtable_util import ExtSrcEnv, VtableQueryUtil
from ext_source_helpers import (
    create_ext_source, create_mysql_table, create_pg_table, create_remote_db,
    extract_show_create_sql,
)


_LOCAL_DB = "vfilter_query"
_REMOTE_DB = "vfilter_query_ext"
_COLUMNS = ("a", "b", "c", "bounded", "empty_v")


class TestVTableQueryExtSourceFilter:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @pytest.fixture(autouse=True)
    def prepare_backend(self, backend):
        VtableQueryUtil._EXT_SOURCE_ADAPTERS[backend]().ensure_running()
        ExtSrcEnv.ensure_qnode()
        source = f"vfilter_query_{backend}"
        tdSql.query("show local variables like 'timezone'")
        original_timezone = str(tdSql.queryResult[0][1]).split(" ")[0]
        try:
            tdSql.execute('alter local "timezone" "UTC"')
            self.prepare_remote_table(backend)
            source_type = "postgresql" if backend == "postgres" else "mysql"
            create_ext_source(source, source_type, _REMOTE_DB)
            tdSql.execute(f"drop database if exists {_LOCAL_DB}")
            tdSql.execute(f"create database {_LOCAL_DB} precision 'ms'")
            self.prepare_vtables(source)
            yield
        finally:
            tdSql.execute(f'alter local "timezone" "{original_timezone}"')
            tdSql.execute(f"drop database if exists {_LOCAL_DB}")
            tdSql.execute(f"drop external source if exists {source}")
            if backend == "mysql":
                ExtSrcEnv.mysql_drop_db(_REMOTE_DB)
            else:
                ExtSrcEnv.pg_drop_db(_REMOTE_DB)

    # --- util ---

    @staticmethod
    def prepare_remote_table(backend):
        is_mysql = backend == "mysql"
        create_remote_db("mysql" if is_mysql else "postgresql", _REMOTE_DB)
        create_table = create_mysql_table if is_mysql else create_pg_table
        timestamp_type = "datetime(3)" if is_mysql else "timestamp(3)"
        # Tenant 2 and device d3 are sentinels: no tested binding may read them.
        rows = [
            "('2024-01-01 00:00:00', 'd1', 1, 10, 100)",
            "('2024-01-01 00:00:01', 'd1', 1, 11, 110)",
            "('2024-01-01 00:00:03', 'd1', 1, 13, 130)",
            "('2024-01-01 00:00:05', 'd1', 1, 15, null)",
            "('2024-01-01 00:00:00', 'd2', 1, 20, 200)",
            "('2024-01-01 00:00:02', 'd2', 1, 22, 220)",
            "('2024-01-01 00:00:03', 'd2', 1, 23, 230)",
            "('2024-01-01 00:00:06', 'd2', 1, 26, 260)",
            "('2024-01-01 00:00:00', 'd1', 2, 900, 9000)",
            "('2024-01-01 00:00:04', 'd2', 2, 940, 9400)",
            "('2024-01-01 00:00:04', 'd3', 1, 990, 9900)",
        ]
        create_table(_REMOTE_DB, "readings",
                     f"ts {timestamp_type} not null, device varchar(8) not null, "
                     "tenant int not null, v int, w int, primary key (ts, device, tenant)", rows)

    @staticmethod
    def references(source, reverse=False, with_types=False):
        ref = f"{source}.{_REMOTE_DB}.readings"
        first, second = ("d2", "d1") if reverse else ("d1", "d2")
        first_filter = f"device = '{first}' and tenant = 1"
        column_type = " int" if with_types else ""
        return ", ".join((
            f"a{column_type} from {ref}.v where {first_filter}",
            f"b{column_type} from {ref.upper()}.w where {first_filter}",
            f"c{column_type} from {ref}.v where device = '{second}' and tenant = 1",
            f"bounded{column_type} from {ref}.v where {first_filter} and v >= {22 if reverse else 11}",
            f"empty_v{column_type} from {ref}.v where device = 'missing' and tenant = 1",
        ))

    @classmethod
    def prepare_vtables(cls, source):
        tdSql.execute(f"create vtable {_LOCAL_DB}.filter_normal "
                      f"(ts timestamp, {cls.references(source, with_types=True)})")
        schema = "ts timestamp, " + ", ".join(f"{column} int" for column in _COLUMNS)
        tdSql.execute(f"create stable {_LOCAL_DB}.filter_stable ({schema}) tags (scenario int) virtual 1")
        for scenario in (1, 2):
            tdSql.execute(f"create vtable {_LOCAL_DB}.child_{scenario} "
                          f"({cls.references(source, reverse=scenario == 2)}) "
                          f"using {_LOCAL_DB}.filter_stable tags ({scenario})")

        # Different outer column IDs must still retrieve the leaf column's filter.
        for table, leaf, columns in (("middle", "child_1", ("c", "empty_v", "b", "a", "bounded")),
                                     ("chain", "middle", _COLUMNS)):
            refs = ", ".join(f"{column} int from {_LOCAL_DB}.{leaf}.{column}" for column in columns)
            tdSql.execute(f"create vtable {_LOCAL_DB}.{table} (ts timestamp, {refs})")
        tdSql.execute(f"create stable {_LOCAL_DB}.chain_stable ({schema}) tags (scenario int) virtual 1")
        refs = ", ".join(f"{column} from {_LOCAL_DB}.chain.{column}" for column in _COLUMNS)
        tdSql.execute(f"create vtable {_LOCAL_DB}.chain_child ({refs}) "
                      f"using {_LOCAL_DB}.chain_stable tags (1)")

    # --- impl ---

    def do_filter_case_sensitive_tables(self, backend):
        tdSql.execute(f"use {_LOCAL_DB}")
        schema = "ts timestamp(3) not null, device varchar(8) not null, v int, primary key (ts, device)"
        for table, value in (("case_readings", 11), ("Case_Readings", 101)):
            create_pg_table(_REMOTE_DB, f'"{table}"', schema,
                            [f"('2024-01-01 00:00:00', 'd1', {value})"])
        source = f"vfilter_query_{backend}.{_REMOTE_DB}"
        refs = (f"a int from {source}.case_readings.v where device = 'd1', "
                f"b int from {source}.`Case_Readings`.v where device = 'd1'")
        tdSql.execute(f"create vtable {_LOCAL_DB}.case_normal (ts timestamp, {refs})")
        tdSql.execute(f"create vtable {_LOCAL_DB}.case_child ({refs.replace(' int from ', ' from ')}) "
                      f"using {_LOCAL_DB}.filter_stable tags (4)")
        for table in ("case_normal", "case_child"):
            tdSql.query(f"select a, b from {_LOCAL_DB}.{table} order by ts")
            assert list(tdSql.queryResult) == [(11, 101)], (table, tdSql.queryResult)
            ddl = extract_show_create_sql(f"show create vtable {_LOCAL_DB}.{table}")
            tdSql.execute(ddl.replace(table, f"{table}_copy", 1))
            tdSql.query(f"select a, b from {_LOCAL_DB}.{table}_copy order by ts")
            assert list(tdSql.queryResult) == [(11, 101)], (table, tdSql.queryResult)

    def do_filter_own_columns(self, backend):
        tdSql.execute(f"use {_LOCAL_DB}")
        ref = f"vfilter_query_{backend}.{_REMOTE_DB}.readings"
        saved = "device = 'd1' and tenant = 1 and v >= 11"
        tdSql.execute(f"create vtable own_child (a from {ref}.v where device = 'd2' and tenant = 1, "
                      f"own_v int from {ref}.v where {saved}) "
                      f"using filter_stable (scenario, own_tag int) tags (3, 9)")
        tdSql.execute(f"alter vtable own_child add column own_w int from {ref}.w where {saved}")
        tdSql.execute("create vtable own_middle (ts timestamp, "
                      f"w int from {_LOCAL_DB}.own_child.own_w, v int from {_LOCAL_DB}.own_child.own_v)")
        tdSql.execute("create stable own_chain_stable (ts timestamp, v int, w int) tags (scenario int) virtual 1")
        tdSql.execute(f"create vtable own_chain_child (v from {_LOCAL_DB}.own_middle.v, "
                      f"w from {_LOCAL_DB}.own_middle.w) using own_chain_stable tags (1)")
        expected = [(11, 110), (13, 130), (15, None)]
        ddl = extract_show_create_sql("show create vtable own_child")
        assert ddl.lower().count(" where ") == 3, ddl
        tdSql.execute(ddl.replace("own_child", "own_copy", 1))
        for restarted in (False, True):
            if restarted:
                tdDnodes.stop(1)
                tdDnodes.start(1)
                ExtSrcEnv.ensure_qnode()
                tdSql.execute(f"use {_LOCAL_DB}")
            assert extract_show_create_sql("show create vtable own_child") == ddl
            for table in ("own_child", "own_copy", "own_middle", "own_chain_child", "own_chain_stable"):
                columns = "own_v, own_w" if table in ("own_child", "own_copy") else "v, w"
                tdSql.query(f"select {columns} from {table} order by ts")
                assert list(tdSql.queryResult) == expected, (table, tdSql.queryResult, expected)
            tdSql.query("select own_v, own_tag from own_child order by ts")
            assert list(tdSql.queryResult) == [(11, 9), (13, 9), (15, 9)]
            tdSql.query("select v from own_chain_stable where v < 11")
            tdSql.checkRows(0)
            self.check_optimizer_plan("select sum(v) from own_chain_stable", disabled=True)

        # Failed validation must not create an own column or disturb existing bindings.
        tdSql.error(f"alter vtable own_child add column bad int from {ref}.v where device = 'd1'",
                    expectedErrno=0x6212)  # TSDB_CODE_VTABLE_INCOMPLETE_EXT_PK_FILTER
        tdSql.error(f"create vtable own_bad (bad int from {ref}.v where device = 'd1') "
                    "using filter_stable tags (4)",
                    expectedErrno=0x6212)
        tdSql.execute("alter vtable own_child drop column own_w")
        tdSql.execute(f"alter vtable own_child add column own_w int from {ref}.w "
                      "where device = 'd2' and tenant = 1")
        tdSql.query("select own_w from own_child order by ts")
        assert list(tdSql.queryResult) == [(200,), (220,), (230,), (260,)]
        print(f"{backend} own-column saved-filter lifecycle ........ [ passed ]")

    def do_filter_query(self, backend, scenario):
        if scenario == "conflict":
            reference = f"vfilter_query_{backend}.{_REMOTE_DB}.readings.v"
            conditions = (("a", "v = 11"), ("bounded", "v >= 11"),
                          ("c", "ts >= '2024-01-01 00:00:01'"))
            # Rebind the existing leaves; chained views must pick up the same saved restrictions.
            for table in ("filter_normal", "child_1", "child_2"):
                for column, condition in conditions:
                    tdSql.execute(f"alter vtable {_LOCAL_DB}.{table} alter column {column} "
                                  f"set {reference} where device = 'd1' and tenant = 1 and {condition}")
        case_dir = os.path.dirname(os.path.dirname(__file__))
        test_case = f"test_vtable_ext_filter_{scenario}"
        VtableQueryUtil.compare_ext_source_testcase_result(
            os.path.join(case_dir, "in", f"{test_case}.in"),
            os.path.join(case_dir, "ans", f"{test_case}.ans"),
            f"{test_case}_{backend}",
        )
        print(f"{backend} saved-filter {scenario} in/ans ........ [ passed ]")

    def do_filter_pseudo_columns(self, backend):
        for table in ("filter_normal", "child_1", "chain", "chain_child"):
            for suffix, count in (("limit 10", 6),
                                  ("partition by tbname order by tbname limit 10", 6),
                                  ("group by tbname order by tbname", 1)):
                tdSql.query(f"select tbname from {_LOCAL_DB}.{table} {suffix}")
                assert list(tdSql.queryResult) == [(table,)] * count

        for table in ("child_1", "chain_child"):
            tdSql.query(f"select tbname, scenario from {_LOCAL_DB}.{table} limit 10")
            assert list(tdSql.queryResult) == [(table, 1)] * 6
            tdSql.query(f"select tags tbname, scenario from {_LOCAL_DB}.{table}")
            assert list(tdSql.queryResult) == [(table, 1)]

        reference = f"vfilter_query_{backend}.{_REMOTE_DB}.readings.v"
        tdSql.execute(f"create vtable {_LOCAL_DB}.empty_child "
                      f"(a from {reference} where device = 'missing' and tenant = 1) "
                      f"using {_LOCAL_DB}.filter_stable tags (3)")
        tdSql.query(f"select tbname, scenario from {_LOCAL_DB}.empty_child")
        tdSql.checkRows(0)
        tdSql.query(f"select tags tbname, scenario from {_LOCAL_DB}.empty_child")
        assert list(tdSql.queryResult) == [("empty_child", 3)]
        print(f"{backend} saved-filter pseudo-column row counts ........ [ passed ]")

    def do_filter_timezone(self, backend, ddl_timezone):
        tdSql.execute(f"use {_LOCAL_DB}")
        tdSql.execute(f'alter local "timezone" "{ddl_timezone}"')
        reference = f"vfilter_query_{backend}.{_REMOTE_DB}.readings.v"
        boundary = "2024-01-01 00:00:01"
        ddls = {}
        for table in ("filter_normal", "child_1", "child_2"):
            tdSql.execute(f"alter vtable {_LOCAL_DB}.{table} alter column c set {reference} "
                          f"where device = 'd1' and tenant = 1 and ts >= '{boundary}'")
            ddls[table] = extract_show_create_sql(f"show create vtable {_LOCAL_DB}.{table}")
            assert f"'{boundary}'" in ddls[table], ddls[table]

        # Stored remote calendar values must not inherit either session's timezone.
        for query_timezone, midnight in (("UTC", 1704067200000), ("Asia/Shanghai", 1704038400000),
                                          ("America/New_York", 1704085200000)):
            tdSql.execute(f'alter local "timezone" "{query_timezone}"')
            expected = [(midnight + offset * 1000, value) for offset, value in ((1, 11), (3, 13), (5, 15))]
            for table in ("filter_normal", "child_1", "child_2", "filter_stable",
                          "chain", "chain_child", "chain_stable"):
                copies = 2 if table == "filter_stable" else 1
                checks = (
                    (f"select cast(ts as bigint), c from {_LOCAL_DB}.{table} order by ts",
                     sorted(expected * copies)),
                    (f"select c from {_LOCAL_DB}.{table} where ts < '{boundary}'", []),
                    (f"select c from {_LOCAL_DB}.{table} where ts <= '{boundary}'", [(11,)] * copies),
                    (f"select b, c from {_LOCAL_DB}.{table} where ts < '{boundary}' order by b",
                     [(100, None), (200, None)] if copies == 2 else
                     [(200 if table == "child_2" else 100, None)]),
                )
                for sql, rows in checks:
                    tdSql.query(sql)
                    assert list(tdSql.queryResult) == rows, (ddl_timezone, query_timezone, sql, tdSql.queryResult, rows)

            for table, ddl in ddls.items():
                assert extract_show_create_sql(f"show create vtable {_LOCAL_DB}.{table}") == ddl
                copy = f"{table}_copy"
                tdSql.execute(ddl.replace(table, copy, 1))
                try:
                    tdSql.query(f"select cast(ts as bigint), c from {_LOCAL_DB}.{copy} order by ts")
                    assert list(tdSql.queryResult) == expected, (ddl_timezone, query_timezone, tdSql.queryResult)
                finally:
                    tdSql.execute(f"drop vtable {_LOCAL_DB}.{copy}")

    @staticmethod
    def check_optimizer_plan(query, disabled):
        tdSql.query(f"explain verbose true {query}")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        if disabled:
            assert "Virtual Stable Scan" in plan, plan
            assert "Virtual Table Scan on" in plan, plan
            for optimized in ("Virtual Stable Agg", "Virtual Table Window", "Virtual Stable Interval"):
                assert optimized not in plan, plan
        else:
            optimized = "Virtual Stable Interval" if "interval(" in query else "Virtual Stable Agg"
            assert optimized in plan, plan

    @staticmethod
    def prepare_expected_rows():
        schema = "ts timestamp, " + ", ".join(f"{column} int" for column in _COLUMNS)
        tdSql.execute(f"create stable {_LOCAL_DB}.expected_stable ({schema}) tags (scenario int)")
        # Independently materialized virtual rows, including gaps and SQL NULLs.
        first = [(0, 10, 100, 20, None), (1, 11, 110, None, 11), (2, None, None, 22, None),
                 (3, 13, 130, 23, 13), (5, 15, None, None, 15), (6, None, None, 26, None)]
        second = [(0, 20, 200, 10, None), (1, None, None, 11, None), (2, 22, 220, None, 22),
                  (3, 23, 230, 13, 23), (5, None, None, 15, None), (6, 26, 260, None, 26)]
        for scenario, rows in enumerate((first, second), start=1):
            table = f"{_LOCAL_DB}.expected_{scenario}"
            tdSql.execute(f"create table {table} using {_LOCAL_DB}.expected_stable tags ({scenario})")
            values = []
            for second_offset, *columns in rows:
                fields = [1704067200000 + second_offset * 1000, *columns, None]
                values.append("(" + ", ".join("null" if value is None else str(value) for value in fields) + ")")
            tdSql.execute(f"insert into {table} values " + " ".join(values))

    def do_filter_optimizer(self, backend):
        self.prepare_expected_rows()
        source = f"vfilter_query_{backend}"
        second_source = f"{source}_second"
        source_type = "postgresql" if backend == "postgres" else "mysql"
        tdSql.query("show local variables like 'timezone'")
        original_timezone = str(tdSql.queryResult[0][1]).split(" ")[0]
        try:
            tdSql.execute('alter local "timezone" "UTC"')
            create_ext_source(second_source, source_type, _REMOTE_DB)
            schema = "ts timestamp, " + ", ".join(f"{column} int" for column in _COLUMNS)
            tdSql.execute(f"create stable {_LOCAL_DB}.mixed_stable ({schema}) tags (scenario int) virtual 1")
            for scenario in (1, 2):
                refs = self.references(source, reverse=scenario == 2).replace(
                    f"c from {source}.", f"c from {second_source}.")
                tdSql.execute(f"create vtable {_LOCAL_DB}.mixed_{scenario} ({refs}) "
                              f"using {_LOCAL_DB}.mixed_stable tags ({scenario})")

            queries = (
                "select sum(a), sum(c) from {table}",
                "select corr(a, c) from {table}",
                "select scenario, corr(a, c) from {table} partition by scenario order by scenario",
                "select _wstart, sum(a), sum(c) from {table} interval(2s) order by _wstart",
                "select scenario, _wstart, sum(a), sum(c) from {table} partition by scenario "
                "interval(2s) order by scenario, _wstart",
                "select scenario, _wstart, sum(a), sum(c) from {table} partition by scenario "
                "state_window(a) order by scenario, _wstart",
                "select scenario, _wstart, sum(a), sum(c) from {table} partition by scenario "
                "event_window start with a >= 10 end with c >= 23 order by scenario, _wstart",
                "select scenario, _wstart, sum(a), sum(c) from {table} partition by scenario "
                "session(ts, 2s) order by scenario, _wstart",
            )
            for table, expected_table in (("filter_stable", "expected_stable"),
                                          ("mixed_stable", "expected_stable"),
                                          ("chain_stable", "expected_1")):
                for template in queries:
                    query = template.format(table=f"{_LOCAL_DB}.{table}")
                    self.check_optimizer_plan(query, disabled=True)
                    tdSql.query(query)
                    actual = list(tdSql.queryResult)
                    tdSql.query(template.format(table=f"{_LOCAL_DB}.{expected_table}"))
                    assert actual == list(tdSql.queryResult), (query, actual, tdSql.queryResult)

            # A timestamp-only remote primary key permits both filtered and unfiltered bindings.
            create_table = create_mysql_table if backend == "mysql" else create_pg_table
            timestamp_type = "datetime(3)" if backend == "mysql" else "timestamp(3)"
            create_table(_REMOTE_DB, "plain_readings", f"ts {timestamp_type} primary key, v int, w int",
                         ["('2024-01-01 00:00:00', 10, 100)", "('2024-01-01 00:00:01', 11, 110)"])
            ref = f"{source}.{_REMOTE_DB}.plain_readings"
            tdSql.execute(f"create stable {_LOCAL_DB}.plain_stable "
                          "(ts timestamp, a int, c int) tags (scenario int) virtual 1")
            for scenario in (1, 2):
                tdSql.execute(f"create vtable {_LOCAL_DB}.plain_{scenario} "
                              f"(a from {ref}.v, c from {ref}.w) "
                              f"using {_LOCAL_DB}.plain_stable tags ({scenario})")
            tdSql.execute(f"create stable {_LOCAL_DB}.plain_chain_stable "
                          "(ts timestamp, a int, c int) tags (scenario int) virtual 1")
            tdSql.execute(f"create vtable {_LOCAL_DB}.plain_chain "
                          f"(a from {_LOCAL_DB}.plain_2.a, c from {_LOCAL_DB}.plain_2.c) "
                          f"using {_LOCAL_DB}.plain_chain_stable tags (2)")

            for action, disabled in ((None, False), (f"set {ref}.v where v >= 11", True),
                                     (f"set {ref}.v", False), (f"set {ref}.v where v >= 11", True),
                                     ("set null", False)):
                if action is not None:
                    tdSql.execute(f"alter vtable {_LOCAL_DB}.plain_2 alter column a {action}")
                for table in ("plain_stable", "plain_chain_stable"):
                    # Even if only a different column is read, a filtered child disables both rules.
                    for suffix in ("", " interval(2s)"):
                        self.check_optimizer_plan(
                            f"select sum(c) from {_LOCAL_DB}.{table}{suffix}", disabled)
        finally:
            tdSql.execute(f'alter local "timezone" "{original_timezone}"')
            tdSql.execute(f"drop database if exists {_LOCAL_DB}")
            tdSql.execute(f"drop external source if exists {second_source}")

    # --- main ---

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    @pytest.mark.parametrize("scenario", ("projection", "query", "aggregate", "conflict"))
    def test_filter_query(self, backend, scenario):
        """Compare saved external-filter queries with shared in/ans answers

        1. Merge equal, distinct, bounded and empty filters with sparse timestamps
        2. Query normal, child, reordered multi-hop and chained super-table bindings
        3. Compose saved filters with WHERE, OR, JOIN, UNION, subqueries and pagination
        4. Check aggregates, HAVING and time windows without leaking sentinel rows
        5. Check mutually exclusive equality, value and timestamp predicates, including boundary and OR controls

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Add external-filter in/ans regressions
            - 2026-09-04 Simon Guan Cover mutually exclusive saved and query predicates

        """
        self.do_filter_query(backend, scenario)

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    def test_filter_pseudo_columns(self, backend):
        """Preserve filtered data rows for virtual-table pseudo-column queries

        1. Check tbname projection, partition and grouping on normal, child and chained virtual tables
        2. Keep saved filters when projecting only tbname and local tags
        3. Distinguish ordinary projections from SELECT TAGS, including an empty filtered child

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Cover external metadata loading for pseudo-column scans

        """
        self.do_filter_pseudo_columns(backend)

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    @pytest.mark.parametrize("ddl_timezone", ("UTC", "Asia/Shanghai"))
    def test_filter_timezone(self, backend, ddl_timezone):
        """Keep saved remote timestamp predicates stable across session timezones

        1. Save timestamp conditions in UTC and Asia/Shanghai sessions
        2. Query normal, child, super-table and multi-hop bindings in UTC, Asia/Shanghai and America/New_York
        3. Check exact timestamp results, mutually exclusive predicates, inclusive boundaries and unaffected columns
        4. Replay SHOW CREATE in each query timezone and compare the same rows

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Cover saved timestamp-filter timezone independence

        """
        self.do_filter_timezone(backend, ddl_timezone)

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    def test_filter_optimizer(self, backend):
        """Keep filtered super-table aggregation and windows above virtual-row merging

        1. Check fallback plans for direct, multi-source and reordered chained references
        2. Compare SUM, CORR, interval, state, event and session results with materialized rows
        3. Keep aggregation and interval optimizations for unfiltered bindings
        4. Toggle conditions on one child and verify SET and SET NULL update both rule guards

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Cover filtered super-table optimizer fallback

        """
        self.do_filter_optimizer(backend)

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    def test_filter_own_columns(self, backend):
        """Preserve external filters on virtual-child own columns

        1. Create and add filtered own columns alongside inherited columns and own tags
        2. Replay SHOW CREATE and verify direct and reordered chain queries before and after restart
        3. Retrieve leaf own-column conditions through the super-table RPC and disable aggregate shortcuts
        4. Reject incomplete primary-key conditions and preserve filters across own-column drop and add

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Jing Sima Cover rebased own-column filter paths

        """
        self.do_filter_own_columns(backend)

    @pytest.mark.parametrize("backend", ("postgres",))
    def test_filter_case_sensitive_tables(self, backend):
        """Keep identically filtered PostgreSQL tables with different name case separate

        1. Create two quoted remote table names that differ only in case
        2. Bind normal and child virtual-table columns with identical saved conditions
        3. Verify each binding reads its own table before and after SHOW CREATE replay

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-08 Jing Sima Cover case-sensitive external table grouping

        """
        self.do_filter_case_sensitive_tables(backend)
