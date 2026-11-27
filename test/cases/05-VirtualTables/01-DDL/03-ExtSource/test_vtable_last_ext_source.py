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

from new_test_framework.utils import tdLog, tdSql

from datetime import datetime, timezone
import os
import re
import sys

_VTABLE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _VTABLE_DIR)

from vtable_util import ExtSrcEnv, VtableQueryUtil
from ext_source_helpers import create_ext_source, create_mysql_table, create_remote_db

_DB = "vlast_ext"
_REMOTE_DB = "vlast_ext_remote"
_SRC = "vlast_ext_src"

# The external rows sit a day after the initial local rows, so no timezone
# offset can change which source owns the newest row.
_LOCAL_TS = ("2025-11-01 10:00:00", "2025-11-01 10:00:01")
_EXT_TS = ("2025-11-02 10:00:00", "2025-11-02 10:00:01")
_LOCAL_LATE_TS = "2025-11-03 10:00:00"
_SRC1_LATE_TS = "2025-11-04 10:00:00"
_EXT_LATE_TS = "2025-11-05 10:00:00"


def _ts_ms(text):
    return int(datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


class TestVtableLastExtSource:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        VtableQueryUtil._EXT_SOURCE_ADAPTERS["mysql"]().ensure_running()
        ExtSrcEnv.ensure_qnode()

    def setup_method(self):
        tdSql.query("show local variables like 'timezone'")
        self.original_timezone = str(tdSql.getData(0, 1)).split(" ")[0]
        tdSql.execute('alter local "timezone" "UTC"')
        create_remote_db("mysql", _REMOTE_DB)
        create_mysql_table(
            _REMOTE_DB, "last_ext",
            "ts datetime(3) not null primary key, v int",
            [f"('{_EXT_TS[0]}', 11)", f"('{_EXT_TS[1]}', 22)"],
        )
        create_mysql_table(_REMOTE_DB, "last_ext_empty",
                           "ts datetime(3) not null primary key, v int")
        create_ext_source(_SRC, "mysql", _REMOTE_DB)
        tdSql.execute(f"drop database if exists {_DB}")
        tdSql.execute(f"create database {_DB} precision 'ms'")
        tdSql.execute(f"use {_DB}")
        tdSql.execute("create stable src (ts timestamp, v int) tags (loc nchar(20))")
        for child, tag in (("src0", "local0"), ("src1", "local1"), ("src_empty", "local2")):
            tdSql.execute(f"create table {child} using src tags ('{tag}')")
        tdSql.execute(f"insert into src0 values ({_ts_ms(_LOCAL_TS[0])}, 1) "
                      f"({_ts_ms(_LOCAL_TS[1])}, 2)")
        tdSql.execute(f"insert into src1 values ({_ts_ms(_LOCAL_TS[0])}, 3)")
        tdSql.execute("create stable vst (ts timestamp, x int, y int, e int) "
                      "tags (t nchar(20)) virtual 1")
        tdSql.execute(f"create vtable vc (x from {_DB}.src0.v, y from {_DB}.src1.v, "
                      f"e from {_SRC}.{_REMOTE_DB}.last_ext.v) "
                      f"using vst tags (t from {_DB}.src0.loc)")
        tdSql.execute(f"create vtable vce (x from {_DB}.src_empty.v, y from {_DB}.src1.v, "
                      f"e from {_SRC}.{_REMOTE_DB}.last_ext.v) "
                      f"using vst tags (t from {_DB}.src0.loc)")
        tdSql.execute(f"create vtable vcx (x from {_DB}.src0.v, y from {_DB}.src1.v, "
                      f"e from {_SRC}.{_REMOTE_DB}.last_ext_empty.v) "
                      f"using vst tags (t from {_DB}.src0.loc)")

    def teardown_method(self):
        tdSql.execute(f"drop database if exists {_DB}")
        tdSql.execute(f"drop external source if exists {_SRC}")
        ExtSrcEnv.mysql_drop_db(_REMOTE_DB)
        tdSql.execute(f'alter local "timezone" "{self.original_timezone}"')

    @staticmethod
    def _plan(sql):
        tdSql.query(f"explain verbose true {sql}")
        return [str(row[0]) for row in tdSql.queryResult]

    @staticmethod
    def _nodes(lines, operator):
        nodes = []
        for i, header in enumerate(lines):
            if not header.lstrip().startswith(f"-> {operator}"):
                continue
            output = None
            for detail in lines[i + 1:]:
                if detail.lstrip().startswith("-> "):
                    break
                if detail.lstrip().startswith("Output: columns="):
                    output = detail
                    break
            nodes.append((header, output))
        return nodes

    @staticmethod
    def _node_rows(lines, operator):
        rows = []
        for line in lines:
            if line.lstrip().startswith(f"-> {operator}"):
                match = re.search(r"\brows=([\d.]+)", line)
                if match:
                    rows.append(float(match.group(1)))
        return rows

    def _assert_bound_plan(self, sql, local_scans=2):
        lines = self._plan(sql)
        text = "\n".join(lines)

        vscans = self._nodes(lines, "Virtual Table Scan on ")
        assert len(vscans) == 1, text
        assert "limit=1" in (vscans[0][1] or ""), text

        scans = self._nodes(lines, "Table Scan on ")
        assert len(scans) == local_scans, text
        for header, output in scans:
            assert "order=[asc|0 desc|1]" in header, text
            assert output is not None and "limit=1" in output, text

        assert len(self._nodes(lines, "Federated Scan on ")) == 1, text
        assert "TagRefSource" in text, text
        assert any("ORDER BY" in line and "DESC" in line for line in lines), text

    def _assert_analyzed_rows(self, sql, expected_local_rows):
        tdSql.query(f"explain analyze verbose true {sql}")
        lines = [str(row[0]) for row in tdSql.queryResult]
        assert self._node_rows(lines, "Virtual Table Scan on ") == [1.0], lines
        assert sorted(self._node_rows(lines, "Table Scan on ")) == sorted(expected_local_rows), lines

    @staticmethod
    def _latest_row(sql):
        tdSql.query(sql)
        tdSql.checkRows(1)
        return int(str(tdSql.getData(0, 0))), str(tdSql.getData(0, 1))

    def test_last_with_external_and_local_sources(self):
        """Plan: LAST(primary timestamp) with referenced tag over local and external sources

        1. Check the plan bounds every local scan and orders the remote SQL by primary key DESC
        2. Check the merged result follows the newest source in four different orders
        3. Check a timestamp tie between a local and an external row keeps one correct row
        4. Check the LAST(tag) fallback keeps the unrestricted path

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-06 wpan Add local + external LAST bound scan coverage
        """

        sql = f"select cast(last(_c0) as bigint), t from {_DB}.vc"
        self._assert_bound_plan(sql)
        self._assert_analyzed_rows(sql, [1, 1])

        # Each source on its own: the row domain follows the selected column.
        ext_ts = self._latest_row(f"select cast(last(_c0) as bigint), e from {_DB}.vc")[0]
        src0_ts = self._latest_row(f"select cast(last(_c0) as bigint), x from {_DB}.vc")[0]
        src1_ts = self._latest_row(f"select cast(last(_c0) as bigint), y from {_DB}.vc")[0]
        merged_ts, tag = self._latest_row(sql)
        assert merged_ts == max(ext_ts, src0_ts, src1_ts), (merged_ts, ext_ts, src0_ts, src1_ts)
        assert tag == "local0"

        # Local src0 owns the newest row.
        late_ts = _ts_ms(_LOCAL_LATE_TS)
        tdSql.execute(f"insert into {_DB}.src0 values ({late_ts}, 9)")
        self._assert_bound_plan(sql)
        assert self._latest_row(sql) == (late_ts, "local0")

        # Local src1 owns the newest row.
        src1_late_ts = _ts_ms(_SRC1_LATE_TS)
        tdSql.execute(f"insert into {_DB}.src1 values ({src1_late_ts}, 7)")
        self._assert_bound_plan(sql)
        assert self._latest_row(sql) == (src1_late_ts, "local0")

        # The external source owns the newest row again.
        ExtSrcEnv.mysql_exec(_REMOTE_DB, [
            f"INSERT INTO last_ext VALUES ('{_EXT_LATE_TS}', 55)"])
        ext_late_ts = self._latest_row(f"select cast(last(_c0) as bigint), e from {_DB}.vc")[0]
        self._assert_bound_plan(sql)
        assert self._latest_row(sql) == (ext_late_ts, "local0")

        # A tie between a local and an external row still yields one row.
        tdSql.execute(f"insert into {_DB}.src0 values ({ext_late_ts}, 8)")
        merged_ts, tag = self._latest_row(sql)
        assert merged_ts == ext_late_ts, (merged_ts, ext_late_ts)
        assert tag == "local0"

        # LAST over the referenced tag keeps the unrestricted fallback.
        fallback = f"select cast(last(_c0) as bigint), last(t) from {_DB}.vc"
        fallback_text = "\n".join(self._plan(fallback))
        assert "Virtual Table Scan" in fallback_text, fallback_text
        assert "limit=1" not in fallback_text, fallback_text
        assert self._latest_row(fallback) == (ext_late_ts, "local0")

    def test_last_with_empty_sources(self):
        """Plan: LAST(primary timestamp) with referenced tag tolerates empty source streams

        1. Check an empty local source does not break the merge or the bound scan
        2. Check an empty external source does not break the merge or the bound scan
        3. Check each result follows the newest non-empty source

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-06 wpan Add empty local/external LAST bound scan coverage
        """

        # Empty local source: the other local source and the external source merge.
        sql = f"select cast(last(_c0) as bigint), t from {_DB}.vce"
        self._assert_bound_plan(sql)
        self._assert_analyzed_rows(sql, [0, 1])
        ext_ts = self._latest_row(f"select cast(last(_c0) as bigint), e from {_DB}.vce")[0]
        assert self._latest_row(sql) == (ext_ts, "local0")

        # Empty external source: the two local sources still merge to the newest row.
        sql = f"select cast(last(_c0) as bigint), t from {_DB}.vcx"
        self._assert_bound_plan(sql)
        self._assert_analyzed_rows(sql, [1, 1])
        src0_ts = self._latest_row(f"select cast(last(_c0) as bigint), x from {_DB}.vcx")[0]
        assert self._latest_row(sql) == (src0_ts, "local0")
