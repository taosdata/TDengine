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
"""DISTINCT aggregates inside a stream's external-window sub-query.

A stream whose sub-query aggregates over the [_twstart, _twend) range runs through
streamexternalwindowoperator.c, not the batch external-window operator. That operator kept no
per-result-row function cleanup, so every window's DISTINCT dedup set -- along with its spill pages
and temp file handle -- was abandoned at three points: when a result-row slot was reused (memset
over the set handle), after a row was emitted, and when the result-row array was freed with rows
still unemitted.

Nothing covered this path: the batch distinct suites never build a stream, and the existing
stream external-window suite uses no DISTINCT. These cases assert the values are right and, run
under the ASan build, that the sets are released.

Since: v3.4.2.3
Labels: common
"""

import time

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamDistinctExternalWindow:

    # 2025-01-01 08:00:00 in ms, so windows land on readable boundaries.
    BASE = 1735689600000

    @classmethod
    def setup_class(cls):
        tdLog.info("=== setup: snode + source/target databases ===")
        tdSql.execute("drop database if exists sdw_src")
        tdSql.execute("drop database if exists sdw_out")
        tdStream.ensureSnode(1)
        tdSql.execute("create database sdw_src")
        tdSql.execute("create database sdw_out")
        tdSql.execute(
            "create table sdw_src.stb (ts timestamp, vin varchar(32), v int) tags (t1 int)"
        )
        tdSql.execute("create table sdw_src.t1 using sdw_src.stb tags (1)")
        tdSql.execute("create table sdw_src.t2 using sdw_src.stb tags (2)")

    @classmethod
    def teardown_class(cls):
        for s in ("sdw_count", "sdw_multi"):
            tdSql.execute(f"drop stream if exists sdw_src.{s}")
        tdSql.execute("drop database if exists sdw_src")
        tdSql.execute("drop database if exists sdw_out")

    def _wait_rows(self, table, want, timeout=120):
        """Streams settle asynchronously; poll until the row count stops being short."""
        deadline = time.time() + timeout
        last = -1
        while time.time() < deadline:
            tdSql.query(f"select count(*) from {table}")
            last = tdSql.queryResult[0][0]
            if last >= want:
                return last
            time.sleep(2)
        raise AssertionError(f"{table}: expected >= {want} rows, got {last} after {timeout}s")

    def _wait_window_row(self, table, ts, tag_tbname, columns="*", timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            tdSql.query(
                f"select {columns} from {table} where ts = {ts} and tag_tbname = '{tag_tbname}'"
            )
            if tdSql.queryResult:
                return tdSql.queryResult
            time.sleep(2)
        raise AssertionError(f"{table}: window ts={ts}, tag={tag_tbname} was not emitted")

    def test_stream_external_window_set_functions(self):
        """Sequence and selection results in the stream external-window path.

        Verify CSUM and TOP results from stream subqueries after the window closes.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.3.0

        Labels: common,ci

        History:
            - 2026-09-29 Cover relaxed set functions in stream subqueries.
        """
        b = self.BASE + 300000
        cases = (("csum", "csum(v)", 7), ("top", "top(v, 1)", 4))
        try:
            for name, function, _ in cases:
                tdSql.execute(
                    f"create stream sdw_src.sdw_set_{name} interval(10s) sliding(10s) "
                    f"from sdw_src.stb partition by tbname into sdw_out.r_set_{name} as "
                    f"select _twend as ts, max(result) as result from ("
                    f"select {function} as result from %%tbname "
                    "where _c0 >= _twstart and _c0 < _twend)"
                )
                tdStream.checkStreamStatus(f"sdw_set_{name}")
            tdSql.execute(
                f"insert into sdw_src.t1 values "
                f"({b}, 'S1', 1),({b + 1000}, 'S2', 2),({b + 2000}, 'S3', 4),"
                f"({b + 20000}, 'S4', 8)"
            )
            for name, _, expected in cases:
                rows = self._wait_window_row(
                    f"sdw_out.r_set_{name}", b + 10000, "t1", "result"
                )
                assert rows == [(expected,)], (name, rows)
        finally:
            for name, _, _ in cases:
                tdSql.execute(f"drop stream if exists sdw_src.sdw_set_{name}")

    def test_stream_count_distinct_external_window(self):
        """COUNT(DISTINCT) in a stream external-window sub-query

        Verifies stream count distinct external window.

        Catalog:
            - Stream:SubQuery:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("stream count(distinct) over the external window range")

        # t1 window values {1,1,2} -> 2 distinct; t2 {1,3,3} -> 2 distinct.
        b = self.BASE
        tdSql.execute(
            f"insert into sdw_src.t1 values "
            f"({b}, 'V1', 1),({b + 1000}, 'V1', 1),({b + 2000}, 'V2', 2)"
        )
        tdSql.execute(
            f"insert into sdw_src.t2 values "
            f"({b}, 'V1', 1),({b + 1000}, 'V3', 3),({b + 2000}, 'V3', 3)"
        )

        tdSql.execute(
            "create stream sdw_src.sdw_count interval(10s) sliding(2s) "
            "from sdw_src.stb partition by tbname into sdw_out.r_count as "
            "select ts, cnt from ("
            "  select _twstart as ts, count(distinct v) as cnt from %%tbname "
            "  where _c0 >= _twstart and _c0 < _twend"
            ")"
        )

        tdStream.checkStreamStatus("sdw_count")

        # Later rows advance the watermark so the first window closes and is emitted.
        tdSql.execute(
            f"insert into sdw_src.t1 values "
            f"({b + 10000}, 'V1', 5),({b + 11000}, 'V1', 5),({b + 12000}, 'V2', 6)"
        )
        tdSql.execute(
            f"insert into sdw_src.t2 values "
            f"({b + 10000}, 'V1', 7),({b + 11000}, 'V3', 8),({b + 12000}, 'V3', 8)"
        )

        # Cross-check against the equivalent batch query rather than hardcoded numbers alone, so a
        # stream-only regression cannot hide behind a wrong expectation.
        tdSql.query(
            "select tbname, count(distinct v) from sdw_src.stb "
            f"where ts >= {b + 2000} and ts < {b + 12000} "
            "group by tbname order by tbname"
        )
        expected = {row[0]: row[1] for row in tdSql.queryResult}
        assert expected == {"t1": 2, "t2": 3}, f"batch reference unexpected: {expected}"

        rows = self._wait_window_row("sdw_out.r_count", b + 2000, "t1", "tag_tbname, cnt")
        rows += self._wait_window_row("sdw_out.r_count", b + 2000, "t2", "tag_tbname, cnt")
        got = {row[0]: row[1] for row in rows}
        assert got == expected, \
            f"stream distinct disagrees with batch: stream={got}, batch={expected}"

    def test_stream_multiple_distinct_aggregates(self):
        """Two DISTINCT aggregates on different columns in one stream window

        Verifies stream multiple distinct aggregates.

        Catalog:
            - Stream:SubQuery:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("stream count(distinct) + sum(distinct) in the same window")

        # Each window holds one set per DISTINCT aggregate per result row, so this is the shape that
        # multiplied the leak. Values {1,1,2}: count(distinct)=2, sum(distinct)=3.
        b = self.BASE + 100000
        tdSql.execute(
            f"insert into sdw_src.t1 values "
            f"({b}, 'V1', 1),({b + 1000}, 'V1', 1),({b + 2000}, 'V2', 2)"
        )

        tdSql.execute(
            "create stream sdw_src.sdw_multi interval(10s) sliding(2s) "
            "from sdw_src.stb partition by tbname into sdw_out.r_multi as "
            "select ts, cnt, total, plain from ("
            "  select _twstart as ts, count(distinct v) as cnt, sum(distinct v) as total, "
            "         count(*) as plain "
            "  from %%tbname where _c0 >= _twstart and _c0 < _twend"
            ")"
        )

        tdStream.checkStreamStatus("sdw_multi")

        # Push the watermark well past the window end (b+12000), otherwise the window never closes
        # and nothing is emitted.
        tdSql.execute(
            f"insert into sdw_src.t1 values "
            f"({b + 10000}, 'V1', 9),({b + 11000}, 'V1', 9),"
            f"({b + 13000}, 'V1', 4),({b + 20000}, 'V1', 4)"
        )
        # Derive the expectation from the equivalent batch query for the same range rather than
        # hardcoding it -- the window covers whichever rows fall in [b+2000, b+12000).
        tdSql.query(
            "select count(*), count(distinct v), sum(distinct v) from sdw_src.t1 "
            f"where ts >= {b + 2000} and ts < {b + 12000}"
        )
        exp_plain, exp_cnt, exp_total = tdSql.queryResult[0]

        rows = self._wait_window_row("sdw_out.r_multi", b + 2000, "t1", "cnt, total, plain")
        assert len(rows) == 1, f"expected one row for the closed window, got {len(rows)}"
        cnt, total, plain = rows[0]
        # count(*) must still see every row -- the DISTINCT sets must not filter the shared input.
        assert plain == exp_plain, f"count(*): stream={plain}, batch={exp_plain}"
        assert cnt == exp_cnt, f"count(distinct v): stream={cnt}, batch={exp_cnt}"
        assert float(total) == float(exp_total), \
            f"sum(distinct v): stream={total}, batch={exp_total}"
