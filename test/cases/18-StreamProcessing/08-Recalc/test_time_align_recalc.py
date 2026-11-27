# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import re
import time
from pathlib import Path

import pytest

from new_test_framework.utils import tdDnodes, tdSql, tdStream


class TestTimeAlignRecalc:
    updatecfgDict = {"stDebugFlag": 135}

    @classmethod
    def setup_class(cls):
        tdStream.ensureSnode(1)

    def _stream_names(self, db):
        tdSql.query(
            "select stream_name, stream_id from information_schema.ins_streams "
            f"where db_name='{db}'"
        )
        return {int(str(uid), 16): name for name, uid in tdSql.queryResult}

    def _named_log_lines(self, names):
        for path in (Path(tdDnodes.getDnodeDir(1)) / "log").glob("taosdlog.*"):
            with path.open(encoding="utf-8", errors="replace") as log:
                for line in log:
                    match = re.search(r"STREAM:([0-9a-f]+),", line)
                    name = names.get(int(match[1], 16)) if match else None
                    if name:
                        yield name, line

    def _history_reads(self, db):
        names = self._stream_names(db)
        reads = {name: [0, 0] for name in names.values()}
        stops = {name: [] for name in names.values()}
        for name, line in self._named_log_lines(names):
            reads[name][0] += "pull tsdb meta from" in line
            reads[name][1] += "pull data request for table:" in line
            if "replay complete, gid:" in line:
                stops[name].append(line)
        return reads, stops

    def test_mutation_matrix(self):
        """Recount a fixed window as separate sources add or remove timestamp contributors.

        1. At the same timestamp both sources contribute, while a second timestamp has one row.
        2. Remove the sole row, then remove the two contributors at the shared timestamp.
        3. Insert a late NULL-only row and verify that its timestamp reopens the empty window.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        db = "ta_recalc_contributors"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        base = (int(time.time()) // 60 - 2) * 60000
        tdSql.execute("create table samples (ts timestamp, temp int)")
        tdSql.execute("create table beats (ts timestamp, beat int)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from samples.temp, beat int from beats.beat)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({base}, 1)")
        source = "table(time_align(table meter, false, 'temp:PREV:_p;beat:PREV:_p'))"
        name = "contributors"

        def expect(offsets):
            lo, hi = base + 10000, base + 20000
            tdSql.query(f"select cast(ts as bigint) from {source} where ts >= {lo} and ts < {hi} order by ts")
            assert [row[0] for row in tdSql.queryResult] == [base + x * 1000 for x in offsets]
            tdSql.checkResultsByFunc(
                sql=f"select n from {name}_out where ws={lo}",
                func=lambda: list(tdSql.queryResult) == [(len(offsets),)],
                retry=45,
            )

        tdSql.execute(
            f"create stream {name} interval(10s) from {source} stream_options(delete_recalc) "
            f"into {name}_out as select _twstart as ws, _twrownum as n from probe"
        )
        try:
            tdStream.checkStreamStatus(name)
            tdSql.execute(f"insert into samples values ({base}, 80) ({base + 11000}, 90)")
            tdSql.execute(f"insert into beats values ({base + 11000}, 1) ({base + 12000}, 2) ({base + 31000}, 3)")
            expect([11, 12])
            tdSql.execute(f"delete from beats where ts={base + 12000}")
            expect([11])
            tdSql.execute(f"delete from samples where ts={base + 11000}")
            expect([11])
            tdSql.execute(f"delete from beats where ts={base + 11000}")
            expect([])
            tdSql.execute(f"insert into samples values ({base + 13000}, NULL)")
            expect([13])
        finally:
            tdSql.execute(f"drop stream if exists {name}")

    def test_recalc_policy_boundaries(self):
        """Apply admitted deletes to every later window that used the changed PREV.

        1. A low reading excludes four later heartbeat rows from two windows.
        2. After a newer high reading closes a window, delete the low reading.
        3. Check both newly eligible windows and a later unchanged window with
           delete recalculation enabled and disabled.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        tdSql.prepare(dbname="ta_recalc_policy", drop=True, vgroups=1)
        tdSql.execute("create table readings (ts timestamp, temperature int, beat int)")
        tdSql.execute(
            "create vtable meter (ts timestamp, temp int from readings.temperature, "
            "beat int from readings.beat)"
        )
        tdSql.execute("create table probe (ts timestamp, v int)")
        base = (int(time.time()) // 60 - 2) * 60000
        tdSql.execute(f"insert into probe values ({base}, 1)")

        def check_windows(name, windows):
            expected = [(base + start * 1000, count) for start, count in windows]
            tdSql.checkResultsByFunc(
                sql=(f"select cast(ws as bigint), n from {name}_out "
                     f"where ws >= {base} and ws < {base + 40000} order by ws"),
                func=lambda: list(tdSql.queryResult) == expected,
                retry=40,
            )

        names = []
        try:
            for name, options in (("apply_delete", "delete_recalc|"), ("skip_delete", "")):
                tdSql.execute(
                    f"create stream {name} interval(10s) from "
                    "table(time_align(table meter, true, 'temp:PREV:_p')) "
                    f"stream_options({options}pre_filter(beat=1 and temp_p>=100)) "
                    f"into {name}_out as select _twstart as ws, _twrownum as n from probe"
                )
                names.append(name)
                tdStream.checkStreamStatus(name)
            rows = [(0, 100, "null"), (1, 40, "null"), (11, "null", 1), (12, "null", 1),
                    (21, "null", 1), (22, "null", 1), (30, 100, 1), (41, "null", 1)]
            values = " ".join(f"({base + s * 1000}, {temp}, {beat})" for s, temp, beat in rows)
            tdSql.execute(f"insert into readings values {values}")
            initial = [(0, 0), (10, 0), (20, 0), (30, 1)]
            for name in names:
                check_windows(name, initial)

            tdSql.execute(f"delete from readings where ts={base + 1000}")
            tdSql.execute(f"insert into readings values ({base + 51000}, null, 1)")
            for name in names:
                tdSql.checkResultsByFunc(
                    sql=f"select n from {name}_out where ws={base + 40000}",
                    func=lambda: list(tdSql.queryResult) == [(1,)],
                    retry=40,
                )
            check_windows("apply_delete", [(0, 0), (10, 2), (20, 2), (30, 1)])
            check_windows("skip_delete", initial)
        finally:
            for name in reversed(names):
                tdSql.execute(f"drop stream if exists {name}")

    def test_interval_repair_to_empty(self):
        """Replace a positive interval count with zero when both rows become ineligible.

        1. Close eight ten-second windows, including one with two qualifying rows.
        2. Correct the reading at second 11 and read the full aligned time series with SQL.
        3. Compare the automatic repair and a fresh history run with the SQL counts.
        4. With empty-window triggers disabled, check that fresh history skips the key.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        tdSql.prepare(dbname="ta_recalc_empty", drop=True, vgroups=1)
        tdSql.execute("create table readings (ts timestamp, temp int, beat int)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from readings.temp, beat int from readings.beat)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        base = (int(time.time()) // 60 - 3) * 60000
        tdSql.execute(f"insert into probe values ({base}, 1)")
        source = "table(time_align(table meter, true, 'temp:PREV:_p'))"
        names = []

        def start(name, options=""):
            tdSql.execute(
                f"create stream {name} interval(10s) from {source} "
                f"stream_options({options}pre_filter(temp_p > 50)) into {name}_out "
                "as select _twstart as ws, _twrownum as n from probe"
            )
            names.append(name)
            tdStream.checkStreamStatus(name)

        def check(name, expected):
            tdSql.checkResultsByFunc(
                sql=(f"select cast(ws as bigint), n from {name}_out "
                     f"where ws >= {base} and ws < {base + 80000} order by ws"),
                func=lambda: list(tdSql.queryResult) == expected,
                retry=40,
            )

        try:
            start("updated")
            start("skip_empty", "ignore_nodata_trigger|")
            ticks = [(0, 100), (11, 100), (12, "null"), (30, 0), (80, 100)]
            rows = " ".join(f"({base + ts * 1000}, {value}, 1)" for ts, value in ticks)
            tdSql.execute(f"insert into readings values {rows}")
            starts = range(base, base + 80000, 10000)
            initial = list(zip(starts, (1, 2, 0, 0, 0, 0, 0, 0)))
            check("updated", initial)
            check("skip_empty", initial[:2])

            tdSql.execute(f"insert into readings values ({base + 11000}, 0, 1)")
            tdSql.query(f"select cast(ts as bigint) from {source} where temp_p > 50 order by ts")
            qualifying = [row[0] for row in tdSql.queryResult]
            assert qualifying == [base, base + 80000]
            expected = [(ws, sum(ws <= ts < ws + 10000 for ts in qualifying)) for ws in starts]
            start("reference", f"fill_history({base})|")
            start("reference_skip", f"fill_history({base})|ignore_nodata_trigger|")
            check("reference", expected)
            check("reference_skip", expected[:1])
            check("updated", expected)
            check("skip_empty", initial[:2])
        finally:
            for name in reversed(names):
                tdSql.execute(f"drop stream if exists {name}")

    def test_interval_repair_inside_one_trigger_tick(self):
        """Recalculate an interval whose first two source rows share a trigger timestamp.

        1. On a millisecond stream, close a ten-second window with two eligible
           nanosecond rows just after its start.
        2. Correct the preceding window's reading. The first row inherits the
           correction and the second row supplies its own value.
        3. Check the repaired row counts in both windows.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-23 OpenAI Created
        """
        db = "ta_recalc_same_tick"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        src = db + "_ns"
        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src} vgroups 1 precision 'ns'")
        tdSql.execute(f"create table {src}.readings (ts timestamp, temp int)")
        tdSql.execute(f"create vtable meter (ts timestamp, temp int from {src}.readings.temp)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        base = (int(time.time()) // 60 - 2) * 60000
        ns = base * 1_000_000
        corrected = ns + 5_000_000_000
        boundary = ns + 10_000_000_000
        tdSql.execute(f"insert into probe values ({base}, 1)")
        tdSql.execute(
            "create stream updated interval(10s) from table(time_align(table meter, true, 'temp:PREV:_p')) "
            "stream_options(pre_filter(temp_p>50)) into updated_out as select _twstart as ws, _twrownum as n from probe"
        )

        def check(first, second):
            tdSql.checkResultsByFunc(
                sql=(f"select cast(ws as bigint), n from updated_out "
                     f"where ws between {base} and {base + 10000} order by ws"),
                func=lambda: list(tdSql.queryResult) == [(base, first), (base + 10000, second)],
                retry=40,
            )

        try:
            tdStream.checkStreamStatus("updated")
            tdSql.execute(f"insert into {src}.readings values ({ns}, 100) ({corrected}, 100) "
                          f"({boundary + 1}, null) ({boundary + 2}, 100) ({ns + 21_000_000_000}, 100)")
            check(2, 2)
            tdSql.execute(f"insert into {src}.readings values ({corrected}, 0)")
            check(1, 1)
        finally:
            tdSql.execute("drop stream if exists updated")

    def test_interval_prefix_probe(self):
        """Bound fixed-window repair reads when the preceding interval has qualifying input.

        1. Place two positive days late in an eighty-day history.
        2. Make both days empty, first with and then without a qualifying previous day.
        3. Check both zero counts and compare metadata pulls for the two repairs.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        tdSql.prepare(dbname="ta_interval_prefix", drop=True, vgroups=1)
        tdSql.execute("create table readings (ts timestamp, temp int, beat int)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from readings.temp, beat int from readings.beat)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        day = 86400000
        base = (int(time.time()) // 86400 - 90) * day
        tdSql.execute(f"insert into probe values ({base}, 1)")
        tdSql.execute(
            "create stream updated interval(1d) from table(time_align(table meter, true, 'temp:PREV:_p')) "
            "stream_options(config(timezone='UTC')|pre_filter(temp_p>50)) into updated_out "
            "as select _twstart as ws, _twrownum as n from probe"
        )

        def check(d, n):
            tdSql.checkResultsByFunc(
                sql=f"select n from updated_out where ws={base + d * day}",
                func=lambda: list(tdSql.queryResult) == [(n,)], retry=40,
            )

        try:
            tdStream.checkStreamStatus("updated")
            ticks = [(0, 100), (29, 0), (30, 100), (31, 0), (60, 100), (61, 100), (62, 0), (80, 100)]
            rows = " ".join(f"({base + d * day + 1000}, {temp}, 1)" for d, temp in ticks)
            tdSql.execute(f"insert into readings values {rows}")
            check(30, 1)
            check(61, 1)
            tdSql.execute(f"insert into readings values ({base + 61 * day + 1000}, 0, 1)")
            check(61, 0)
            dense, _ = self._history_reads("ta_interval_prefix")
            tdSql.execute(f"insert into readings values ({base + 30 * day + 1000}, 0, 1)")
            check(30, 0)
            combined, _ = self._history_reads("ta_interval_prefix")
            dense_meta, dense_body = dense["updated"]
            gap_meta = combined["updated"][0] - dense_meta
            gap_body = combined["updated"][1] - dense_body
            assert dense_meta == 2 < gap_meta, (dense_meta, gap_meta)
            assert 0 < dense_body < gap_body, (dense_body, gap_body)
        finally:
            tdSql.execute("drop stream if exists updated")

    def test_nested_state_uses_selected_aligned_column(self):
        """Bind a nested STATE leaf to the requested PREV output across sources.

        1. Select a raw temperature, two aligned temperatures, and a separate heartbeat.
        2. Compare the first closed leaf of realtime and fresh history with ordinary STATE SQL.
        3. Verify that a leaf explicitly reading the raw temperature keeps its own slot.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        tdSql.prepare(dbname="ta_nested_state_slot", drop=True, vgroups=1)
        tdSql.execute("create table readings (ts timestamp, v int)")
        tdSql.execute("create table ticks (ts timestamp, v int)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from readings.v, beat int from ticks.v)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        base = (int(time.time()) // 120 - 3) * 120000
        tdSql.execute(f"insert into probe values ({base}, 1)")
        source = "table(time_align(table meter, true, 'temp:PREV(1s):_short;temp:PREV:_p;beat:PREV:_p'))"
        names = []

        def start(name, column, options=""):
            tdSql.execute(
                f"create stream {name} window(interval(120s) as parent_win, state_window({column}) as leaf_win) "
                f"from {source} stream_options({options}pre_filter(beat_p=1)) into {name}_out as select "
                "leaf_win._twstart as ws, leaf_win._twend as we, leaf_win._twrownum as n from probe"
            )
            names.append(name)
            tdStream.checkStreamStatus(name)

        def check(name, expected):
            tdSql.checkResultsByFunc(
                sql=f"select cast(we as bigint), n from {name}_out where ws={base}",
                func=lambda: list(tdSql.queryResult) == expected, retry=30,
            )

        try:
            start("live_state", "temp_p")
            start("raw_state", "temp")
            tdSql.execute(
                f"insert into readings values ({base}, 100) ({base + 10000}, 0) ({base + 20000}, 100) "
                f"ticks values ({base}, 1) ({base + 5000}, 1) ({base + 11000}, 1)"
            )
            tdSql.query(f"select temp, temp_short, temp_p, beat_p from {source} order by ts")
            assert list(tdSql.queryResult)[1] == (None, None, 100, 1)
            tdSql.query(
                f"select cast(_wend as bigint), count(*) from {source} where beat_p=1 "
                "state_window(temp_p) order by _wstart"
            )
            expected = list(tdSql.queryResult)[:1]
            assert expected == [(base + 5000, 2)]
            start("history_state", "temp_p", f"fill_history({base})|")
            check("history_state", expected)
            check("live_state", expected)
            check("raw_state", [(base, 1)])
        finally:
            for name in reversed(names):
                tdSql.execute(f"drop stream if exists {name}")

    def test_nested_history_uses_trigger_slots(self):
        """Replay a nested EVENT whose raw filter changes trigger column order.

        1. Filter a temperature row using the raw heartbeat and carry its value into later rows.
        2. Open and close a nested EVENT using an aligned predicate in a historical SESSION.
        3. Check the timestamp, size and parent start of the completed leaf.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        tdSql.prepare(dbname="ta_nested_history_slots", drop=True, vgroups=1)
        tdSql.execute("create table readings (ts timestamp, temp int, beat int)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from readings.temp, beat int from readings.beat)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        base = (int(time.time()) - 60) * 1000
        tdSql.execute(f"insert into probe values ({base}, 1)")
        tdSql.execute(f"insert into readings values ({base},100,0) ({base+2000},null,1) "
                      f"({base+5000},null,1) ({base+8000},0,1)")
        tdSql.execute("create stream replay_slots window(session(ts,120s) as parent_win, "
                      "event_window(start with (temp_p=100 or temp_p=101) end with temp_p=0) as leaf_win) "
                      "from table(time_align(table meter,true,'temp:PREV:_p')) "
                      f"stream_options(fill_history({base})|pre_filter(beat=1)) into replay_slots_out as select "
                      "leaf_win._twstart as ws, leaf_win._twend as we, leaf_win._twrownum as n, "
                      "parent_win._twstart as parent_ws from probe")
        try:
            tdStream.checkStreamStatus("replay_slots")
            tdSql.checkResultsByFunc(
                sql="select cast(ws as bigint),cast(we as bigint),n,cast(parent_ws as bigint) from replay_slots_out",
                func=lambda: list(tdSql.queryResult) == [(base+2000, base+8000, 3, base+2000)],
                retry=30,
            )
        finally:
            tdSql.execute("drop stream if exists replay_slots")

    def test_nested_event_materialized_history(self):
        """Evaluate nested EVENT predicates on the historical physical-table scan.

        1. Materialize a start, a continuation, and an end into a regular table.
        2. Compare the first FILL_HISTORY leaf with the ordinary EVENT query.

        Catalog:
            - Streams:Trigger:NestedWindow

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        tdSql.prepare(dbname="ta_materialized_event", drop=True, vgroups=1)
        tdSql.execute("create table materialized (ts timestamp, temperature int, beat int)")
        tdSql.execute("create table probe (ts timestamp, val int)")
        base = (int(time.time()) // 120 - 3) * 120000
        tdSql.execute(f"insert into probe values ({base}, 1)")
        tdSql.execute(f"insert into materialized values ({base}, 100, 1) ({base + 5000}, 100, 1) "
                      f"({base + 10000}, 0, 1)")
        tdSql.query("select cast(_wend as bigint), count(*) from materialized where beat=1 "
                    "event_window start with temperature=100 end with temperature=0")
        expected = list(tdSql.queryResult)
        assert expected == [(base + 10000, 3)]
        tdSql.execute("create stream plain_event window(interval(120s) as parent_win, "
                      "event_window(start with temperature=100 end with temperature=0) as leaf_win) "
                      f"from materialized stream_options(fill_history({base})|pre_filter(beat=1)) "
                      "into plain_out as select leaf_win._twstart as ws, leaf_win._twend as we, "
                      "leaf_win._twrownum as n from probe")
        try:
            tdStream.checkStreamStatus("plain_event")
            tdSql.checkResultsByFunc(sql=f"select cast(we as bigint), n from plain_out where ws={base}",
                                     func=lambda: list(tdSql.queryResult) == expected, retry=30)
        finally:
            tdSql.execute("drop stream if exists plain_event")

    @pytest.mark.parametrize("leaf", ["event", "state", "state_expr"])
    def test_nested_leaf_replay_and_future_input(self, leaf):
        """Keep nested leaf predicates and the open leaf correct across two corrections.

        1. Close a leaf with values from two physical sources inside one fixed parent.
        2. Correct its input and compare the closed result with SQL and fresh history.
        3. Correct the open tail and compare two subsequent closures with window SQL.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        db = f"ta_nested_{leaf}_replay"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        tdSql.execute("create table therm (ts timestamp, val int)")
        tdSql.execute("create table ticks (ts timestamp, beat int)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from therm.val, beat int from ticks.beat)")
        tdSql.execute("create table probe (ts timestamp, val int)")
        base = (int(time.time()) // 120 - 3) * 120000
        tdSql.execute(f"insert into probe values ({base}, 1)")
        source = "table(time_align(table meter, true, 'temp:PREV:_p;beat:PREV:_p'))"
        is_state = leaf != "event"
        win = {"state": "state_window(temp_p)", "state_expr": "state_window(cast(temp_p as bigint))",
               "event": "event_window(start with temp_p=100 end with temp_p=0)"}[leaf]
        query_win = win if is_state else "event_window start with temp_p=100 end with temp_p=0"
        names = []

        def start(name, history=""):
            tdSql.execute(
                f"create stream {name} window(interval(120s) as parent_win, {win} as leaf_win) "
                f"from {source} stream_options({history}pre_filter(beat_p=1)) into {name}_out as select "
                "leaf_win._twstart as ws, leaf_win._twend as we, leaf_win._twrownum as n, "
                "parent_win._twstart as parent_ws from probe"
            )
            names.append(name)
            tdStream.checkStreamStatus(name)

        def write(values, ticks=()):
            rows = " ".join(f"({base + second * 1000}, {val})" for second, val in values)
            sql = f"insert into therm values {rows}"
            if ticks:
                rows = " ".join(f"({base + second * 1000}, 1)" for second in ticks)
                sql += f" ticks values {rows}"
            tdSql.execute(sql)

        def check(name, start_second, end_second, count):
            ws, we = base + start_second * 1000, base + end_second * 1000
            parent_ws = base + start_second // 120 * 120000
            tdSql.query(f"select cast(_wstart as bigint), cast(_wend as bigint), count(*) "
                        f"from {source} where beat_p=1 {query_win} order by _wstart")
            assert (ws, we, count) in tdSql.queryResult, tdSql.queryResult
            tdSql.checkResultsByFunc(
                sql=f"select cast(we as bigint), n, cast(parent_ws as bigint) from {name}_out where ws={ws}",
                func=lambda: list(tdSql.queryResult) == [(we, count, parent_ws)], retry=40,
            )

        try:
            start("live")
            write([(0, 100), (10, 0), (20, 100), (25, 0), (30, 100)], [0, 5, 11, 15, 21, 35])
            check("live", 0, 5 if is_state else 10, 2 if is_state else 3)
            write([(10, 100)])
            start("ref", f"fill_history({base})|")
            first_end, first_count = (21, 7) if is_state else (25, 8)
            check("ref", 0, first_end, first_count)
            check("live", 0, first_end, first_count)
            write([(30, 0)])
            # The correction must be admitted at H=35 before the row at 40 arrives.
            ids = self._stream_names(db)
            expected_range = f"calcRange: [{base + 30000}, {base + 35000}]"
            for _ in range(40):
                if any(name == "live" and "add recalc request," in line and expected_range in line
                       for name, line in self._named_log_lines(ids)):
                    break
                time.sleep(1)
            else:
                pytest.fail(f"live did not admit the second correction at {expected_range}")
            write([(40, 100)])
            if is_state:
                check("live", 25, 35, 3)
            write([(50, 0)], [45])
            check("live", 40, 45 if is_state else 50, 2 if is_state else 3)
            write([(60, 100), (70, 0)], [65])
            check("live", 60, 65 if is_state else 70, 2 if is_state else 3)
            write([(120, 100), (130, 0)], [125])
            check("live", 120, 125 if is_state else 130, 2 if is_state else 3)
            tdSql.query(f"select ws from live_out where ws={base + 30000}")
            tdSql.checkRows(0)
        finally:
            for name in reversed(names):
                tdSql.execute(f"drop stream if exists {name}")

    @pytest.mark.parametrize("previous_window", [False, True])
    def test_early_stop_matches_full_replay(self, previous_window):
        """Split the updated SESSION and compare its closed windows with a fresh replay.

        1. Build a five-second SESSION using aligned temperature and heartbeat rows.
        2. Replace the temperature at second 10 with zero after the old session closes.
        3. Compare the split windows and the successor with a fresh stream.
        4. When the output includes the previous start, check that the successor gets the new key.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        tdSql.prepare(dbname="ta_recalc_session", drop=True, vgroups=1)
        tdSql.execute("create table samples (ts timestamp, temperature int, beat int)")
        tdSql.execute("create table air (ts timestamp, humidity int)")
        tdSql.execute(
            "create vtable meter (ts timestamp, temp int from samples.temperature, "
            "beat int from samples.beat, humidity int from air.humidity)"
        )
        tdSql.execute("create table probe (ts timestamp, v int)")
        day = 86400000
        base = (int(time.time()) // 864000 - 3) * (10 * day) + day
        tdSql.execute(f"insert into probe values ({base}, 1)")
        names = []

        previous_column = ", _tpre_wstart as ps" if previous_window else ""

        def start(name, history=""):
            tdSql.execute(
                f"create stream {name} session(ts,5s) from "
                "table(time_align(table meter, true, 'temp:PREV:_p;humidity:PREV:_p')) "
                f"stream_options({history}pre_filter(temp_p > 50 and humidity_p is null)) "
                f"into {name}_out as select _twstart as ws, _twend as we, "
                f"_twrownum as n{previous_column} from probe"
            )
            names.append(name)
            tdStream.checkStreamStatus(name)

        def check(name, windows):
            expected = [(base + a * 1000, base + b * 1000, count) for a, b, count in windows]
            tdSql.checkResultsByFunc(
                sql=(f"select cast(ws as bigint), cast(we as bigint), n from {name}_out "
                     f"where ws >= {base} and ws < {base + 70000} order by ws"),
                func=lambda: list(tdSql.queryResult) == expected,
                retry=40,
            )

        try:
            start("updated")
            times = (0, 8, 10, 12, 14, 18, 20, 24, 28, 32, 36, 40, 41, 42, 60, 61, 70, 11 * 86400, 21 * 86400)
            rows = " ".join(
                f"({base + ts * 1000}, 100, null)" if ts in (0, 10, 40)
                else f"({base + ts * 1000}, null, 1)" for ts in times
            )
            tdSql.execute(f"insert into samples values {rows}")
            check("updated", [(0, 0, 1), (8, 42, 13), (60, 61, 2)])
            tdSql.execute(f"insert into samples values ({base + 10000}, 0, null)")
            start("reference", f"fill_history({base})|")
            expected = [(0, 0, 1), (8, 8, 1), (40, 42, 3), (60, 61, 2)]
            check("reference", expected)
            check("updated", expected)

            if previous_window:
                for name in ("reference", "updated"):
                    tdSql.checkResultsByFunc(
                        sql=f"select cast(ps as bigint) from {name}_out where ws={base + 60000}",
                        func=lambda: list(tdSql.queryResult) == [(base + 40000,)],
                        retry=40,
                    )
            else:
                reads, stops = self._history_reads("ta_recalc_session")
                assert reads == {"updated": [1, 1], "reference": [3, 3]}, reads
                proofs = stops["updated"]
                assert len(proofs) == 1
                assert f"anchor:{base + 8000}, stable:{base + 40000}, stop:{base + 60000}" in proofs[0]
        finally:
            for name in reversed(names):
                tdSql.execute(f"drop stream if exists {name}")

    def test_no_early_stop_keeps_future_repair(self):
        """Repair an open SESSION after stable input continues without a shared gap.

        1. Keep the section after the unchanged reading at second 40 open.
        2. Replace the reading at second 10 after the stream has observed the first session.
        3. Add a future heartbeat that closes the tail and check the split results.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        tdSql.prepare(dbname="ta_recalc_open", drop=True, vgroups=1)
        tdSql.execute("create table samples (ts timestamp, temperature int, beat int)")
        tdSql.execute(
            "create vtable meter (ts timestamp, temp int from samples.temperature, "
            "beat int from samples.beat)"
        )
        tdSql.execute("create table probe (ts timestamp, v int)")
        base = (int(time.time()) // 60 - 3) * 60000
        tdSql.execute(f"insert into probe values ({base}, 1)")
        tdSql.execute(
            "create stream open_session session(ts,5s) from "
            "table(time_align(table meter, true, 'temp:PREV:_p')) "
            "stream_options(pre_filter(temp_p > 50)) into session_out "
            "as select _twstart as ws, _twend as we, _twrownum as n from probe"
        )
        try:
            tdStream.checkStreamStatus("open_session")
            ticks = (0, 8, 10, 12, 14, 18, 20, 24, 28, 32, 36, 40, 41, 42, *range(45, 85, 3))
            rows = " ".join(
                f"({base + ts * 1000}, 100, null)" if ts in (0, 10, 40)
                else f"({base + ts * 1000}, null, 1)" for ts in ticks
            )
            tdSql.execute(f"insert into samples values {rows}")
            sql = ("select cast(ws as bigint), cast(we as bigint), n "
                   "from session_out order by ws")
            tdSql.checkResultsByFunc(
                sql=sql, func=lambda: list(tdSql.queryResult) == [(base, base, 1)], retry=40,
            )
            tdSql.execute(f"insert into samples values ({base + 10000}, 0, null)")
            tdSql.execute(f"insert into samples values ({base + 100000}, null, 1)")
            expected = [(base, base, 1), (base + 8000, base + 8000, 1),
                        (base + 40000, base + 84000, 17)]
            tdSql.checkResultsByFunc(
                sql=sql, func=lambda: list(tdSql.queryResult) == expected, retry=40,
            )
        finally:
            tdSql.execute("drop stream if exists open_session")

    @pytest.mark.parametrize("count_window", [False, True], ids=["session", "count"])
    def test_delete_starts_near_qualifying_prefix(self, count_window):
        """Check the repaired windows and the Reader ranges for a delete late in a long history.

        Delete a qualifying row after sixty days of input. Compare the repaired
        SESSION and COUNT windows and check how far the Reader went back.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        db = "ta_delete_prefix_count" if count_window else "ta_delete_prefix_session"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        tdSql.execute("create table samples (ts timestamp, temp int, beat int)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from samples.temp, beat int from samples.beat)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        day = 86400000
        base = (int(time.time()) // 86400 - 2) * day
        tdSql.execute(f"insert into probe values ({base}, 1)")
        window = "count_window(3,1)" if count_window else "session(ts,5s)"
        tdSql.execute(
            f"create stream updated {window} from table(time_align(table meter, true, 'temp:PREV:_p')) "
            "stream_options(delete_recalc|pre_filter(temp_p>50)) into updated_out as select "
            "_twstart as ws, _twend as we, _twrownum as n from probe"
        )

        def check(windows):
            keys = ",".join(str(base + start * 1000) for start, _, _ in windows)
            expected = [(base + start * 1000, base + end * 1000, n) for start, end, n in windows]
            tdSql.checkResultsByFunc(
                sql=f"select cast(ws as bigint), cast(we as bigint), n from updated_out where ws in ({keys}) order by ws",
                func=lambda: list(tdSql.queryResult) == expected, retry=40,
            )

        try:
            tdStream.checkStreamStatus("updated")
            ticks = [(-60 * day, 100), (-30 * day, 0), (-day, None),
                     (0, 100), (3000, None), (6000, None), (10000, None), (12000, None),
                     (14000, None), (40000, 100), (41000, None), (60000, 100), (90000, 100)]
            values = " ".join(f"({base + offset},{temp if temp is not None else 'null'},1)" for offset, temp in ticks)
            tdSql.execute(f"insert into samples values {values}")
            check([(3, 10, 3), (6, 12, 3)] if count_window else [(0, 14, 6)])
            names = self._stream_names(db)
            before = sum("pull tsdb meta from" in line for _, line in self._named_log_lines(names))
            tdSql.execute(f"delete from samples where ts={base + 10000}")
            check([(3, 12, 3), (6, 14, 3)] if count_window else [(0, 6, 3), (12, 14, 2)])
            ranges = []
            for _, line in self._named_log_lines(names):
                match = re.search(r"pull tsdb meta from .*time range:\[(-?\d+), (-?\d+)\]", line)
                if match:
                    ranges.append((int(match[1]), int(match[2])))
            reads = ranges[before:]
            assert len(reads) >= 2, reads
            assert reads[0][1] == base + 9999, reads
            assert min(start for start, _ in reads) >= base - 10 * day, reads
        finally:
            tdSql.execute("drop stream if exists updated")

    def test_count_stable_prefix_evicts_influence(self):
        """Stop COUNT(n,1) after replacing its members and previous-window context.

        1. Build count windows over a positive temperature carried through sparse heartbeats.
        2. Replace the temperature at second 10 and verify new membership and predecessors.
        3. Check the number of source requests through a 21-day input horizon.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        tdSql.prepare(dbname="ta_recalc_count", drop=True, vgroups=1)
        tdSql.execute("create table samples (ts timestamp, temperature int, beat int)")
        tdSql.execute(
            "create vtable meter (ts timestamp, temp int from samples.temperature, "
            "beat int from samples.beat)"
        )
        tdSql.execute("create table probe (ts timestamp, v int)")
        day = 86400000
        base = (int(time.time()) // 864000 - 3) * (10 * day) + day
        tdSql.execute(f"insert into probe values ({base}, 1)")
        tdSql.execute(
            "create stream aligned_count count_window(3,1) from "
            "table(time_align(table meter, true, 'temp:PREV:_p')) "
            "stream_options(pre_filter(temp_p > 50)) into count_out "
            "as select _twstart as ws, _twend as we, _twrownum as n, _tpre_wstart as ps from probe"
        )
        try:
            tdStream.checkStreamStatus("aligned_count")
            ticks = (0, 8, 10, 12, 14, 18, 20, 24, 28, 32, 36, 40, 41, 42, 60, 61, 70, 11 * 86400, 21 * 86400)
            rows = " ".join(
                f"({base + ts * 1000}, 100, null)" if ts in (0, 10, 40)
                else f"({base + ts * 1000}, null, 1)" for ts in ticks
            )
            tdSql.execute(f"insert into samples values {rows}")
            keys = ",".join(str(base + t * 1000) for t in (0, 8, 40, 41))
            sql = ("select cast(ws as bigint), cast(we as bigint), n, cast(ps as bigint) "
                   f"from count_out where ws in ({keys}) order by ws")

            def windows(rows):
                return [(base + a * 1000, base + b * 1000, 3,
                         None if prev is None else base + prev * 1000) for a, b, prev in rows]

            initial = windows([(0, 10, None), (8, 12, 0), (40, 42, 36), (41, 60, 40)])
            tdSql.checkResultsByFunc(sql=sql, func=lambda: list(tdSql.queryResult) == initial, retry=40)
            tdSql.execute(f"insert into samples values ({base + 10000}, 0, null)")
            updated = windows([(0, 40, None), (8, 41, 0), (40, 42, 8), (41, 60, 40)])
            tdSql.checkResultsByFunc(sql=sql, func=lambda: list(tdSql.queryResult) == updated, retry=40)
            reads, stops = self._history_reads("ta_recalc_count")
            assert reads["aligned_count"] == [1, 1], reads
            assert len(stops["aligned_count"]) == 1
            assert f"stable:{base + 40000}, stop:{base + 60000}" in stops["aligned_count"][0]
        finally:
            tdSql.execute("drop stream if exists aligned_count")

    def test_overlapping_interval_repair(self):
        """Recalculate overlapping fixed windows and their immediate predecessor-dependent successor.

        1. Hold a low temperature across two days of heartbeats.
        2. Replace it with a high value and compare all nearby two-day windows with fresh history.
        3. Check that the reader skips the late metadata and body batches.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        tdSql.prepare(dbname="ta_recalc_overlaps", drop=True, vgroups=1)
        tdSql.execute("create table samples (ts timestamp, temp int, beat int)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from samples.temp, beat int from samples.beat)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        day = 86400000
        base = (int(time.time()) // 864000 - 3) * (10 * day) + day
        tdSql.execute(f"insert into probe values ({base}, 1)")
        names = []

        def start(name, history=""):
            tdSql.execute(
                f"create stream {name} interval(2d) sliding(1d) from "
                "table(time_align(table meter, true, 'temp:PREV:_p')) "
                f"stream_options({history}pre_filter(temp_p > 50 and beat = 1)) "
                f"into {name}_out as select _twstart as ws, _twrownum as n, "
                "_tpre_wstart as ps from probe"
            )
            names.append(name)
            tdStream.checkStreamStatus(name)

        def nearby(name):
            return (f"select cast(ws as bigint), n, cast(ps as bigint) from {name}_out "
                    f"where ws >= {base} and ws < {base + 8 * day} order by ws")

        def wait_for_tail(name):
            tdSql.checkResultsByFunc(
                sql=f"select max(cast(ws as bigint)) from {name}_out",
                func=lambda: tdSql.queryResult[0][0] is not None and tdSql.queryResult[0][0] > base + 8 * day,
                retry=40,
            )

        try:
            start("updated")
            hours = (0, 6, 24, 25, 48, 49, 72, 96, 120, 264, 504)
            values = " ".join(
                f"({base + h * 3600000}, {100 if h in (0, 72) else 0 if h == 6 else 'null'}, "
                f"{'null' if h == 6 else 1})" for h in hours
            )
            tdSql.execute(f"insert into samples values {values}")
            wait_for_tail("updated")
            tdSql.query(nearby("updated"))
            initial = list(tdSql.queryResult)
            tdSql.execute(f"insert into samples values ({base + 6 * 3600000}, 100, null)")
            start("reference", f"fill_history({base})|")
            wait_for_tail("reference")
            tdSql.query(nearby("reference"))
            expected = list(tdSql.queryResult)
            assert expected and initial != expected
            tdSql.checkResultsByFunc(sql=nearby("updated"), func=lambda: list(tdSql.queryResult) == expected, retry=40)
            reads, _ = self._history_reads("ta_recalc_overlaps")
            assert reads["updated"][0] == 1 and reads["reference"][0] >= 3, reads
            assert reads["updated"][1] < reads["reference"][1], reads
        finally:
            for name in reversed(names):
                tdSql.execute(f"drop stream if exists {name}")

    def test_calendar_month_repair_with_daily_slide(self):
        """Recount every overlapping month after revising a historical temperature.

        1. Close over two months of daily sliding windows over sparse temperatures.
        2. Raise one day's temperature before the next independent reading.
        3. Compare revised and unaffected windows with a fresh history replay.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        tdSql.prepare(dbname="ta_recalc_month_slide", drop=True, vgroups=1)
        tdSql.execute("create table samples (ts timestamp, temp int, beat int)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from samples.temp, beat int from samples.beat)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        day = 86400000
        base = (int(time.time()) // 86400 - 140) * day + day // 2
        tdSql.execute(f"insert into probe values ({base}, 1)")
        names = []

        def start(name, history=""):
            tdSql.execute(
                f"create stream {name} interval(1n) sliding(1d) from "
                "table(time_align(table meter, true, 'temp:PREV:_p')) "
                f"stream_options({history}pre_filter(temp_p > 50 and beat = 1)) into {name}_out "
                "as select _twstart as ws, _twrownum as n, _tpre_wstart as ps from probe"
            )
            names.append(name)
            tdStream.checkStreamStatus(name)

        def windows(name):
            return (f"select cast(ws as bigint), n, cast(ps as bigint) from {name}_out "
                    f"where ws between {base + 5 * day} and {base + 83 * day} order by ws")

        def wait_for_tail(name):
            tdSql.checkResultsByFunc(
                sql=f"select max(cast(ws as bigint)) from {name}_out",
                func=lambda: tdSql.queryResult[0][0] is not None and tdSql.queryResult[0][0] > base + 84 * day,
                retry=40,
            )

        try:
            start("updated")
            readings = " ".join(
                f"({base + i * day},{100 if i in (0, 43) else 0 if i == 42 else 'null'},1)"
                for i in range(121)
            )
            tdSql.execute(f"insert into samples values {readings}")
            wait_for_tail("updated")
            tdSql.query(windows("updated"))
            initial = {ws: n for ws, n, _ in tdSql.queryResult}
            tdSql.execute(f"insert into samples values ({base + 42 * day},100,1)")
            start("reference", f"fill_history({base})|")
            wait_for_tail("reference")
            tdSql.query(windows("reference"))
            expected = list(tdSql.queryResult)
            assert sum(n == initial[ws] + 1 for ws, n, _ in expected) >= 27
            tdSql.checkResultsByFunc(sql=windows("updated"), func=lambda: list(tdSql.queryResult) == expected, retry=40)
        finally:
            for name in reversed(names):
                tdSql.execute(f"drop stream if exists {name}")

    def test_two_fixed_durations_without_successor(self):
        """Recover both bounded PREV outputs after the longer inclusive expiry.

        1. Select heartbeats using two durations on the same physical temperature.
        2. Update the final temperature and keep independent heartbeats after its expiry.
        3. Check the closed sessions, the first stable tick, and the number of reader pulls.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        tdSql.prepare(dbname="ta_recalc_expiry", drop=True, vgroups=1)
        tdSql.execute("create table samples (ts timestamp, temperature int, beat int)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from samples.temperature, beat int from samples.beat)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        day = 86400000
        base = (int(time.time()) // 864000 - 3) * (10 * day) + day
        tdSql.execute(f"insert into probe values ({base}, 1)")
        tdSql.execute(
            "create stream bounded_session session(ts,5s) from "
            "table(time_align(table meter, true, 'temp:PREV(2s):_short;temp:PREV(5s):_long')) "
            "stream_options(pre_filter(temp_short>50 or temp_long>50 or beat=2)) "
            "into bounded_out as select _twstart as ws, _twend as we, _twrownum as n from probe"
        )
        try:
            tdStream.checkStreamStatus("bounded_session")
            ticks = [(0, "100", "null"), (8, "null", 2), (10, "100", "null"),
                     (12, "null", 1), (15, "null", 1), (16, "null", 1),
                     (20, "null", 2), (22, "null", 2), (40, "null", 2), (50, "null", 2),
                     (11 * 86400, "null", 2), (21 * 86400, "null", 2)]
            rows = " ".join(f"({base + ts * 1000}, {temp}, {beat})" for ts, temp, beat in ticks)
            tdSql.execute(f"insert into samples values {rows}")
            sql = (f"select cast(ws as bigint), cast(we as bigint), n from bounded_out "
                   f"where ws < {base + 40000} order by ws")
            initial = [(base, base, 1), (base + 8000, base + 22000, 6)]
            tdSql.checkResultsByFunc(sql=sql, func=lambda: list(tdSql.queryResult) == initial, retry=40)
            tdSql.execute(f"insert into samples values ({base + 10000}, 0, null)")
            updated = [(base, base, 1), (base + 8000, base + 8000, 1), (base + 20000, base + 22000, 2)]
            tdSql.checkResultsByFunc(sql=sql, func=lambda: list(tdSql.queryResult) == updated, retry=40)
            reads, stops = self._history_reads("ta_recalc_expiry")
            assert reads["bounded_session"] == [1, 1], reads
            assert len(stops["bounded_session"]) == 1
            assert f"stable:{base + 15001}, stop:{base + 40000}" in stops["bounded_session"][0]
        finally:
            tdSql.execute("drop stream if exists bounded_session")

    @pytest.mark.parametrize("late_ms, eligible", [(50000, False), (50001, True)])
    def test_expired_time_and_ignored_disorder(self, late_ms, eligible):
        """Check which late readings can propagate PREV into two later heartbeats.

        1. Close a ten-second window containing one qualifying heartbeat.
        2. Write a high reading exactly ten seconds behind the last timestamp, or
           one millisecond inside that interval.
        3. Compare the closed counts for default, expiry and ignored-disorder
           policies after each stream consumes a later heartbeat.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        db = f"ta_late_{late_ms}"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        tdSql.execute("create table readings (ts timestamp, temp int, beat int)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from readings.temp, beat int from readings.beat)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        base = (int(time.time()) // 60 - 2) * 60000
        tdSql.execute(f"insert into probe values ({base}, 1)")

        def expect(name, start, count):
            tdSql.checkResultsByFunc(
                sql=f"select n from {name}_out where ws={base + start}",
                func=lambda: list(tdSql.queryResult) == [(count,)],
                retry=40,
            )

        policies = (("ordinary", "", 3), ("expiry", "expired_time(10s)|", 3 if eligible else 1),
                    ("ordered", "ignore_disorder|", 1))
        names = []
        try:
            for name, option, _ in policies:
                tdSql.execute(
                    f"create stream {name} interval(10s) from table(time_align(table meter, true, 'temp:PREV:_p')) "
                    f"stream_options({option}pre_filter(beat=1 and temp_p=100)) "
                    f"into {name}_out as select _twstart as ws, _twrownum as n from probe"
                )
                names.append(name)
                tdStream.checkStreamStatus(name)
            tdSql.execute(
                f"insert into readings values ({base},100,1) ({base+40000},0,null) "
                f"({base+51000},null,1) ({base+52000},null,1) ({base+59000},100,1) ({base+60000},100,1)"
            )
            for name in names:
                expect(name, 50000, 1)

            tdSql.execute(f"insert into readings values ({base + late_ms},100,null)")
            tdSql.execute(f"insert into readings values ({base + 70000},100,1)")
            for name, _, count in policies:
                expect(name, 60000, 1)
                expect(name, 50000, count)
            # Read through the following on-time group check in all three streams.
            # Its log follows synchronous admission of the preceding late write.
            streams = self._stream_names(db)
            target = f"start: {base + late_ms}, end: {base + late_ms}"
            for _ in range(10):
                admitted, advanced = set(), set()
                for name, line in self._named_log_lines(streams):
                    if f"add recalc request for disorder data, threshold: {base + 60000}," in line and target in line:
                        admitted.add(name)
                    if "start realtime group check" in line and f"newThreshold:{base + 70000}," in line:
                        advanced.add(name)
                if advanced == set(names):
                    break
                time.sleep(1)
            assert advanced == set(names), advanced
            assert admitted == {name for name, _, count in policies if count == 3}, admitted
        finally:
            for name in reversed(names):
                tdSql.execute(f"drop stream if exists {name}")
