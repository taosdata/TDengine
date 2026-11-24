# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import glob
import os
import re
import time

import pytest

from new_test_framework.utils import clusterComCheck, sc, tdDnodes, tdLog, tdSql, tdStream


class TestTimeAlign:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.ensureSnode(1)

    def setup_method(self):
        self.streams = []

    def teardown_method(self):
        for name in reversed(self.streams):
            tdSql.execute(f"drop stream if exists {name}")

    def _prepare(self, name, precision="ms", **db_options):
        self.db = f"ta_stream_{name}"
        ticks_per_second = {"ms": 1000, "us": 1000000, "ns": 1000000000}[precision]
        self.base = (int(time.time() // 60) - 1) * 60 * ticks_per_second
        tdSql.prepare(dbname=self.db, drop=True, vgroups=4, precision=precision, **db_options)
        tdSql.execute(f"use {self.db}")
        sources = {}
        for index in range(32):
            table = f"source_{index}"
            tdSql.execute(f"create table {table} (ts timestamp, v int)")
            tdSql.query(
                "select vgroup_id from information_schema.ins_tables "
                f"where db_name='{self.db}' and table_name='{table}'"
            )
            sources.setdefault(tdSql.queryResult[0][0], table)
            if len(sources) == 3:
                break
        assert len(sources) == 3, "need three distinct source vgroups"
        self.a, self.b, self.c = list(sources.values())
        tdSql.execute(
            "create vtable vt (ts timestamp, "
            f"a int from {self.a}.v, b int from {self.b}.v, c int from {self.c}.v)"
        )
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({self.base}, 1)")
        return ticks_per_second

    def _create(self, name, rules, raw=False, predicate=None, source="vt", partition="", window="count_window(1)"):
        options = f"stream_options(pre_filter({predicate}))" if predicate else ""
        tdSql.execute(
            f"create stream {name} {window} "
            f"from table(time_align(table {source}, {str(raw).lower()}, '{rules}')) "
            f"{partition} {options} into {name}_out as "
            "select _twstart as ws, _twend as we, _twrownum as nr from probe"
        )
        self.streams.append(name)
        tdStream.checkStreamStatus(name)

    def _expect_windows(self, name, offsets):
        expected = [(self.base + offset, self.base + offset, 1) for offset in offsets]
        tdSql.checkResultsByFunc(
            sql=f"select cast(ws as bigint), cast(we as bigint), nr from {name}_out order by ws",
            func=lambda: list(tdSql.queryResult) == expected,
            retry=60,
        )

    def test_seed_without_history(self):
        """Seed the first trigger row from fixed-time historical predecessors.

        1. Use physical sources in distinct vgroups and insert history before creation.
        2. Trigger at the inclusive PREV duration boundary and one millisecond after it.
        3. Observe windows through a separate one-row probe table.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-17 OpenAI Created
        """
        self._prepare("seed")
        tdSql.execute(f"insert into {self.a} values ({self.base + 10000}, 7)")
        self._create("seed", "a:PREV(8s):_a;b:PREV:_a", predicate="a_a=7")
        tdSql.execute(f"insert into {self.b} values ({self.base + 18000}, 1) ({self.base + 18001}, 2)")
        self._expect_windows("seed", [18000])

    def test_flat_wide_string_seed(self):
        """Use the physical string width for a flat virtual trigger.

        Seed a 1200-byte value before stream creation. Select the first
        heartbeat using the full value and check the virtual super-table limit.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        self._prepare("wide_seed")
        tdSql.execute("create table strings (ts timestamp, v varchar(2048))")
        tdSql.execute(f"create vtable wide_sensor (ts timestamp, v varchar(4) from strings.v, b int from {self.b}.v)")
        tdSql.execute("create stable wide_parent (ts timestamp, v varchar(4)) tags(site int) virtual 1")
        tdSql.execute("create vtable narrow_child (v from strings.v) using wide_parent tags(1)")
        tdSql.execute(f"insert into strings values ({self.base + 1000}, '{'x' * 1200}')")
        tdSql.error("select v_p from table(time_align(table wide_parent, 'v:PREV:_p'))", 0x8000070B)
        self._create("wide_seed", "v:PREV:_p;b:PREV:_p", raw=True, source="wide_sensor",
                     predicate="length(v_p)=1200 and b is not null")
        tdSql.execute(f"insert into {self.b} values ({self.base + 2000}, 1)")
        self._expect_windows("wide_seed", [2000])

    def test_nanosecond_calendar_seed(self):
        """Retain a predecessor when its calendar expiry exceeds the nanosecond timestamp range.

        1. Store a value one day before the first trigger in a nanosecond database.
        2. Filter the trigger using that value with a 300-year calendar span.
        3. Check that a timestamp from another source emits exactly one window.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        second = self._prepare("calendar_seed", "ns")
        tdSql.execute(f"insert into {self.a} values ({self.base - 86400 * second}, 7)")
        self._create("calendar_seed", "a:PREV(300y):_a;b:PREV:_b", predicate="a_a=7")
        tdSql.execute(f"insert into {self.b} values ({self.base}, 1)")
        self._expect_windows("calendar_seed", [0])

    @pytest.mark.parametrize("cold", [True, False], ids=["seed", "live"])
    def test_long_fixed_nanosecond_range(self, cold):
        """Apply a fixed duration longer than the signed nanosecond tick range.

        1. Store the predecessor before or after creating the stream.
        2. Send heartbeats in 2200, at expiry and one tick after it.
        3. Check that the eligible rows and the final control row are emitted.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        name = "long_fixed_seed" if cold else "long_fixed_live"
        self._prepare(name, "ns", keep=100000)
        first = -2208988800000000000  # 1900-01-01 UTC
        heartbeat = 7258118400000000000  # 2200-01-01 UTC
        expiry = first + 110000 * 86400 * 10**9
        sample = f"insert into {self.a} values ({first}, 7)"
        if cold:
            tdSql.execute(sample)
        self._create(name, "a:PREV(110000d):_p;b:PREV:_p", raw=True,
                     predicate="a=7 or (a_p=7 and b=1) or b=2")
        query = f"select cast(ws as bigint) from {name}_out order by ws"
        expected = []
        if not cold:
            tdSql.execute(sample)
            expected.append((first,))
            tdSql.checkResultsByFunc(sql=query, func=lambda: list(tdSql.queryResult) == expected, retry=60)
        tdSql.execute(f"insert into {self.b} values ({heartbeat}, 1) ({expiry}, 1) ({expiry + 1}, 1) ({expiry + 2}, 2)")
        expected.extend([(heartbeat,), (expiry,), (expiry + 2,)])
        tdSql.checkResultsByFunc(sql=query, func=lambda: list(tdSql.queryResult) == expected, retry=60)

    def test_before_epoch_calendar_filter(self):
        """Expire a February 1969 predecessor at the inclusive March boundary in UTC.

        1. Store the last millisecond of February as the predecessor.
        2. Send heartbeats at the expiry, one millisecond later and at a control timestamp.
        3. Verify that only the boundary and the control pass the filter.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        self._prepare("before_epoch", keep=36500)
        first = -26438400001  # 1969-02-28 23:59:59.999 UTC
        expiry = -24019200001  # 1969-03-28 23:59:59.999 UTC
        tdSql.execute(f"insert into {self.a} values ({first}, 7)")
        name = "epoch_filter"
        tdSql.execute(
            f"create stream {name} count_window(1) "
            "from table(time_align(table vt, false, 'a:PREV(1n):_p;b:PREV:_p')) "
            "stream_options(config(timezone='UTC')|pre_filter((a_p=7 and b_p=1) or b_p=2)) "
            f"into {name}_out as select _twstart as ws, _twrownum as nr from probe"
        )
        self.streams.append(name)
        tdStream.checkStreamStatus(name)
        tdSql.execute(f"insert into {self.b} values ({expiry}, 1) ({expiry + 1}, 1) ({expiry + 2}, 2)")
        tdSql.checkResultsByFunc(
            sql=f"select cast(ws as bigint), nr from {name}_out order by ws",
            func=lambda: list(tdSql.queryResult) == [(expiry, 1), (expiry + 2, 1)],
            retry=60,
        )

    def test_calendar_dst_gap(self):
        """Keep a predecessor through a monthly expiry inside New York's spring gap.

        1. Store a February predecessor at 02:30:00.123 local time.
        2. Send heartbeats the next day and around the normalized March expiry.
        3. Check the inclusive boundary and a final control row.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        self._prepare("dst_gap", keep=3650)
        first = 1707550200123  # 2024-02-10 02:30:00.123 EST
        next_day = first + 86400000
        expiry = 1710055800123  # 2024-03-10 03:30:00.123 EDT
        tdSql.execute(f"insert into {self.a} values ({first}, 7)")
        tdSql.execute(
            "create stream dst_gap count_window(1) "
            "from table(time_align(table vt, false, 'a:PREV(1n):_p;b:PREV:_p')) "
            "stream_options(config(timezone='America/New_York')|pre_filter((a_p=7 and b_p=1) or b_p=2)) "
            "into dst_gap_out as select _twstart as ws, _twrownum as nr from probe"
        )
        self.streams.append("dst_gap")
        tdStream.checkStreamStatus("dst_gap")
        tdSql.execute(
            f"insert into {self.b} values ({next_day}, 1) ({expiry}, 1) ({expiry + 1}, 1) ({expiry + 2}, 2)"
        )
        tdSql.checkResultsByFunc(
            sql="select cast(ws as bigint), nr from dst_gap_out order by ws",
            func=lambda: list(tdSql.queryResult) == [(next_day, 1), (expiry, 1), (expiry + 2, 1)],
            retry=60,
        )

    def test_history_aliases_share_file_value(self):
        """Read two virtual columns bound to the same stored column during history replay.

        1. Bind two virtual columns to one physical value column.
        2. Flush a value before creating a stream with historical input.
        3. Require both column values in the filter and verify the output window.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        self._prepare("file_alias")
        tdSql.execute(
            f"create vtable alias_vt (ts timestamp, left_v int from {self.a}.v, right_v int from {self.a}.v)"
        )
        tdSql.execute(f"insert into {self.a} values ({self.base}, 42)")
        tdSql.execute(f"flush database {self.db}")
        name = "history_alias"
        tdSql.execute(
            f"create stream {name} count_window(1) "
            "from table(time_align(table alias_vt, false, 'left_v:PREV:_p;right_v:PREV:_p')) "
            f"stream_options(fill_history({self.base})|pre_filter(left_v_p=42 and right_v_p=42)) "
            f"into {name}_out as select _twstart as ws, _twend as we, _twrownum as nr from probe"
        )
        self.streams.append(name)
        tdStream.checkStreamStatus(name)
        self._expect_windows(name, [0])

    def test_alignment_filter_and_rowset(self):
        """Retain candidate updates before filtering and preserve the declared row set.

        1. A filtered raw row supplies PREV to a later row from another vgroup.
        2. Unconsumed alignment definitions still contribute timestamps.
        3. Output raw includes timestamps from unselected columns.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-17 OpenAI Created
        """
        self._prepare("rowset")
        self._create("filtered", "a:PREV:_a", raw=True, predicate="a_a=7 and b is not null")
        self._create("selected", "a:PREV:_a;b:PREV:_a")
        self._create("all_raw", "a:PREV:_a;b:PREV:_a", raw=True)
        tdSql.execute(f"insert into {self.a} values ({self.base + 10000}, 7)")
        tdSql.execute(f"insert into {self.b} values ({self.base + 15000}, 1)")
        tdSql.execute(f"insert into {self.c} values ({self.base + 16000}, 1)")
        self._expect_windows("filtered", [15000])
        self._expect_windows("selected", [10000, 15000])
        self._expect_windows("all_raw", [10000, 15000, 16000])

    @pytest.mark.parametrize("precision", ["ms", "us", "ns"])
    def test_duration_boundary_matrix(self, precision):
        """Evaluate duplicate PREV durations at the inclusive boundary and the next tick.

        1. Create short, long, and unbounded rules for the same temperature.
        2. Send timestamp-only rows at the boundary and one database tick beyond.
        3. Check exactly which rows retain only the longer rules.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        second = self._prepare(f"duration_{precision}", precision)
        rules = "a:PREV(2s):_short;a:PREV(4s):_long;a:PREV:_always;b:PREV:_tick"
        self._create("short_span", rules, predicate="a_short=100 and a_long=100")
        self._create("long_span", rules, predicate="a_short is null and a_long=100")
        self._create("unbounded_span", rules, predicate="a_long is null and a_always=100")
        tdSql.execute(f"insert into {self.a} values ({self.base}, 100)")
        times = [second, 2 * second, 2 * second + 1, 4 * second, 4 * second + 1]
        rows = " ".join(f"({self.base + t}, null)" for t in times)
        tdSql.execute(f"insert into {self.b} values {rows}")
        self._expect_windows("short_span", [0, second, 2 * second])
        self._expect_windows("long_span", [2 * second + 1, 4 * second])
        self._expect_windows("unbounded_span", [4 * second + 1])

    def test_trigger_type_matrix(self):
        """Check window boundaries over an aligned input containing a NULL heartbeat.

        1. Share an input among count, session, state, event, interval and sliding triggers.
        2. Send gaps on both sides of the session limit and data on an interval boundary.
        3. Compare closed windows and clock ticks with fixed expected timestamps.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("matrix")
        rules = "a:PREV:_aligned"
        for name, window in [
            ("matrix_count", "count_window(3)"),
            ("matrix_session", "session(ts, 1s)"),
            ("matrix_state", "state_window(a_aligned)"),
            ("matrix_event", "event_window(start with a_aligned=100 end with a_aligned=0)"),
            ("matrix_interval", "interval(10s)"),
        ]:
            self._create(name, rules, window=window)
        tdSql.execute(
            f"create stream matrix_sliding sliding(10s) from table(time_align(table vt, false, '{rules}')) "
            "into matrix_sliding_out as select _tcurrent_ts as tick, v from probe"
        )
        self.streams.append("matrix_sliding")
        tdStream.checkStreamStatus("matrix_sliding")
        data = [(0, 100), (1000, "null"), (2000, 100), (7000, 0), (9000, 0), (20000, 100), (21500, 0)]
        rows = " ".join(f"({self.base+t}, {val})" for t, val in data)
        tdSql.execute(f"insert into {self.a} values {rows}")
        # Inspect the closed windows starting in the first ten seconds. Later open windows
        # are deliberately left to the engine until another row supplies their closing boundary.
        for name, windows in [
            ("matrix_count", [(0, 2000, 3), (7000, 20000, 3)]),
            ("matrix_session", [(0, 2000, 3), (7000, 7000, 1), (9000, 9000, 1)]),
            ("matrix_state", [(0, 2000, 3), (7000, 9000, 2)]),
            ("matrix_event", [(0, 7000, 4)]),
            ("matrix_interval", [(0, 10000, 5)]),
        ]:
            expected = [(self.base + start, self.base + end, n) for start, end, n in windows]
            tdSql.checkResultsByFunc(
                sql=(f"select cast(ws as bigint), cast(we as bigint), nr from {name}_out "
                     f"where ws >= {self.base} and ws < {self.base + 10000} order by ws"),
                func=lambda: list(tdSql.queryResult) == expected,
                retry=60,
            )
        tdSql.checkResultsByFunc(
            sql=(f"select cast(tick as bigint) from matrix_sliding_out "
                 f"where tick between {self.base+10000} and {self.base+20000} order by tick"),
            func=lambda: list(tdSql.queryResult) == [(self.base + 10000,), (self.base + 20000,)],
            retry=60,
        )

    def test_independent_virtual_children(self):
        """Keep PREV candidates separate for children in the same trigger group.

        1. Two virtual children have historical values 7 and 70.
        2. Both children receive a timestamp from a shared source and share a tag.
        3. Only the child aligned to 7 passes the filter and emits a window.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-17 OpenAI Created
        """
        self._prepare("children")
        tdSql.execute("create stable children (ts timestamp, a int, b int) tags(g int) virtual 1")
        tdSql.execute(f"create vtable child1 ({self.a}.v, {self.b}.v) using children tags(1)")
        tdSql.execute(f"create vtable child2 ({self.c}.v, {self.b}.v) using children tags(1)")
        tdSql.execute(f"insert into {self.a} values ({self.base + 10000}, 7)")
        tdSql.execute(f"insert into {self.c} values ({self.base + 10000}, 70)")
        self._create("children", "a:PREV:_a;b:PREV:_a", predicate="a_a=7",
                     source="children", partition="partition by g", window="session(ts, 1s)")
        tdSql.execute(f"insert into {self.b} values ({self.base + 18000}, 1) ({self.base + 20000}, 2)")
        self._expect_windows("children", [18000])

    def test_pending_shared_source_after_child_rebind(self):
        """Keep pending source rows for the sibling that still uses the old binding.

        Two children share a session group, a value source and a heartbeat.
        East accepts a raw value of 17 while west switches to a source holding
        30. Later heartbeats exercise both bindings on the original task.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("pending_shared_rebind")
        tdSql.execute("create table east_marker (ts timestamp, v int)")
        tdSql.execute(f"insert into east_marker values ({self.base}, 2)")
        tdSql.execute("create stable meters (ts timestamp, a int, pulse int, marker int) tags(g int) virtual 1")
        tdSql.execute(f"create vtable west ({self.a}.v, {self.b}.v, {self.c}.v) using meters tags(1)")
        tdSql.execute(f"create vtable east ({self.a}.v, {self.b}.v, east_marker.v) using meters tags(1)")
        tdSql.execute(f"insert into {self.a} values ({self.base}, 10)")
        tdSql.execute(f"insert into {self.c} values ({self.base}, 30)")
        self._create(
            "sibling_input", "a:PREV:_p;marker:PREV:_p", raw=True,
            predicate="(a=17 and marker_p=2) or (pulse is not null and pulse=a_p)",
            source="meters", partition="partition by g", window="session(ts, 1s)",
        )
        task_query = (
            "select task_id from information_schema.ins_stream_tasks "
            "where stream_name='sibling_input' and type='Trigger' and status='Running'"
        )
        tdSql.query(task_query)
        initial = list(tdSql.queryResult)
        assert len(initial) == 1
        tdSql.execute(f"insert into {self.a} values ({self.base + 1000}, 17)")
        tdSql.execute(f"insert into {self.b} values ({self.base + 2000}, -1)")
        tdSql.execute(f"alter vtable west alter column a set {self.c}.v")
        tdSql.execute(
            f"insert into {self.b} values ({self.base + 21000}, 17) ({self.base + 24000}, 30) "
            f"({self.base + 27000}, 99) ({self.base + 31000}, 17) ({self.base + 41000}, 30)"
        )
        self._expect_windows("sibling_input", [1000, 21000, 24000, 31000])
        tdSql.query(task_query)
        assert list(tdSql.queryResult) == initial

    def test_explicit_calc_alignment(self):
        """Use independent TIME_ALIGN rules in the trigger and calculation query.

        1. Trigger with PREV 8s at a row nine seconds after the historical value.
        2. Calculate through %%tbname with an explicit PREV 20s rule.
        3. Verify the calculation reads 7 under its own alignment duration.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-17 OpenAI Created
        """
        self._prepare("calc")
        tdSql.execute(f"insert into {self.a} values ({self.base + 10000}, 7)")
        tdSql.execute(
            "create stream explicit_calc count_window(1) "
            "from table(time_align(table vt, false, 'a:PREV(8s):_a;b:PREV:_a')) "
            "partition by tbname into calc_out (ws, v) as "
            "select ts as ws, a_a as v "
            "from table(time_align(table %%tbname, false, 'a:PREV(20s):_a;b:PREV:_a')) "
            "where ts=_twstart"
        )
        self.streams.append("explicit_calc")
        tdStream.checkStreamStatus("explicit_calc")
        tdSql.execute(f"insert into {self.b} values ({self.base + 19000}, 1)")
        tdSql.checkResultsByFunc(
            sql="select cast(ws as bigint), v from calc_out",
            func=lambda: list(tdSql.queryResult) == [(self.base + 19000, 7)],
            retry=60,
        )

    def _create_period(self, name, raw=False, ignore=True, source="vt"):
        options = "stream_options(ignore_nodata_trigger)" if ignore else ""
        tdSql.execute(
            f"create stream {name} period(5s) "
            f"from table(time_align(table {source}, {str(raw).lower()}, 'a:PREV:_a')) "
            f"{options} into {name}_out nodelay_create_subtable as "
            "select cast(_tlocaltime / 1000000 as timestamp) ts, 1 as n from probe"
        )
        self.streams.append(name)
        tdStream.checkStreamStatus(name)

    def _period_count(self, name):
        tdSql.query(f"select count(*) from {name}_out")
        return tdSql.queryResult[0][0]

    def _wait_period_ticks(self):
        # Observe actual scheduler output beyond two boundaries; elapsed time alone
        # does not establish that input collection and dispatch have run.
        target = (int(time.time()) // 5 + 3) * 5000
        tdSql.checkResultsByFunc(
            sql="select max(cast(ts as bigint)) from ticks_out",
            func=lambda: bool(tdSql.queryResult) and tdSql.queryResult[0][0] is not None
                         and tdSql.queryResult[0][0] >= target,
            retry=40,
        )

    def _period_patch_counts(self):
        tdSql.query(
            "select stream_id from information_schema.ins_streams "
            f"where db_name='{self.db}' and stream_name in ('selected_p','raw_p')"
        )
        ids = {str(row[0]).lower().removeprefix("0x").lstrip("0") for row in tdSql.queryResult}
        counts = dict.fromkeys(ids, 0)
        for path in glob.glob(os.path.join(tdDnodes.getDnodeDir(1), "log", "taosdlog.*")):
            with open(path, encoding="utf-8", errors="replace") as log_file:
                for line in log_file:
                    if "finished virtual table patch" in line:
                        match = re.search(r"STREAM:([0-9a-f]+)", line)
                        if match and match.group(1).lstrip("0") in counts:
                            counts[match.group(1).lstrip("0")] += 1
        return counts

    def test_period_selected_sources(self):
        """PERIOD uses participating source rows without computing PREV values.

        1. Exclude unrelated sources unless output raw includes them.
        2. Count a NULL aligned column and a sibling-column write on the same table.
        3. Preserve empty scheduling, reject PRE_FILTER, and switch source bindings.
        4. Restart without replaying historical rows into new PERIOD input flags.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-17 OpenAI Created
        """
        self._prepare("period")
        tdSql.execute("create table pair (ts timestamp, a int, b int)")
        tdSql.execute("create vtable pair_vt (ts timestamp, a int from pair.a, b int from pair.b)")
        tdSql.execute("create table clock_input (ts timestamp, v int)")
        tdSql.execute("create vtable clock_vt (ts timestamp, a int from clock_input.v)")
        self._create_period("selected_p")
        self._create_period("raw_p", raw=True)
        self._create_period("ticks", ignore=False, source="clock_vt")
        self._create_period("pair_p", source="pair_vt")
        tdSql.execute("insert into clock_input values (now, 1)")
        for predicate in ("1=1", "a_a=7", "ts>0"):
            tdSql.error(
                "create stream rejected period(5s) "
                "from table(time_align(table vt, 'a:PREV:_a')) "
                f"stream_options(pre_filter({predicate})) into rejected_out as "
                "select cast(_tlocaltime / 1000000 as timestamp) ts from probe"
            )

        tdSql.execute(f"insert into {self.c} values ({self.base + 10000}, 1)")
        self._wait_period_ticks()
        assert self._period_count("selected_p") == 0
        assert self._period_count("raw_p") == 1
        assert self._period_count("pair_p") == 0

        tdSql.execute(f"insert into {self.a} values ({self.base + 11000}, NULL)")
        tdSql.execute(f"insert into pair(ts,b) values ({self.base + 12000}, 1)")
        self._wait_period_ticks()
        assert self._period_count("selected_p") == 1
        assert self._period_count("raw_p") == 2
        assert self._period_count("pair_p") == 1

        before = self._period_patch_counts()
        assert len(before) == 2
        tdSql.execute(f"alter vtable vt alter column a set {self.c}.v")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            after = self._period_patch_counts()
            if all(after.get(key, 0) > count for key, count in before.items()):
                break
            time.sleep(0.2)
        else:
            raise AssertionError("PERIOD source binding patch did not finish")
        tdSql.execute(f"insert into {self.a} values ({self.base + 13000}, 7)")
        self._wait_period_ticks()
        assert self._period_count("selected_p") == 1
        tdSql.execute(f"insert into {self.c} values ({self.base + 14000}, 70)")
        self._wait_period_ticks()
        assert self._period_count("selected_p") == 2

        before_restart = {name: self._period_count(name) for name in ("selected_p", "raw_p", "pair_p")}
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        for name in self.streams:
            tdStream.checkStreamStatus(name)
        tdSql.execute("insert into clock_input values (now, 2)")
        self._wait_period_ticks()
        for name, count in before_restart.items():
            assert self._period_count(name) == count, f"{name} replayed historical input after restart"

    def test_nested_realtime_seed(self):
        """Use a historical predecessor when the first nested count window opens.

        1. Insert a predecessor before starting an interval with a count-window leaf.
        2. Insert two rows from another source without supplying a new predecessor.
        3. Read the leaf bounds and row count from the result table.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("nested_seed")
        tdSql.execute(f"insert into {self.a} values ({self.base + 1000}, 7)")
        window = "window (interval(1m) sliding(1m) as outer_win, count_window(2,2))"
        self._create("nested_seed", "a:PREV:_a;b:PREV:_b", predicate="a_a=7", window=window)
        self._create("nested_control", "b:PREV:_b", window=window)
        tdSql.execute(f"insert into {self.b} values ({self.base + 10000}, 10) ({self.base + 20000}, 20)")
        # Both streams see the same heartbeat beyond the parent window boundary.
        tdSql.execute(f"insert into {self.b} values ({self.base + 61000}, 30)")
        expected = [(self.base + 10000, self.base + 20000, 2)]
        for stream in ("nested_control", "nested_seed"):
            tdSql.checkResultsByFunc(
                sql=(f"select cast(ws as bigint), cast(we as bigint), nr from {stream}_out "
                     f"where ws < {self.base + 60000} order by ws"),
                func=lambda: list(tdSql.queryResult) == expected,
                retry=30,
            )

    def test_delete_without_recalc_keeps_open_count(self):
        """Preserve consumed rows in an open count window when delete recalculation is off.

        1. Feed three samples to plain and aligned streams sharing the same virtual table.
        2. Wait for both overlapping count windows to emit their first result.
        3. Delete one counted sample and verify the next input closes both windows.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("count_delete_off")
        tdSql.execute(
            "create stream plain_count count_window(3, 1) from vt "
            "stream_options(pre_filter(a>0)) into plain_count_out as "
            "select _twstart as ws, _twend as we, _twrownum as nr from probe"
        )
        self.streams.append("plain_count")
        tdStream.checkStreamStatus("plain_count")
        self._create("aligned_count", "a:PREV:_a", predicate="a_a>0", window="count_window(3, 1)")
        tdSql.execute(f"insert into {self.a} values ({self.base}, 1) "
                      f"({self.base + 1000}, 2) ({self.base + 2000}, 3)")

        def check_windows(expected):
            for name in ("plain_count", "aligned_count"):
                tdSql.checkResultsByFunc(
                    sql=f"select cast(ws as bigint), cast(we as bigint), nr from {name}_out order by ws",
                    func=lambda: list(tdSql.queryResult) == expected,
                    retry=30,
                )

        first = (self.base, self.base + 2000, 3)
        check_windows([first])
        tdSql.execute(f"delete from {self.a} where ts={self.base + 2000}")
        tdSql.query(f"select count(*) from {self.a}")
        tdSql.checkData(0, 0, 2)
        tdSql.execute(f"insert into {self.a} values ({self.base + 3000}, 4)")
        check_windows([first, (self.base + 1000, self.base + 3000, 3)])

    def test_history_seed_before_start(self):
        """Use an earlier temperature to evaluate heartbeats inside the history range.

        1. Insert a temperature before the start and two heartbeats after it.
        2. Run each history scheduling mode over the same input and filter.
        3. Check that only the heartbeat inside the PREV duration qualifies.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("history_seed")
        tdSql.execute(f"insert into {self.a} values ({self.base}, 7)")
        tdSql.execute(f"insert into {self.b} values ({self.base + 8000}, 1) ({self.base + 8001}, 2)")
        for name, option in (("history_seed", "fill_history"), ("history_first_seed", "fill_history_first")):
            tdSql.execute(
                f"create stream {name} count_window(1) from "
                "table(time_align(table vt, false, 'a:PREV(8s):_p;b:PREV:_p')) "
                f"stream_options({option}({self.base + 1000})|pre_filter(a_p=7)) "
                f"into {name}_out as select _twstart as ws, _twend as we, _twrownum as nr from probe"
            )
            self.streams.append(name)
            tdStream.checkStreamStatus(name)
            self._expect_windows(name, [8000])

    def test_history_after_source_overwrite(self):
        """Read corrected heartbeats through ordinary and aligned streams.

        1. Flush an old predecessor and heartbeat, then overwrite both timestamps.
        2. Backfill the physical table, its virtual table, and its aligned virtual table from the same start.
        3. Change the temperature between heartbeats and check the carried value.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        self._prepare("history_overwrite")
        tdSql.execute(f"insert into {self.a} values ({self.base}, 70)")
        tdSql.execute(f"insert into {self.b} values ({self.base + 1000}, 10)")
        tdSql.execute(f"flush database {self.db}")
        tdSql.execute(f"insert into {self.a} values ({self.base}, 7) ({self.base + 2000}, 9)")
        tdSql.execute(f"insert into {self.b} values ({self.base + 1000}, 1) ({self.base + 3000}, 2)")

        for name, source, predicate in (
            ("ordinary_history", self.b, "v=1 or v=2"),
            ("virtual_history", "vt", "b in (1, 2)"),
            ("aligned_history", "table(time_align(table vt, true, 'a:PREV:_p'))",
             "(b=1 and a_p=7) or (b=2 and a_p=9)"),
        ):
            tdSql.execute(
                f"create stream {name} count_window(1) from {source} "
                f"stream_options(fill_history_first({self.base + 1})|pre_filter({predicate})) "
                f"into {name}_out as select _twstart as ws, _twend as we, _twrownum as nr from probe"
            )
            self.streams.append(name)
            tdStream.checkStreamStatus(name)
            self._expect_windows(name, [1000, 3000])

    def test_direct_child_default_rules(self):
        """Carry a VARCHAR value on a directly selected virtual child with default rules.

        1. Two children have distinct labels and share a heartbeat source.
        2. Keep the selected child's full variable-length label across two input batches.
        3. Replace its label with an empty string and check the earlier value stops matching.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("direct_child")
        tdSql.execute("create table labels (ts timestamp, v varchar(120))")
        tdSql.execute("create table other_labels (ts timestamp, v varchar(120))")
        tdSql.execute("create stable meters (ts timestamp, label varchar(120), beat int) tags(g int) virtual 1")
        tdSql.execute(f"create vtable selected_meter (labels.v, {self.a}.v) using meters tags(1)")
        tdSql.execute(f"create vtable other_meter (other_labels.v, {self.a}.v) using meters tags(1)")
        label = "temperature-reading-" * 5
        tdSql.execute(f"insert into labels values ({self.base}, '{label}')")
        tdSql.execute(f"insert into other_labels values ({self.base}, 'other')")
        for name, predicate in (("full_label", f"label='{label}'"), ("empty_label", "label=''")):
            tdSql.execute(
                f"create stream {name} count_window(1) from table(time_align(table selected_meter)) "
                f"stream_options(pre_filter({predicate})) into {name}_out as "
                "select _twstart as ws, _twend as we, _twrownum as nr from probe"
            )
            self.streams.append(name)
            tdStream.checkStreamStatus(name)
        tdSql.execute(f"insert into {self.a} values ({self.base + 1000}, 1)")
        self._expect_windows("full_label", [1000])
        tdSql.execute(f"insert into other_labels values ({self.base + 2000}, '{label}')")
        tdSql.execute(f"insert into {self.a} values ({self.base + 3000}, 2)")
        self._expect_windows("full_label", [1000, 3000])
        tdSql.execute(f"insert into labels values ({self.base + 4000}, '')")
        self._expect_windows("empty_label", [4000])
        tdSql.execute(f"insert into {self.a} values ({self.base + 5000}, 3)")
        self._expect_windows("empty_label", [4000, 5000])
        self._expect_windows("full_label", [1000, 3000])

    def test_decimal_candidates_across_heartbeats(self):
        """Carry both decimal widths across NULL inputs and expire only the short rule.

        1. Start with an empty source and read a heartbeat before and after an all-NULL row.
        2. Carry one negative decimal and a 128-bit decimal to the inclusive duration boundary.
        3. Expire the short rule, then replace only the narrow decimal and check the wide value.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("decimal_candidates")
        tdSql.execute("create table amounts (ts timestamp, price decimal(12,2), total decimal(38,4))")
        tdSql.execute(
            "create vtable balances (ts timestamp, price decimal(12,2) from amounts.price, "
            f"total decimal(38,4) from amounts.total, beat int from {self.b}.v)"
        )
        wide = "cast('1234567890123456789012345678.1234' as decimal(38,4))"
        negative = "cast('-1234.56' as decimal(12,2))"
        replacement = "cast('87.65' as decimal(12,2))"
        rules = "price:PREV(2s):_short;price:PREV:_carry;total:PREV:_carry"
        predicate = (
            "(beat in (1,2) and price_short is null and price_carry is null and total_carry is null) or "
            f"(beat=3 and price_short={negative} and price_carry={negative} and total_carry={wide}) or "
            f"(beat=4 and price_short is null and price_carry={negative} and total_carry={wide}) or "
            f"(beat=5 and price_short={replacement} and price_carry={replacement} and total_carry={wide})"
        )
        self._create("decimal_candidates", rules, raw=True, source="balances", predicate=predicate)
        offsets = []

        def heartbeat(offset):
            tdSql.execute(f"insert into {self.b} values ({self.base + offset}, {len(offsets) + 1})")
            offsets.append(offset)
            self._expect_windows("decimal_candidates", offsets)

        heartbeat(0)
        tdSql.execute(f"insert into amounts values ({self.base + 1000}, NULL, NULL)")
        heartbeat(2000)
        tdSql.execute(f"insert into amounts values ({self.base + 3000}, -1234.56, 1234567890123456789012345678.1234)")
        heartbeat(5000)
        heartbeat(5001)
        tdSql.execute(f"insert into amounts values ({self.base + 6000}, 87.65, NULL)")
        heartbeat(7000)

    def test_shared_table_different_physical_columns(self):
        """Keep seed and live values distinct when two children share one logical slot.

        Bind each child to a different column on the same table. Check the selected
        child on the first heartbeat and again after replacing the source values.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        self._prepare("physical_slots")
        tdSql.execute("create table raw_values (ts timestamp, x int, y int)")
        tdSql.execute("create stable meters (ts timestamp, v int, beat int) tags(site int) virtual 1")
        tdSql.execute(f"create vtable mx (v from raw_values.x, beat from {self.b}.v) using meters tags(1)")
        tdSql.execute(f"create vtable my (v from raw_values.y, beat from {self.b}.v) using meters tags(2)")
        tdSql.execute(f"insert into raw_values values ({self.base}, 7, 9)")
        name = "physical_slots"
        tdSql.execute(
            f"create stream {name} count_window(1) from table(time_align(table meters, true, 'v:PREV:_p')) "
            f"partition by tbname, site stream_options(pre_filter(beat is not null and v_p=7)) into {name}_out "
            "tags(source_site int as site) as select _twstart as ws, _twrownum as n from probe"
        )
        self.streams.append(name)
        tdStream.checkStreamStatus(name)
        expected = []
        for offset, site in ((1000, 1), (3000, 2)):
            if site == 2:
                tdSql.execute(f"insert into raw_values values ({self.base + 2000}, 9, 7)")
            tdSql.execute(f"insert into {self.b} values ({self.base + offset}, 1)")
            expected.append((self.base + offset, site))
            tdSql.checkResultsByFunc(
                sql=f"select cast(ws as bigint), source_site from {name}_out order by ws, source_site",
                func=lambda: list(tdSql.queryResult) == expected, retry=45,
            )

    def test_nanosecond_source_millisecond_seed(self):
        """Seed the first millisecond heartbeat from the final nanosecond of the preceding tick.

        Store different predecessors in a fresh nanosecond database. Match both the
        latest value and its ordinary timestamp column on the first heartbeat, then
        replace both values in real time and match the second heartbeat.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        self._prepare("mixed_clock")
        source = "ta_stream_mixed_clock_source"
        tdSql.execute(f"drop database if exists {source}")
        tdSql.execute(f"create database {source} precision 'ns'")
        tdSql.execute(f"create table {source}.voltage (ts timestamp, v int, sampled timestamp)")
        tdSql.execute(f"create vtable mixed (ts timestamp, voltage int from {source}.voltage.v, "
                      f"sampled timestamp from {source}.voltage.sampled, beat int from {self.b}.v)")
        ns_per_ms = 1000000
        before_beat = (self.base + 1000) * ns_per_ms - 1
        tdSql.execute(f"insert into {source}.voltage values ({self.base * ns_per_ms}, 3, {self.base * ns_per_ms}) "
                      f"({before_beat}, 7, {(self.base + 123) * ns_per_ms + 456})")
        predicate = (f"(beat=1 and voltage_p=7 and sampled_p=cast({self.base + 123} as timestamp)) or "
                     f"(beat=2 and voltage_p=9 and sampled_p=cast({self.base + 1123} as timestamp))")
        self._create("mixed_clock", "voltage:PREV:_p;sampled:PREV:_p", raw=True, source="mixed", predicate=predicate)
        tdSql.execute(f"insert into {self.b} values ({self.base + 1000}, 1)")
        self._expect_windows("mixed_clock", [1000])
        tdSql.execute(f"insert into {source}.voltage values ({(self.base + 1500) * ns_per_ms}, "
                      f"9, {(self.base + 1123) * ns_per_ms + 789})")
        tdSql.execute(f"insert into {self.b} values ({self.base + 2000}, 2)")
        self._expect_windows("mixed_clock", [1000, 2000])

    def test_shared_source_changes_virtual_column_by_child(self):
        """Bind one physical column to different aligned columns in two child tables.

        1. Map the shared value to the left column of one child and the right column of the other.
        2. Seed each child's other operand independently before creating the stream.
        3. Change only the shared source and check which tagged group accepts each heartbeat.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("shared_binding")
        tdSql.execute("create table ticks (ts timestamp, v int)")
        tdSql.execute("create stable meters (ts timestamp, lhs int, rhs int, beat int) tags(site int) virtual 1")
        tdSql.execute(f"create vtable west ({self.a}.v, {self.b}.v, ticks.v) using meters tags(1)")
        tdSql.execute(f"create vtable east ({self.c}.v, {self.a}.v, ticks.v) using meters tags(2)")
        for table, value in ((self.a, 7), (self.b, 9), (self.c, 7)):
            tdSql.execute(f"insert into {table} values ({self.base}, {value})")
        name = "shared_binding"
        tdSql.execute(
            f"create stream {name} count_window(1) "
            "from table(time_align(table meters, true, 'lhs:PREV:_p;rhs:PREV:_p')) partition by tbname, site "
            f"stream_options(pre_filter(beat is not null and lhs_p=7 and rhs_p=9)) into {name}_out "
            "tags(source_site int as site) as select _twstart as ws, _twrownum as n from probe"
        )
        self.streams.append(name)
        tdStream.checkStreamStatus(name)
        expected = []
        for offset, shared, site in ((1000, None, 1), (3000, 9, 2), (5000, 7, 1)):
            if shared is not None:
                tdSql.execute(f"insert into {self.a} values ({self.base + offset - 500}, {shared})")
            tdSql.execute(f"insert into ticks values ({self.base + offset}, 1)")
            expected.append((self.base + offset, site, 1))
            tdSql.checkResultsByFunc(
                sql=f"select cast(ws as bigint), source_site, n from {name}_out order by ws, source_site",
                func=lambda: list(tdSql.queryResult) == expected,
                retry=45,
            )

    def test_nested_read_segments_per_vnode(self):
        """Read one ancestor range for two physical inputs on the same vnode.

        1. Populate temperature and heartbeat tables before creating a partitioned stream.
        2. Check both closed history windows and their parent session.
        3. Insert a separate live session and check the same fields and output tag.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        tdSql.prepare(dbname="ta_nested_reader", drop=True, vgroups=1)
        base = (int(time.time()) // 120 - 4) * 120000
        tdSql.execute("create table therm (ts timestamp, v int)")
        tdSql.execute("create table beat (ts timestamp, v int)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({base}, 1)")
        tdSql.execute("create stable meters (ts timestamp, temp int, pulse int) tags(site int) virtual 1")
        tdSql.execute("create vtable meter (therm.v, beat.v) using meters tags(7)")

        def insert_session(t):
            tdSql.execute(f"insert into therm values ({t},100) ({t + 10000},0) ({t + 20000},100) "
                          f"beat values ({t},1) ({t + 5000},1) ({t + 15000},1)")

        def expect_closed(t):
            tdSql.checkResultsByFunc(
                sql=(f"select cast(ws as bigint), cast(we as bigint), n, cast(parent_ws as bigint), source_site "
                     f"from nested_reader_out where ws in ({t},{t + 10000}) order by ws"),
                func=lambda: list(tdSql.queryResult) == [
                    (t, t + 5000, 2, t, 7), (t + 10000, t + 15000, 2, t, 7)
                ], retry=45,
            )

        insert_session(base)
        tdSql.execute(
            "create stream nested_reader window(session(ts,120s) as parent_win, state_window(temp_p) as leaf_win) "
            "from table(time_align(table meters,true,'temp:PREV:_p;pulse:PREV:_p')) partition by tbname, site "
            "stream_options(fill_history_first|low_latency_calc|pre_filter(pulse_p=1)) "
            "into nested_reader_out tags(source_site int as site) "
            "as select leaf_win._twstart as ws, leaf_win._twend as we, leaf_win._twrownum as n, "
            "parent_win._twstart as parent_ws from probe"
        )
        self.streams.append("nested_reader")
        tdStream.checkStreamStatus("nested_reader")
        expect_closed(base)
        insert_session(base + 200000)
        expect_closed(base + 200000)
