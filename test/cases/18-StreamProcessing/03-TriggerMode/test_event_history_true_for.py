# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestEventHistoryTrueFor:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @pytest.mark.parametrize("virtual", [False, True], ids=["table", "vtable"])
    @pytest.mark.parametrize("limit", ["count 3", "2d", "2d and count 3", "2d or count 3"])
    @pytest.mark.parametrize("ending", ["confirmed", "interrupted", "pending"])
    def test_history_open_excludes_pending_end_rows(self, virtual, limit, ending):
        """Qualify OPEN using only guaranteed window rows across history steps.

        Reject a two-row window whose END confirmation crosses the ten-day scan
        step. An interrupted END must restore normal qualification, and a window
        that already qualifies must emit OPEN even without a confirmed END.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7167723423

        History:
            - 2026-11-13 Created
        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="event_history_open", vgroups=1)
        tdSql.execute("create table src (ts timestamp, phase int)")
        source = "src"
        if virtual:
            tdSql.execute("create vtable vt (ts timestamp, phase int from src.phase)")
            source = "vt"
        base = 1735776000000  # Aligned to a ten-day history scan step.
        day = 86400000
        if ending == "confirmed":
            rows = [(0, 1), (1, 0), (2, 0), (11, 0), (20, 1), (21, 1), (22, 0), (23, 0), (31, 0)]
            expected = 20 * day  # The later valid window also proves OPEN delivery completed.
        elif ending == "interrupted":
            rows = [(0, 1), (1, 0), (2, 0), (11, 1)]
            expected = 0
        else:
            rows = [(0, 1), (1, 1), (2, 0), (11, 0)]
            expected = 0
        values = " ".join(f"({base + offset * day}, {phase})" for offset, phase in rows)
        tdSql.execute(f"insert into src values {values}")
        tdSql.execute(
            "create stream s event_window(start with phase = 1 end with phase = 0) "
            f"true_for({limit}, end(count 3)) from {source} "
            "stream_options(fill_history | event_type(window_open)) "
            f"into result as select _twstart ts, phase from {source} where ts = _twstart"
        )
        tdStream.checkStreamStatus()
        for _ in range(300):
            tdSql.query(
                "select history_progress_pct from information_schema.ins_streams "
                "where db_name='event_history_open' and stream_name='s'"
            )
            if tdSql.queryRows == 1 and tdSql.getData(0, 0) == 100:
                break
            time.sleep(0.2)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)
        for _ in range(300):
            tdSql.query(f"select cast(ts as bigint)-{base}, phase from result order by ts")
            if tdSql.queryRows > 0 and tdSql.getData(tdSql.queryRows - 1, 0) == expected:
                break
            time.sleep(0.2)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, expected)
        tdSql.checkData(0, 1, 1)

    @pytest.mark.parametrize("virtual", [False, True], ids=["table", "vtable"])
    def test_history_open_end_streak_across_blocks(self, virtual):
        """Do not release an invalid OPEN while reading END across data blocks.

        Five thousand consecutive END rows cross the reader's 4096-row batches.
        Only the later three-row window, which stays open, satisfies COUNT 3.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7167723423

        History:
            - 2026-11-13 Created
        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="event_history_open_blocks", vgroups=1)
        tdSql.execute("create table src (ts timestamp, phase int)")
        source = "src"
        if virtual:
            tdSql.execute("create vtable vt (ts timestamp, phase int from src.phase)")
            source = "vt"
        base = 1735776000000
        rows = [(0, 1)] + [(i, 0) for i in range(1, 5001)] + [(i, 1) for i in range(5001, 5004)]
        for first in range(0, len(rows), 1000):
            values = " ".join(f"({base + offset}, {phase})" for offset, phase in rows[first:first + 1000])
            tdSql.execute(f"insert into src values {values}")
        tdSql.execute(
            "create stream s event_window(start with phase = 1 end with phase = 0) "
            f"true_for(count 3, end(count 5000)) from {source} "
            "stream_options(fill_history | event_type(window_open)) "
            f"into result as select _twstart ts, phase from {source} where ts = _twstart"
        )
        tdStream.checkStreamStatus()
        for _ in range(300):
            tdSql.query(f"select cast(ts as bigint)-{base}, phase from result order by ts")
            if tdSql.queryRows > 0 and tdSql.getData(tdSql.queryRows - 1, 0) == 5001:
                break
            time.sleep(0.2)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5001)
        tdSql.checkData(0, 1, 1)

    @pytest.mark.parametrize("virtual", [False, True], ids=["table", "vtable"])
    @pytest.mark.parametrize(
        "limits, expected",
        [
            ("10m, start(5m), end(5m)", [(120, 1200, 6), (1800, 2400, 2)]),
            ("10m, start(count 2), end(count 2)", [(120, 1200, 6), (1800, 2400, 2)]),
            ("10m, start(5m and count 2), end(5m or count 2)", [(120, 1200, 6), (1800, 2400, 2)]),
            ("10m, start(5m)", [(120, 720, 2), (1800, 2400, 2)]),
            ("10m, end(5m)", [(0, 1200, 8), (1800, 2400, 2)]),
            ("10m", [(0, 720, 4), (1800, 2400, 2)]),
        ],
        ids=["duration", "count", "and_or", "start_only", "end_only", "window_only"],
    )
    def test_history_start_end_streaks(self, virtual, limits, expected):
        """History event windows honor literal start and end streak thresholds.

        Replay interrupted START and END streaks, including isolated NULL heat
        samples that match both conditions. Check first-row window boundaries,
        aggregate input ranges, and the independent minimum window duration.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7167723423

        History:
            - 2026-11-13 Created
        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="event_history_tf", vgroups=1)
        tdSql.execute("create table src (ts timestamp, heat int, speed int, flow int)")
        source = "src"
        if virtual:
            tdSql.execute(
                "create vtable vt (ts timestamp, heat int from src.heat, "
                "speed int from src.speed, flow int from src.flow)"
            )
            source = "vt"

        base = 1735689600000
        rows = [
            (0, 2500, 1, 16),
            (60, 100, 1, 0),  # Interrupt the first START streak.
            (120, 2500, 1, 16),
            (420, 2500, 1, 16),
            (720, "null", 0, 16),  # Isolated END match must not close the window.
            (721, 2500, 0, 16),
            (900, "null", 0, 16),
            (901, 2500, 0, 16),
            (1200, 0, 0, 0),
            (1500, 0, 0, 0),  # END qualifies, with the boundary at 1200.
            (1800, 2500, 1, 16),
            (2100, 2500, 1, 16),
            (2400, 0, 0, 0),
            (2700, 0, 0, 0),
            (3000, 2500, 1, 16),
            (3300, 2500, 1, 16),
            (3301, 0, 0, 0),
            (3601, 0, 0, 0),  # Qualified streaks, but the window is shorter than 10m.
        ]
        values = " ".join(
            f"({base + seconds * 1000}, {heat}, {speed}, {flow})"
            for seconds, heat, speed, flow in rows
        )
        tdSql.execute(f"insert into src values {values}")
        tdSql.execute(
            "create stream s event_window("
            "start with (heat > 500 or heat is null) and (flow > 5 or flow is null) "
            "end with (heat < 500 or heat is null) and (speed = 0 or speed is null)) "
            f"true_for({limits}) from {source} "
            "stream_options(fill_history | ignore_disorder | event_type(window_close)) "
            "into result as select _twstart ts, _twend te, _twduration win_duration, count(*) cnt "
            f"from {source} where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            tdSql.query(
                "select history_progress_pct from information_schema.ins_streams "
                "where db_name='event_history_tf' and stream_name='s'"
            )
            if tdSql.queryRows == 1 and tdSql.getData(0, 0) == 100:
                break
            time.sleep(0.2)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)

        sql = f"select cast(ts as bigint)-{base}, cast(te as bigint)-{base}, win_duration, cnt from result order by ts"
        for _ in range(100):
            tdSql.query(sql)
            if tdSql.queryRows == len(expected):
                break
            time.sleep(0.2)
        tdSql.checkRows(len(expected))
        for index, (start, end, count) in enumerate(expected):
            tdSql.checkData(index, 0, start * 1000)
            tdSql.checkData(index, 1, end * 1000)
            tdSql.checkData(index, 2, (end - start) * 1000)
            tdSql.checkData(index, 3, count)

    @pytest.mark.parametrize("virtual", [False, True], ids=["table", "vtable"])
    @pytest.mark.parametrize("explicit_end", [False, True], ids=["implicit_end", "explicit_end"])
    def test_history_streak_across_steps(self, virtual, explicit_end):
        """Retain pending START rows across history scan steps.

        Accumulate an eleven-day START streak across the ten-day history step.
        Check that %%trows still includes the first START row after END qualifies.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7167723423

        History:
            - 2026-11-13 Created
        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="event_history_steps", vgroups=1)
        tdSql.execute("create table src (ts timestamp, phase int)")
        source = "src"
        if virtual:
            tdSql.execute("create vtable vt (ts timestamp, phase int from src.phase)")
            source = "vt"
        base = 1735689600000
        day = 86400000
        values = " ".join(
            f"({base + offset * day}, {phase})"
            for offset, phase in [(0, 0), (1, 1), (12, 1), (13, 0), (14, 0)]
        )
        tdSql.execute(f"insert into src values {values}")
        end_clause = " end with phase = 0" if explicit_end else ""
        end_limit = ", end(1d)" if explicit_end else ""
        tdSql.execute(
            f"create stream s event_window(start with phase = 1{end_clause}) "
            f"true_for(start(11d and count 2){end_limit}) from {source} "
            "stream_options(fill_history | event_type(window_close)) "
            "into result as select _twstart ts, _twend te, count(phase) cnt from %%trows"
        )
        tdStream.checkStreamStatus()
        sql = f"select cast(ts as bigint)-{base}, cast(te as bigint)-{base}, cnt from result"
        for _ in range(300):
            tdSql.query(sql)
            if tdSql.queryRows == 1:
                break
            time.sleep(0.2)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, day)
        tdSql.checkData(0, 1, (13 if explicit_end else 12) * day)
        tdSql.checkData(0, 2, 3 if explicit_end else 2)

    @pytest.mark.parametrize("count_limit", [2, 3])
    def test_history_window_count_excludes_end_confirmation(self, count_limit):
        """Whole-window COUNT excludes rows after the END boundary.

        A second consecutive END row confirms closure but does not extend the
        window. Require two or three actual window rows before emitting output.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7167723423

        History:
            - 2026-11-13 Created
        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="event_history_count", vgroups=1)
        tdSql.execute("create table src (ts timestamp, phase int)")
        base = 1735689600000
        values = " ".join(
            f"({base + offset * 1000}, {phase})"
            for offset, phase in [(0, 1), (1, 0), (2, 0), (10, 1), (11, 1), (12, 0), (13, 0)]
        )
        tdSql.execute(f"insert into src values {values}")
        tdSql.execute(
            "create stream s event_window(start with phase = 1 end with phase = 0) "
            f"true_for(count {count_limit}, end(count 2)) from src "
            "stream_options(fill_history | event_type(window_close)) "
            "into result as select _twstart ts, _twend te, _twrownum nrows, count(*) cnt from %%trows"
        )
        tdStream.checkStreamStatus()
        sql = f"select cast(ts as bigint)-{base}, cast(te as bigint)-{base}, nrows, cnt from result order by ts"
        for _ in range(300):
            tdSql.query(sql)
            if tdSql.queryRows > 0 and tdSql.getData(tdSql.queryRows - 1, 0) == 10000:
                break
            time.sleep(0.2)
        expected = [(0, 1000, 2), (10000, 12000, 3)] if count_limit == 2 else [(10000, 12000, 3)]
        tdSql.checkRows(len(expected))
        for index, (start, end, count) in enumerate(expected):
            tdSql.checkData(index, 0, start)
            tdSql.checkData(index, 1, end)
            tdSql.checkData(index, 2, count)
            tdSql.checkData(index, 3, count)
