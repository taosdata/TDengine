# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import re
import sys
import time
from pathlib import Path

import pytest

from new_test_framework.utils import clusterComCheck, sc, tdDnodes, tdLog, tdSql, tdStream

notify_helpers = str(Path(__file__).resolve().parents[1] / "05-Notify")
if notify_helpers not in sys.path:
    sys.path.insert(0, notify_helpers)
from notify_check import NotifyLog
from stream_notify_server import start_notify_server_background, stop_notify_server_background


class TestTimeAlignNestedContinuation:
    updatecfgDict = {"streamTriggerMaxPendingParams": 4, "stDebugFlag": 135}
    result = "live_out"

    @classmethod
    def setup_class(cls):
        tdStream.ensureSnode(1)

    def _prepare(self, db, grouped=False, padded=None, qualify_value=None, expire=False,
                 filter_offsets=(34000, 40000), flush_gap=None, notify=False, separate_output=False, base=None):
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        self.base = base if base is not None else (int(time.time()) // 120 - 3) * 120000
        self.result = "live_out"
        if separate_output:
            tdSql.execute(f"drop database if exists {db}_out")
            tdSql.execute(f"create database {db}_out vgroups 1")
            self.result = f"{db}_out.live_out"
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({self.base}, 1)")
        self.padded = padded
        for table in ("therm", "ticks"):
            extra = ", noise int" if table == padded else ""
            tdSql.execute(f"create table {table} (ts timestamp, v int{extra})")
        if grouped:
            tdSql.execute("create table peer_therm (ts timestamp, v int)")
            tdSql.execute("create table peer_ticks (ts timestamp, v int)")
            tdSql.execute("create stable meters (ts timestamp, temp int, beat int) tags(site int) virtual 1")
            tdSql.execute("create vtable meter (therm.v, ticks.v) using meters tags(1)")
            tdSql.execute("create vtable peer (peer_therm.v, peer_ticks.v) using meters tags(2)")
        else:
            extra = f", noise int from {padded}.noise" if padded else ""
            tdSql.execute(f"create vtable meter (ts timestamp, temp int from therm.v, beat int from ticks.v{extra})")
        self.predicate = "beat_p=1 and noise is null" if padded else "beat_p=1"
        state = "temp_p"
        options = "low_latency_calc"
        if flush_gap:
            options += "|flush_on_outer_close"
        if expire or qualify_value is not None:
            options += "|expired_time(5s)"
        if qualify_value is not None:
            state = "beat_p"
            self.predicate = (f"noise is null and beat_p is not null and "
                              f"(ts <> {self.base + filter_offsets[0]} or temp_p=100) and "
                              f"(ts <> {self.base + filter_offsets[1]} or temp_p={qualify_value})")

        aligned = "table(time_align(table {}, true, 'temp:PREV:_p;beat:PREV:_p'))"
        self.source = aligned.format("meter")
        trigger = aligned.format("meters") if grouped else self.source
        partition = "partition by tbname, site" if grouped else ""
        tags = "tags(origin_name varchar(64) as tbname)" if grouped else ""
        notifications = "notify('ws://localhost:12347/closed') on(window_close)" if notify else ""
        # Distinct end timestamps preserve evidence of a spurious early close even
        # after a later correction emits the same start with the proper end.
        tdSql.execute(
            f"create stream live window(session(ts,{flush_gap or '120s'}) as parent_win, state_window({state}) as leaf_win) "
            f"from {trigger} {partition} stream_options({options}|pre_filter({self.predicate})) "
            f"{notifications} into {self.result} {tags} as select leaf_win._twend as we, leaf_win._twstart as ws, "
            "leaf_win._twrownum as n, parent_win._twstart as parent_ws from probe"
        )
        tdStream.checkStreamStatus("live")

    def _insert(self, **tables):
        clauses = []
        for table, rows in tables.items():
            if rows is not None:
                values = " ".join(f"({self.base + offset},{value})" for offset, value in rows)
                clauses.append(f"{table} (ts, v) values {values}")
        tdSql.execute("insert into " + " ".join(clauses))

    def _prefix(self, at25=0, at30=100, interior=None):
        offsets = [0, 5000, 11000, 15000, 21000, 35000]
        if interior:
            offsets.append(interior)
        self._insert(therm=[(0, 100), (10000, 0), (20000, 100), (25000, at25), (30000, at30)],
                     ticks=[(offset, 1) for offset in sorted(offsets)])
        self._expect(0, 5000, 2)

    def _expect(self, start, end, count, origin=None, parent=0):
        condition = f" and origin_name='{origin}'" if origin else ""
        tdSql.checkResultsByFunc(
            sql=(f"select cast(ws as bigint), n, cast(parent_ws as bigint) from {self.result} "
                 f"where we={self.base + end}{condition}"),
            func=lambda: list(tdSql.queryResult) == [(self.base + start, count, self.base + parent)], retry=45,
        )

    def _submit_at_page_boundary(self, rows):
        # Keep the correction, real suffix and padding in one physical record;
        # a separate table adds a second metadata range for the same group.
        values = [f"({self.base + offset},{v},null)" for offset, v in rows]
        values.extend(f"({self.base + 41000 + i},null,1)" for i in range(4096))
        tdSql.execute(f"insert into {self.padded} values " + " ".join(values))

    def _aligned_windows(self):
        tdSql.query(f"select cast(_wstart as bigint), cast(_wend as bigint), count(*) "
                    f"from {self.source} where {self.predicate} state_window(temp_p) order by _wstart")
        return [(ws - self.base, we - self.base, n) for ws, we, n in tdSql.queryResult]

    @pytest.mark.parametrize("timing", ["same_record", "before_freeze", "after_freeze"])
    def test_ignored_delete_around_correction(self, timing):
        """Close the repaired STATE leaf using the heartbeat visible to its correction.

        1. Read a temperature and an independent heartbeat through a nested stream.
        2. Correct the open tail and close it, varying when an ordinary delete commits.
        3. Verify the correction's closed prefix and the next live leaf against SQL.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        self._prepare(f"ta_nested_{timing}")
        try:
            self._prefix()
            delete = f"delete from ticks where ts={self.base + 35000}"
            # Updating 10 also changes a previously closed leaf. Its new endpoint
            # makes the already captured correction observable through SQL.
            revised = [(10000, 100), (30000, 0)]
            if timing == "before_freeze":
                tdSql.execute(delete)
            if timing == "after_freeze":
                self._insert(therm=revised)
                self._expect(0, 21000, 7)
                assert self._aligned_windows()[-1] == (25000, 35000, 3)
                tdSql.execute(delete)
                self._insert(therm=[(40000, 100)])
            else:
                self._insert(therm=revised + [(40000, 100)])
            self._expect(0, 21000, 7)
            tail = (25000, 30000, 2) if timing == "before_freeze" else (25000, 35000, 3)
            physical_tail = tail if timing != "after_freeze" else (25000, 30000, 2)
            assert physical_tail in self._aligned_windows()
            self._expect(*tail)
        finally:
            tdSql.execute("drop stream if exists live")

    @pytest.mark.parametrize("interior", [32000, 34000])
    def test_delete_queued_behind_a_mixed_submit(self, interior):
        """Use the corrected closed prefix to identify which deletion the round saw.

        1. Submit two corrected readings and an ordinary row in the same WAL record.
        2. Erase heartbeats on both sides of a newly inserted state change in the next record.
        3. Verify the future closure includes exactly the rows visible in the same round.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        self._prepare(f"ta_nested_queued_delete_{interior}", padded="therm")
        try:
            self._prefix(at30=0, interior=interior)
            self._submit_at_page_boundary([(10000, 100), (33000, 100), (40000, 0)])
            retained_tail = (33000, 35000, 2 if interior == 32000 else 3)
            assert retained_tail in self._aligned_windows()
            tdSql.execute(f"delete from ticks where ts >= {self.base + 21000} and ts <= {self.base + 35000}")
            # The heartbeats disappear together in one version. The corrected earlier
            # window discloses which version was frozen without timing the server.
            before = (self.base + 21000, 7)
            after = (self.base + 20000, 6)
            tdSql.checkResultsByFunc(
                sql=(f"select cast(we as bigint), n from live_out where ws={self.base} "
                     f"and we between {self.base + 20000} and {self.base + 21000}"),
                func=lambda: list(tdSql.queryResult) in ([before], [after]), retry=45,
            )
            saw_delete = tdSql.queryResult[0] == after
            erased_tail = (33000, 33000, 1)
            assert erased_tail in self._aligned_windows()
            self._expect(*(erased_tail if saw_delete else retained_tail))
        finally:
            tdSql.execute("drop stream if exists live")

    @pytest.mark.parametrize("delete_heartbeats", [False, True])
    def test_no_mixed_source_boundaries(self, delete_heartbeats):
        """Keep a queued ordinary heartbeat and nested state on a common input basis.

        1. Update the earlier reading and append a NULL at 40 in the same submit.
        2. Insert a later version at 33 with and without an intervening ignored delete.
        3. Confirm the historical prefix reflects both the later reading and the visible heartbeats.
        4. Close the final high window and check its contents and the next ordinary closure.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        self._prepare(f"ta_nested_mixed_cut_{int(delete_heartbeats)}", padded="therm")
        try:
            self._prefix(at25=100, at30=0)
            self._submit_at_page_boundary([(25000, 0), (40000, "null")])
            assert self._aligned_windows()[-1] == (25000, 40000, 4)
            if delete_heartbeats:
                tdSql.execute(f"delete from ticks where ts >= {self.base + 15000} and ts <= {self.base + 35000}")
            self._insert(therm=[(12000, 200), (33000, 100)])
            closed_prefix = (12000, 12000, 1) if delete_heartbeats else (12000, 15000, 2)
            open_tail = (33000, 40000, 2 if delete_heartbeats else 3)
            windows = self._aligned_windows()
            assert closed_prefix in windows and windows[-1] == open_tail
            self._expect(*closed_prefix)
            self._insert(therm=[(50000, 0), (60000, 100)])
            self._expect(*open_tail)
            self._expect(50000, 50000, 1)
            tdSql.query(f"select ws from live_out where ws={self.base + 33000} and we={self.base + 35000}")
            tdSql.checkRows(0)
        finally:
            tdSql.execute("drop stream if exists live")

    @pytest.mark.parametrize("qualify_value", [0, 100])
    def test_normal_filter_uses_admitted_source(self, qualify_value):
        """Keep or reject a queued ordinary row on its original input version.

        1. Admit a correction at 33 and 34 with a normal phase transition at 40.
        2. Replace the temperature at 28, outside the five-second expiry horizon.
        3. Close two later leaves and check whether the original phase at 40 participated.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        db = f"ta_nested_filter_{qualify_value}"
        self._prepare(db, padded="ticks", qualify_value=qualify_value)
        try:
            self._insert(therm=[(0, 100), (28000, 0)],
                         ticks=[(0, 0), (5000, 0), (10000, 1), (20000, 0), (32000, 1), (35000, 3)])
            self._expect(0, 5000, 2)
            cursor = self._stream_log_cursor(db)
            self._submit_at_page_boundary([(33000, 4), (34000, 2), (40000, 5)])
            at40 = f"select temp_p, beat_p from {self.source} where ts={self.base + 40000}"
            tdSql.query(at40)
            assert list(tdSql.queryResult) == [(0, 5)]
            # The Reader and realtime Trigger must admit this submit under their original
            # source bound before the replacement. History can still freeze on either side.
            first, last = self.base + 33000, self.base + 45095
            self._wait_for_stream_markers(cursor, (
                f"scanSubmitData process meta data:skey {first}, ekey {last}, id ".encode(),
                f"add recalc request for disorder data, threshold: {self.base + 35000}, "
                f"gid: 0, start: {first}, end: {last}".encode(),
            ))
            # Even against the original H=35, the new value at 28 is expired.
            # A separate historical correction cannot add or remove the ordinary row at 40.
            self._insert(therm=[(28000, 100)])
            tdSql.query(at40)
            assert list(tdSql.queryResult) == [(100, 5)]
            self._expect(33000, 33000, 1)
            self._insert(ticks=[(50000, 6), (60000, 7)])
            self._expect(50000, 50000, 1)
            if qualify_value == 0:
                self._expect(40000, 40000, 1)
            tdSql.query(f"select cast(we as bigint), cast(ws as bigint), n from live_out "
                        f"where we in ({self.base + 34000}, {self.base + 40000}) order by we")
            observed = [(we - self.base, ws - self.base, n) for we, ws, n in tdSql.queryResult]
            tdLog.info(f"Repaired phase at 34 and normal phase at 40 for temp_p={qualify_value}: {observed}")
            assert [row for row in observed if row[0] == 34000] in ([], [(34000, 34000, 1)])
            assert [row for row in observed if row[0] == 40000] == ([(40000, 40000, 1)] if qualify_value == 0 else [])
        finally:
            tdSql.execute("drop stream if exists live")

    def test_expired_state_replacement_does_not_close_at_40(self):
        """Close a later stable leaf without publishing a STATE window from mixed versions.

        1. Append a late heartbeat at 32 and an ordinary heartbeat at 40 in one full page.
        2. Replace the temperature at 28 beyond the expiry horizon and read both input versions.
        3. Close the agreed successor at 70 and check that neither version closed a leaf at 35.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        self._prepare("ta_nested_expired_state", padded="ticks", expire=True)
        try:
            self._insert(therm=[(0, 100), (10000, 0), (20000, 100), (25000, 0)],
                         ticks=[(0, 1), (5000, 1), (11000, 1), (15000, 1), (21000, 1), (35000, 1)])
            self._expect(0, 5000, 2)
            self._submit_at_page_boundary([(32000, 1), (40000, 1)])
            assert self._aligned_windows()[-1] == (25000, 40000, 4)
            # At H=35, the timestamp at 28 is outside the five-second expiry horizon.
            self._insert(therm=[(28000, 100)])
            assert self._aligned_windows()[-1] == (28000, 40000, 4)
            self._insert(therm=[(50000, 0), (60000, 100), (70000, 0)])
            assert (60000, 60000, 1) in self._aligned_windows()
            self._expect(60000, 60000, 1)
            tdSql.query(f"select ws from live_out where ws={self.base + 28000} and we={self.base + 35000}")
            tdSql.checkRows(0)
        finally:
            tdSql.execute("drop stream if exists live")

    @pytest.mark.parametrize("grouped", [False, True])
    def test_repair_continues_across_reader_pages(self, grouped):
        """Drain a mixed submit, two input pages, and both partitioned groups.

        1. Start a SESSION containing STATE leaves for each of two virtual children.
        2. Correct the first group's active leaf and queue more than two reader pages.
        3. Check all subsequent closures with a four-parameter pending limit, including
           heartbeat filtering on the other source and the second group's independent values.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        self._prepare(f"ta_nested_pages_{int(grouped)}", grouped=grouped)
        try:
            self._prefix()
            if grouped:
                self._insert(peer_therm=[(0, 7)], peer_ticks=[(0, 1)])
            # The WAL reader finishes its current record at 4096 rows. Keep these
            # submits separate so the normal suffix cannot all be served in one page.
            temperatures = [(40000 + i, "null") for i in range(4096)]
            for run in range(16):
                offset = run * 256
                temperatures[offset] = (40000 + offset, 100 if run % 2 == 0 else 0)
            self._insert(therm=[(10000, 100), (30000, 0)] + temperatures,
                         peer_therm=[(40500, 8)] if grouped else None)
            self._insert(ticks=[(44096 + i, int(i >= 2048)) for i in range(4096)],
                         peer_ticks=[(46000, 1)] if grouped else None)
            self._insert(therm=[(50000, 100), (51000, 0)], peer_therm=[(50000, 9)] if grouped else None)

            tail = [(40000 + i * 256, 40255 + i * 256, 256) for i in range(15)]
            tail.extend([(43840, 48191, 2304), (50000, 50000, 1)])
            reference = self._aligned_windows()
            assert (25000, 35000, 3) in reference
            assert [row for row in reference if 40000 <= row[0] < 51000] == tail
            if grouped:
                self._expect(0, 0, 1, origin="peer")
                self._expect(40500, 46000, 2, origin="peer")
            origin = "origin_name='meter' and " if grouped else ""
            tdSql.checkResultsByFunc(
                sql=(f"select cast(ws as bigint), cast(we as bigint), n, cast(parent_ws as bigint) "
                     f"from live_out where {origin}we >= {self.base + 40000} order by we"),
                func=lambda: list(tdSql.queryResult) == [
                    (self.base + ws, self.base + we, n, self.base) for ws, we, n in tail
                ], retry=65,
            )
            self._expect(25000, 35000, 3, origin="meter" if grouped else None)
        finally:
            tdSql.execute("drop stream if exists live")

    def test_idle_vgroup_contributes_after_mixed_page(self):
        """Resume an initially idle source on a different vnode after a mixed WAL page.

        1. Bind temperature and heartbeat to two verified, distinct physical vgroups.
        2. Seed the heartbeat, then correct and advance temperature in a single full page.
        3. Send the first new heartbeat and count it once in the next closed STATE leaf.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        db = "ta_nested_idle_vgroup"
        tdSql.prepare(dbname=db, drop=True, vgroups=2)
        self.base = (int(time.time()) // 120 - 3) * 120000
        by_vgroup = {}
        for index in range(32):
            table = f"readings_{index}"
            tdSql.execute(f"create table {table} (ts timestamp, v int, noise int)")
            tdSql.query("select vgroup_id from information_schema.ins_tables "
                        f"where db_name='{db}' and table_name='{table}'")
            by_vgroup.setdefault(tdSql.queryResult[0][0], table)
            if len(by_vgroup) == 2:
                break
        assert len(by_vgroup) == 2, by_vgroup
        tdLog.info(f"Nested stream physical sources by vgroup: {by_vgroup}")
        therm, ticks = by_vgroup.values()
        self.padded = therm
        tdSql.execute(f"create vtable meter (ts timestamp, temp int from {therm}.v, "
                      f"beat int from {ticks}.v, noise int from {therm}.noise)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        self._insert(probe=[(0, 1)], **{ticks: [(-1000, 1)]})
        source = "table(time_align(table meter, true, 'temp:PREV:_p;beat:PREV:_p'))"
        state = "state_window(cast(temp_p + beat_p as bigint))"
        predicate = "noise is null and temp_p is not null and beat_p is not null"
        tdSql.execute(
            f"create stream live window(session(ts,120s) as parent_win, {state} as leaf_win) "
            f"from {source} stream_options(low_latency_calc|pre_filter({predicate})) into live_out "
            "as select leaf_win._twend as we, leaf_win._twstart as ws, leaf_win._twrownum as n, "
            "parent_win._twstart as parent_ws from probe"
        )
        try:
            tdStream.checkStreamStatus("live")
            self._insert(**{therm: [(0, 100), (5000, "null"), (10000, 0), (15000, "null"), (20000, 100),
                                   (25000, 0), (30000, 100), (35000, "null")]})
            self._expect(0, 5000, 2)
            self._submit_at_page_boundary([(10000, 100), (30000, 0), (40000, 100)])
            self._expect(0, 20000, 5)
            # The heartbeat vnode had no new WAL row while the mixed page was
            # admitted. Its first new value closes the temperature-only leaf.
            self._insert(**{ticks: [(46000, 2)]})
            self._expect(40000, 40000, 1)
            self._insert(**{therm: [(50000, 100), (60000, 0)]})
            tdSql.query(f"select cast(_wstart as bigint), cast(_wend as bigint), count(*) from {source} "
                        f"where {predicate} {state} order by _wstart")
            windows = [(ws - self.base, we - self.base, n) for ws, we, n in tdSql.queryResult]
            assert (25000, 35000, 3) in windows and (46000, 50000, 2) in windows
            self._expect(25000, 35000, 3)
            self._expect(46000, 50000, 2)
        finally:
            tdSql.execute("drop stream if exists live")

    def test_repair_survives_expired_change_before_reset(self):
        """Close the repaired leaf after an older write changes an earlier leaf.

        1. Advance a nested STATE window to 35 seconds with a five-second expiry.
        2. Correct the reading at 31 seconds and append the next state at 40 in one submit.
        3. Replace an expired reading before the state reset at 20 seconds.
        4. Compare both source versions, then verify the repaired and following live closures.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        self._prepare("ta_nested_expired_before_reset", padded="therm", expire=True)
        try:
            self._insert(therm=[(0, 100), (10000, 0), (20000, 100), (25000, 0), (31000, 100)],
                         ticks=[(ts, 1) for ts in (0, 5000, 11000, 15000, 21000, 35000)])
            self._expect(0, 5000, 2)
            self._submit_at_page_boundary([(31000, 0), (40000, 100)])
            repaired = (25000, 35000, 3)
            before = self._aligned_windows()
            assert (10000, 15000, 3) in before and repaired in before
            self._insert(therm=[(12000, 200)])
            after = self._aligned_windows()
            assert (12000, 15000, 2) in after and repaired in after
            self._insert(therm=[(50000, 0), (60000, 100)])
            self._expect(50000, 50000, 1)
            self._expect(*repaired)
        finally:
            tdSql.execute("drop stream if exists live")

    def _stream_log_lines(self, directory, offsets, stream_id, timeout):
        offsets = offsets.copy()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for path in sorted(directory.glob("taosdlog.[0-9]*"), key=lambda log: log.stat().st_mtime_ns):
                with path.open("rb") as log:
                    stat = path.stat()
                    offset = offsets.get(stat.st_ino, 0)
                    log.seek(offset if offset <= stat.st_size else 0)
                    for line in log:
                        if stream_id in line:
                            yield line
                    offsets[stat.st_ino] = log.tell()
            time.sleep(0.2)

    def _wait_for_repair_retirement(self, *cursor):
        self._wait_for_stream_markers(cursor, (b"nested retained WAL proof", b"[trigger history] destroy groups"))

    def _wait_for_stream_markers(self, cursor, markers):
        expected = iter(markers)
        marker = next(expected)
        for line in self._stream_log_lines(*cursor, timeout=65):
            if marker in line:
                tdLog.debug(line.decode(errors="replace").strip())
                marker = next(expected, None)
                if marker is None:
                    return
        pytest.fail(f"Stream {cursor[2]!r} did not reach {marker!r} before the next write")

    def _wait_for_done_version(self, cursor, start, end):
        marker = f"scanSubmitData process meta data:skey {self.base + start}, ekey {self.base + end},".encode()
        submitted = None
        completed = None
        # Reader progress statistics are published every three minutes.
        for line in self._stream_log_lines(*cursor, timeout=225):
            if marker in line:
                submitted = int(line.split(b"ver:", 1)[1])
            if submitted is not None and b"record=reader_progress " in line:
                match = re.search(rb" done_ver=(\d+) ", line)
                if match:
                    completed = int(match[1])
                    if completed > submitted:
                        tdLog.info(f"Completed WAL cursor {completed} passed input version {submitted}")
                        return
        pytest.fail(f"Completed WAL cursor {completed} did not pass input version {submitted}")

    def _stream_log_cursor(self, db):
        directory = Path(tdDnodes.getDnodeDir(1)) / "log"
        offsets = {}
        for path in directory.glob("taosdlog.[0-9]*"):
            stat = path.stat()
            offsets[stat.st_ino] = stat.st_size
        tdSql.query(f"select stream_id from information_schema.ins_streams "
                    f"where db_name='{db}' and stream_name='live'")
        stream_id = f"STREAM:{int(str(tdSql.queryResult[0][0]), 16):x},".encode()
        return directory, offsets, stream_id

    def _submit_two_pages(self):
        self._insert(therm=[(0, 100), (28000, 0)],
                     ticks=[(0, 0), (5000, 0), (10000, 1), (20000, 1), (25000, 2), (31000, 3), (33000, 2), (35000, 2)])
        self._expect(0, 5000, 2)
        self._submit_at_page_boundary([(31000, 2), (40000, 2)])
        data = [f"({self.base + 46000},3,null)"]
        data.extend(f"({self.base + 47000 + i},null,1)" for i in range(4096))
        tdSql.execute(f"insert into therm (ts, v) values ({self.base + 46000},0) ticks values " + " ".join(data))
        tdSql.query(f"select temp_p, beat_p from {self.source} where ts={self.base + 46000}")
        assert list(tdSql.queryResult) == [(0, 3)]
        reference = (f"select cast(_wstart as bigint), cast(_wend as bigint), count(*) from {self.source} "
                     f"where {self.predicate} state_window(beat_p) order by _wstart")
        tdSql.query(reference)
        before = [(ws - self.base, we - self.base, n) for ws, we, n in tdSql.queryResult]
        assert (25000, 40000, 5) in before
        self._insert(therm=[(28000, 100), (46000, 100)])
        tdSql.query(f"select temp_p, beat_p from {self.source} where ts={self.base + 46000}")
        assert list(tdSql.queryResult) == [(100, 3)]
        return reference

    @pytest.mark.parametrize("after_catchup", [False, True], ids=["within_frozen_input", "after_catchup"])
    def test_second_page_before_frozen_cut(self, after_catchup):
        """Close the corrected leaf when a second ordinary page disagrees with frozen history.

        1. Correct a STATE window and append two ordinary pages before history freezes.
        2. Check both source versions and send successors either immediately or after history retires.
        3. Wait for a later window to close, reject a mixed count, and check that the correction closed its leaf.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        db = "ta_nested_after_catchup" if after_catchup else "ta_nested_second_page"
        self._prepare(db, padded="ticks", qualify_value=0, filter_offsets=(33000, 46000))
        try:
            if after_catchup:
                cursor = self._stream_log_cursor(db)
            reference = self._submit_two_pages()
            if after_catchup:
                self._wait_for_repair_retirement(*cursor)
            self._insert(ticks=[(55000, 2), (60000, 3), (70000, 4)])
            tdSql.query(reference)
            after = [(ws - self.base, we - self.base, n) for ws, we, n in tdSql.queryResult]
            assert not any(we == 40000 for _, we, _ in after)
            assert (25000, 55000, 7) in after
            self._expect(60000, 60000, 1)
            tdSql.checkResultsByFunc(
                sql=(f"select cast(we as bigint), n from live_out where ws={self.base + 25000} "
                     f"and we in ({self.base + 40000},{self.base + 55000})"),
                func=lambda: any((end - self.base, n) in {(40000, 5), (55000, 7)} for end, n in tdSql.queryResult),
                retry=45,
            )
            tdSql.query("select cast(ws as bigint), cast(we as bigint), n from live_out order by we")
            windows = [(ws - self.base, we - self.base, n) for ws, we, n in tdSql.queryResult]
            assert (25000, 40000, 6) not in windows
        finally:
            tdSql.execute("drop stream if exists live")

    def _restart_dnode(self, wait_for_progress, cursor, start, end):
        if wait_for_progress:
            self._wait_for_done_version(cursor, start, end)
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdStream.checkStreamStatus("live")

    @pytest.fixture
    def close_log(self, tmp_path):
        start_notify_server_background(12347, str(tmp_path))
        try:
            yield tmp_path / "closed.log"
        finally:
            stop_notify_server_background()

    def _check_close_counts(self, path, expected):
        closed = []
        stable_since = None
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                events = NotifyLog(path).events()
            except (FileNotFoundError, ValueError):
                time.sleep(0.2)
                continue
            closed = [(e.windowStart - self.base, e.windowEnd - self.base) for e in events]
            if all(closed.count(window) == count for window, count in expected.items()):
                if stable_since is None:
                    stable_since = time.monotonic()
                if time.monotonic() - stable_since >= 2:
                    return
            else:
                stable_since = None
            time.sleep(0.2)
        pytest.fail(f"Expected close counts {expected}, received {closed}")

    @pytest.mark.parametrize("wait_for_progress", [False, True], ids=["after_output", "after_completed_cursor"])
    def test_restart_with_pending_correction(self, close_log, wait_for_progress):
        """Resume an accepted correction after its history group has retired.

        1. Deliver a correction inside the expiry horizon and wait for its historical round to retire.
        2. Restart immediately or after the completed WAL cursor advances, then close the ordinary and repaired leaves.
        3. Restart at the same point relative to the final output and verify each target close is notified once.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        db = "ta_nested_restart_accepted"
        self._prepare(db, padded="ticks", qualify_value=0, filter_offsets=(33000, 46000), notify=True)
        try:
            cursor = self._stream_log_cursor(db)
            self._submit_two_pages()
            self._wait_for_repair_retirement(*cursor)
            self._restart_dnode(wait_for_progress, cursor, 28000, 46000)
            cursor = self._stream_log_cursor(db)
            self._insert(ticks=[(55000, 2), (60000, 3), (70000, 4)])
            self._expect(60000, 60000, 1)
            self._expect(46000, 46000, 1)
            self._expect(25000, 55000, 7)
            tdSql.query(f"select n from live_out where ws={self.base + 25000} and we={self.base + 40000} and n=6")
            tdSql.checkRows(0)

            self._restart_dnode(wait_for_progress, cursor, 55000, 70000)
            self._insert(ticks=[(80000, 5), (90000, 6)])
            self._expect(80000, 80000, 1)
            self._check_close_counts(close_log, {(0, 5000): 1, (46000, 46000): 1, (25000, 55000): 1,
                                                (60000, 60000): 1, (80000, 80000): 1})
        finally:
            tdSql.execute("drop stream if exists live")

    @pytest.mark.parametrize("wait_for_progress", [False, True], ids=["after_output", "after_completed_cursor"])
    def test_restart_after_expired_correction(self, close_log, wait_for_progress):
        """Keep an expired correction out of a recovered open STATE leaf.

        1. Observe the state change at 40 before replacing the reading at 31 with a five-second expiry.
        2. Close a peer window on the same vnode and restart immediately or after the completed WAL cursor advances.
        3. Close the ordinary leaf and compare it with the longer window visible in current history.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        db = "ta_nested_restart_expired"
        self._prepare(db, grouped=True, expire=True, notify=True)
        try:
            cursor = self._stream_log_cursor(db)
            self._insert(therm=[(0, 100), (10000, 0), (20000, 100), (25000, 0), (31000, 100), (40000, 0)],
                         ticks=[(0, 1), (5000, 1), (35000, 1)], peer_therm=[(0, 0)], peer_ticks=[(0, 1)])
            self._expect(31000, 35000, 2, origin="meter")
            self._insert(therm=[(31000, 0)])
            self._insert(peer_therm=[(46000, 1)])
            self._expect(0, 0, 1, origin="peer")
            self._restart_dnode(wait_for_progress, cursor, 46000, 46000)
            self._insert(ticks=[(55000, 1)], therm=[(60000, 100), (70000, 0)])
            self._expect(60000, 60000, 1, origin="meter")
            assert (25000, 55000, 5) in self._aligned_windows()
            self._expect(40000, 55000, 2, origin="meter")
            self._check_close_counts(close_log, {(31000, 35000): 1, (25000, 55000): 0, (40000, 55000): 1,
                                                (60000, 60000): 1})
        finally:
            tdSql.execute("drop stream if exists live")

    @pytest.fixture(scope="class")
    def ordinary_restart_base(self):
        return (int(time.time()) // 120 - 3) * 120000

    @pytest.mark.parametrize("separate", [True, False], ids=["separate", "coalesced"])
    def test_restart_preserves_ordinary_poll_boundary(self, separate, ordinary_restart_base):
        """Preserve the consumed ordinary STATE window across a dnode restart.

        1. Submit identical source WAL with the expired replacement in a separate read
           or coalesced with the initial rows.
        2. Wait for the completed WAL cursor and verify the initial closure for each schedule.
        3. Restart, close the recovered leaf using a heartbeat and verify its start,
           count, outer SESSION start, and successor against the current PREV query.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        db = f"ta_nested_ordinary_{int(separate)}"
        # A live poll can fall between the three submits. Retry only the warm-up
        # on a fresh source when the requested coalesced read did not occur.
        for _ in range(1 if separate else 3):
            self._prepare(db, grouped=True, expire=True, separate_output=True, base=ordinary_restart_base)
            try:
                cursor = self._stream_log_cursor(db)
                self._insert(therm=[(0, 100), (10000, 0), (20000, 100), (25000, 0), (31000, 100), (40000, 0)],
                             ticks=[(0, 1), (5000, 1), (35000, 1)], peer_therm=[(0, 0)], peer_ticks=[(0, 1)])
                if separate:
                    self._expect(31000, 35000, 2, origin="meter")
                self._insert(therm=[(31000, 0)])
                self._insert(peer_therm=[(46000, 1)])
                self._expect(0, 0, 1, origin="peer")
                self._wait_for_done_version(cursor, 46000, 46000)

                # A separate read closes the high leaf at 35 before the expired replacement.
                tdSql.query(f"select cast(ws as bigint), n from {self.result} "
                            f"where origin_name='meter' and we={self.base + 35000}")
                separated_leaf = [(self.base + 31000, 2)]
                observed = list(tdSql.queryResult)
                if not separate and observed == separated_leaf:
                    continue
                assert observed == (separated_leaf if separate else [])
                self._restart_dnode(False, cursor, 46000, 46000)

                self._insert(ticks=[(55000, 1)], therm=[(60000, 100), (70000, 0)])
                self._expect(60000, 60000, 1, origin="meter")
                assert (25000, 55000, 5) in self._aligned_windows()
                tdSql.query(f"select cast(ws as bigint), n, cast(parent_ws as bigint) from {self.result} "
                            f"where origin_name='meter' and we={self.base + 55000}")
                start, count = (40000, 2) if separate else (25000, 5)
                assert list(tdSql.queryResult) == [(self.base + start, count, self.base)]
                return
            finally:
                tdSql.execute("drop stream if exists live")
                tdSql.execute(f"drop database if exists {db}_out")
        pytest.fail(f"Could not establish the requested ordinary poll boundary (separate={separate})")

    def test_filtered_final_page_advances_session(self):
        """Close a corrected nested leaf when the final page advances the session clock.

        1. Correct a STATE leaf inside a session with a 5.5-second gap.
        2. Filter out the last heartbeat in the frozen history and wait for that history group to retire.
        3. Check the corrected close after the gap expires, without sending any further rows.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        db = "ta_nested_filtered_final_page"
        self._prepare(db, padded="ticks", qualify_value=0, filter_offsets=(33000, 46000), flush_gap="5500a")
        try:
            cursor = self._stream_log_cursor(db)
            reference = self._submit_two_pages()
            tdSql.query(reference)
            assert (self.base + 25000, self.base + 40000, 6) in tdSql.queryResult
            self._wait_for_repair_retirement(*cursor)
            tdSql.checkResultsByFunc(
                sql=f"select cast(ws as bigint), n from live_out where we={self.base + 40000}",
                func=lambda: any(start == self.base + 25000 and count in (5, 6) for start, count in tdSql.queryResult),
                retry=45,
            )
        finally:
            tdSql.execute("drop stream if exists live")

    def test_corrected_event_parent_closes_new_leaf(self):
        """Close a leaf opened after the frontier under a parent opened by the eligible correction.

        1. Process the ordinary input with no event start at the 31-second marker.
        2. Correct that marker and open a child with a different state on the first ordinary page.
        3. Verify both child closes and their corrected parent when the second page changes state.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-21 OpenAI Created
        """
        tdSql.prepare(dbname="ta_nested_repaired_event_parent", drop=True, vgroups=1)
        self.base = base = (int(time.time()) // 120 - 3) * 120000
        tdSql.execute("create table probe(ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({base}, 1)")
        tdSql.execute("create table ticks(ts timestamp, marker int, beat int, noise int)")
        tdSql.execute("create table therm(ts timestamp, v int)")
        tdSql.execute("create vtable meter(ts timestamp, m int from ticks.marker, b int from ticks.beat, "
                      "gate int from therm.v, noise int from ticks.noise)")
        source = "table(time_align(table meter, true, 'm:PREV:_p;b:PREV:_p;gate:PREV:_p'))"
        predicate = f"noise is null and (ts <> {base + 46000} or gate_p=100)"
        tdSql.execute(
            f"create stream live window(event_window(start with ts={base} or (ts={base + 31000} and m_p=1) "
            f"end with ts={base + 10000} or ts={base + 70000}) as parent_win, state_window(b_p) as leaf_win) "
            f"from {source} stream_options(low_latency_calc|expired_time(5s)|pre_filter({predicate})) "
            "into live_out as select leaf_win._twend as we, leaf_win._twstart as ws, "
            "leaf_win._twrownum as n, parent_win._twstart as parent_ws from probe"
        )
        try:
            tdStream.checkStreamStatus("live")
            prefix = [(0, 0), (5000, 1), (10000, 1), (20000, 2), (25000, 2), (31000, 2), (35000, 2)]
            tdSql.execute(f"insert into therm values ({base},0) ({base + 28000},0) ticks values " +
                          " ".join(f"({base + ts},0,{beat},null)" for ts, beat in prefix))
            self._expect(0, 0, 1)
            first = [f"({base + 31000},1,2,null)", f"({base + 40000},null,3,null)"]
            first.extend(f"({base + 41000 + i},null,null,1)" for i in range(4096))
            tdSql.execute("insert into ticks values " + " ".join(first))
            second = [f"({base + 46000},null,4,null)", f"({base + 47000},null,4,null)"]
            second.extend(f"({base + 48000 + i},null,null,1)" for i in range(4096))
            tdSql.execute(f"insert into therm values ({base + 46000},0) ticks values " + " ".join(second))
            reference = f"select cast(ts as bigint), m_p, b_p from {source} where {predicate} and ts>={base + 31000} order by ts"
            tdSql.query(reference)
            before = [(ts - base, marker, beat) for ts, marker, beat in tdSql.queryResult]
            assert before == [(31000, 1, 2), (35000, 0, 2), (40000, 0, 3), (47000, 0, 4)]
            self._insert(therm=[(28000, 100), (46000, 100)])
            tdSql.query(reference)
            after = [(ts - base, marker, beat) for ts, marker, beat in tdSql.queryResult]
            assert after == before[:-1] + [(46000, 0, 4), (47000, 0, 4)]
            self._expect(31000, 35000, 2, parent=31000)
            self._expect(40000, 40000, 1, parent=31000)
        finally:
            tdSql.execute("drop stream if exists live")
