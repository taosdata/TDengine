# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestExternalWindowOutputFilter:
    db = "ext_window_output_filter"
    base = 1793000000000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.ensureSnode()

    def prepare_source(self, virtual):
        tdSql.execute(f"drop database if exists {self.db}")
        tdSql.execute(f"create database {self.db} vgroups 1")
        tdSql.execute(f"create table {self.db}.src (ts timestamp, state_value int)")
        if virtual:
            tdSql.execute(
                f"create stable {self.db}.vst (ts timestamp, state_value int) "
                "tags (gid int) virtual 1"
            )
            tdSql.execute(
                f"create vtable {self.db}.vt (state_value from {self.db}.src.state_value) "
                f"using {self.db}.vst tags (1)"
            )
        return f"{self.db}.{'vt' if virtual else 'src'}"

    def insert_rows(self, rows):
        values = " ".join(f"({self.base + offset}, {state})" for offset, state in rows)
        tdSql.execute(f"insert into {self.db}.src values {values}")

    def wait_last_output(self, offset):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            tdSql.query(
                f"select * from {self.db}.result where output_timestamp={self.base + offset}"
            )
            if tdSql.queryResult:
                return
            time.sleep(0.2)
        raise AssertionError(f"later window at {offset} was not emitted")

    def wait_history(self):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            tdSql.query(
                "select history_progress_pct from information_schema.ins_streams "
                f"where db_name='{self.db}' and stream_name='s'"
            )
            if tdSql.queryResult and tdSql.getData(0, 0) == 100:
                return
            time.sleep(0.2)
        raise AssertionError("history calculation did not finish")

    @pytest.mark.parametrize("virtual", [False, True])
    @pytest.mark.parametrize("history", [False, True])
    def test_state_window_scalar_output_filter(self, virtual, history):
        """Filter scalar external-window results by state and window duration.

        Verify the customer's nested STATE_WINDOW query for physical and virtual
        tables, in history and realtime processing. Reject short state 103 and
        duration exactly 120000, retain longer state 103 and later other states.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.3.6

        Labels: common,ci

        Feishu: None

        History:
            - 2026-10-26 Add scalar output predicate regression.
        """
        source = self.prepare_source(virtual)
        rows = [
            (0, 103), (5000, 103), (10000, 3), (15000, 3),
            (20000, 103), (140000, 103), (140001, 2), (145000, 2),
            (150000, 103), (270001, 103), (280001, 4), (285000, 4),
            (290000, 5), (295000, 5), (300000, 6),
        ]
        if history:
            self.insert_rows(rows)
        history_option = f" | fill_history({self.base})" if history else ""
        tdSql.execute(
            f"create stream {self.db}.s state_window(state_value) extend(1) true_for(3s) "
            f"from {source} stream_options(ignore_disorder{history_option}) "
            f"into {self.db}.result nodelay_create_subtable as select * from ("
            "select _twstart+0s as output_timestamp, state_value as stateCorr, "
            f"_twduration as wduration from {source} "
            "where ts >= _twstart and ts <= _twend) "
            "where stateCorr <> 103 or wduration > 120000"
        )
        tdStream.waitStreamStatus(self.db, "s", "Running")
        if not history:
            self.insert_rows(rows)
        self.wait_last_output(290000)
        if history:
            self.wait_history()
        tdSql.query(
            "select cast(output_timestamp as bigint), stateCorr, wduration "
            f"from {self.db}.result order by output_timestamp"
        )
        expected = [(10000, 3, 9999), (140001, 2, 9998), (150000, 103, 130000),
                    (280001, 4, 9998), (290000, 5, 9999)]
        tdSql.checkRows(len(expected))
        for row, (offset, state, duration) in enumerate(expected):
            tdSql.checkData(row, 0, self.base + offset)
            tdSql.checkData(row, 1, state)
            tdSql.checkData(row, 2, duration)

    def test_scalar_filter_continues_after_empty_block(self):
        """Continue external-window output after a whole block is filtered.

        A first state window fills multiple result blocks with rejected rows.
        A later state window must still be emitted after filtering those blocks.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.3.6

        Labels: common,ci

        Feishu: None

        History:
            - 2026-10-26 Cover fully filtered blocks and later windows.
        """
        source = self.prepare_source(False)
        for start in range(0, 20000, 1000):
            self.insert_rows([(i, 103) for i in range(start, start + 1000)])
        self.insert_rows([(30000, 3), (35000, 3), (40000, 4)])
        tdSql.execute(
            f"create stream {self.db}.s state_window(state_value) extend(1) true_for(3s) "
            f"from {source} stream_options(fill_history({self.base})) "
            f"into {self.db}.result nodelay_create_subtable as select * from ("
            "select ts as output_timestamp, state_value as stateCorr, _twduration as wduration "
            f"from {source} where ts >= _twstart and ts <= _twend) "
            "where stateCorr <> 103 or wduration > 120000"
        )
        tdStream.waitStreamStatus(self.db, "s", "Running")
        self.wait_last_output(35000)
        tdSql.query(f"select count(*) from {self.db}.result where stateCorr=103")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select stateCorr, wduration from {self.db}.result where stateCorr=3")
        tdSql.checkRows(2)
        for row in range(2):
            tdSql.checkData(row, 0, 3)
            tdSql.checkData(row, 1, 9999)

    @pytest.mark.parametrize(
        "true_for,tail_offsets,expected_duration",
        [("3s", [10000], None), ("3s", [10000, 13000], 3000),
         ("count 2", [10000], None), ("count 2", [10000, 10001], 1),
         ("3s and count 3", [10000, 13000], None),
         ("3s or count 3", [10000, 10001, 10002], 2)],
    )
    def test_history_tail_respects_true_for(self, true_for, tail_offsets, expected_duration):
        """Apply TRUE_FOR when history forces the last state window to close.

        Reject an insufficient tail without losing preceding valid windows.
        Retain tails that meet duration, count, AND or OR conditions.

        Catalog:
            - Streams:Trigger:StateWindow

        Since: v3.4.3.6

        Labels: common,ci

        Feishu: None

        History:
            - 2026-10-26 Cover TRUE_FOR on history force-close.
        """
        source = self.prepare_source(False)
        self.insert_rows([(0, 1), (2500, 1), (5000, 1)] + [(ts, 3) for ts in tail_offsets])
        tdSql.execute(
            f"create stream {self.db}.s state_window(state_value) extend(1) "
            f"true_for({true_for}) from {source} stream_options(fill_history({self.base})) "
            f"into {self.db}.result nodelay_create_subtable as "
            "select _twstart as output_timestamp, _twduration as wduration "
            f"from {source} where ts >= _twstart and ts <= _twend"
        )
        tdStream.waitStreamStatus(self.db, "s", "Running")
        self.wait_history()
        self.wait_last_output(0)
        tdSql.query(f"select wduration from {self.db}.result order by output_timestamp")
        tdSql.checkRows(1 if expected_duration is None else 2)
        tdSql.checkData(0, 0, 9999)
        if expected_duration is not None:
            tdSql.checkData(1, 0, expected_duration)
