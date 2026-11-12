# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStateOpenCloseRecalc:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def _rows(sql):
        tdSql.query(sql, queryTimes=1)
        return list(tdSql.queryResult)

    def _wait_rows(self, sql, predicate):
        deadline = time.monotonic() + 60
        rows = []
        while time.monotonic() < deadline:
            rows = self._rows(sql)
            if predicate(rows):
                return rows
            time.sleep(0.2)
        raise AssertionError(f"query did not reach expected result: {sql}; {rows!r}")

    @pytest.mark.parametrize("virtual", [False, True], ids=["ordinary", "virtual"])
    @pytest.mark.parametrize("manual", [False, True], ids=["history", "manual"])
    def test_state_open_close_preserves_start_row(self, virtual, manual):
        """Overlapping OPEN and CLOSE calculations each read the starting state.

        1. Replay a state window containing a later row with a NULL state.
        2. Compare OPEN/CLOSE output with a CLOSE-only control on ordinary and
           sparse virtual sources, including independent event rows.
        3. For manual replay, establish a realtime group, update a historical
           reason code, and verify Finished means the updated result was written.

        Catalog: Streams:Recalculation

        Since: v3.4.3.11

        Labels: stream,recalc,state-window,virtual-table,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7166751399

        History:
            - 2026-11-12 OpenAI Created
        """
        tdStream.ensureSnode(1)
        db = f"state_oc_{int(virtual)}_{int(manual)}"
        base = int(time.time() * 1000) - 3600000
        start, middle, end = base + 300000, base + 340000, base + 442007
        tdSql.executes([
            f"drop database if exists {db}",
            f"create database {db} vgroups 1",
        ])
        if virtual:
            tdSql.executes([
                f"create table {db}.states (ts timestamp, stval int)",
                f"create table {db}.attrs (ts timestamp, standard double, code bigint)",
                f"create stable {db}.vst (ts timestamp, stval int, standard double, "
                "code bigint) tags(site int) virtual 1",
                f"create vtable {db}.vt (stval from {db}.states.stval, "
                f"standard from {db}.attrs.standard, code from {db}.attrs.code) "
                f"using {db}.vst tags(1)",
                f"insert into {db}.states values ({base},103)({start},105)({end + 1},4)",
                f"insert into {db}.attrs values ({base},10,405001000)"
                f"({start},10,405001000)({middle},10,405001000)({end + 1},10,405001000)",
            ])
            source, partition, calc_source = f"{db}.vst", "partition by tbname", "%%tbname"
        else:
            tdSql.executes([
                f"create table {db}.raw (ts timestamp, stval int, standard double, code bigint)",
                f"insert into {db}.raw values ({base},103,10,405001000)"
                f"({start},105,10,405001000)({middle},NULL,10,405001000)"
                f"({end + 1},4,10,405001000)",
            ])
            source, partition, calc_source = f"{db}.raw", "", f"{db}.raw"

        names = [f"{db}_both", f"{db}_close"]
        for name, events in zip(names, ["window_open|window_close", "window_close"]):
            tdSql.execute(
                f"create stream {db}.{name} state_window(stval) extend(1) true_for(60s) "
                f"from {source} {partition} stream_options(fill_history({base})|"
                f"event_type({events})|ignore_disorder|max_delay(30s)|watermark(20s)) "
                f"into {db}.out_{name} nodelay_create_subtable as select "
                "_twend as ts, _twstart as ws, _twduration as duration_ms, "
                "last(stval) as last_state, count(*) as cnt, count(stval) as state_count, "
                "case when last(stval)=105 then _twduration/1000 end as duration105, "
                "case when last(stval)=105 and _twduration<last(standard)*60000 "
                "then last(code) end as reason "
                f"from {calc_source} where ts>=_twstart and ts<=_twend"
            )
        tdStream.checkStreamStatus()
        for name in names:
            self._wait_rows(
                f"select count(*) from {db}.out_{name} where ts={end}",
                lambda rows: rows == [(1,)],
            )

        reason = 405001000
        if manual:
            # Actual realtime output proves a group exists for manual replay.
            recent = int(time.time() * 1000) + 1000
            for i in range(3):
                ts = recent + i * 120000
                if virtual:
                    tdSql.execute(f"insert into {db}.attrs values ({ts},10,405001000)")
                    tdSql.execute(f"insert into {db}.states values ({ts},{106 + i})")
                else:
                    tdSql.execute(f"insert into {db}.raw values ({ts},{106 + i},10,405001000)")
            for name in names:
                self._wait_rows(
                    f"select count(*) from {db}.out_{name} where ws={recent}",
                    lambda rows: rows[0][0] > 0,
                )
            reason = 405001001
            update = f"{db}.attrs values ({middle},10,{reason})" if virtual else (
                f"{db}.raw values ({middle},NULL,10,{reason})"
            )
            tdSql.execute(f"insert into {update}")
            for name in names:
                tdSql.execute(f"recalculate stream {db}.{name} from {base} to {end + 1}")
                status = self._wait_rows(
                    "select status,progress,message from information_schema.ins_stream_recalculates "
                    f"where stream_name='{name}'",
                    lambda rows: len(rows) == 1 and rows[0][0] in ("Finished", "Failed"),
                )
                assert status == [("Finished", "100%", None)], (name, status)

        for name in names:
            rows = self._rows(
                "select cast(ts as bigint),duration_ms,last_state,cnt,state_count,duration105,reason "
                f"from {db}.out_{name} where ws={start} order by ts"
            )
            expected = [(end, 142007, 105, 2, 1, 142.007, reason)]
            if name.endswith("_both"):
                expected.insert(0, (start, 0, 105, 1, 1, 0.0, 405001000))
            assert rows == expected, (name, rows, expected)
