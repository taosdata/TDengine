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

from new_test_framework.utils import tdLog, tdSql, etool

import math
import time

from taos.bind import new_bind_params


class TestStreamDerivativeTag:
    BASE_MS = 1793491200000  # 2026-11-01 00:00:00 UTC.

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def prepare(self, precision="ms"):
        self.db = f"stream_derivative_tag_{precision}"
        self.scale = {"ms": 1, "us": 1000, "ns": 1000000}[precision]
        self.base = self.BASE_MS * self.scale
        tdSql.execute(f"drop database if exists {self.db}")
        tdSql.execute(f"create database {self.db} precision '{precision}' keep 36500 vgroups 2")
        tdSql.execute(f"use {self.db}")
        tdSql.query("show snodes")
        if tdSql.queryRows == 0:
            tdSql.execute("create snode on dnode 1")
        tdSql.execute(
            "create stable src (ts timestamp, v double, unit_col bigint) tags "
            "(unit_text varchar(64), unit_wide nchar(64), unit_ticks bigint, "
            "unit_unsigned bigint unsigned, unsupported double, gid int)"
        )
        for table, text, wide, ticks in (
            ("d1", "'1s'", "'2s'", 1000 * self.scale),
            ("d2", "'2s'", "'1m'", 2000 * self.scale),
            ("bad", "'999a'", "'1s'", 1000 * self.scale),
            ("missing", "NULL", "'1s'", 1000 * self.scale),
        ):
            tdSql.execute(
                f"create table {table} using src tags "
                f"({text},{wide},{ticks},{ticks},1.0,1)"
            )

    def stream(self, name, select, source="%%tbname", trigger="count_window(3)",
               table="src", partition="partition by tbname"):
        return (
            f"create stream {name} {trigger} from {table} {partition} "
            "stream_options(event_type(window_close)) "
            f"into out_{name} as select ts as result_ts, {select} from {source} "
            + ("" if source == "%%trows" else "where ts >= _twstart and ts <= _twend")
        )

    def wait_running(self, count):
        for _ in range(300):
            tdSql.query(
                "select status from information_schema.ins_streams "
                f"where db_name='{self.db}'"
            )
            if tdSql.queryRows == count and all(row[0] == "Running" for row in tdSql.queryResult):
                return
            time.sleep(0.1)
        raise AssertionError(f"streams did not start: {tdSql.queryResult}")

    def wait_rows(self, table, count):
        for _ in range(300):
            tdSql.query(f"select count(*) from {table}")
            if tdSql.getData(0, 0) == count:
                return
            time.sleep(0.1)
        tdSql.checkData(0, 0, count)

    def write(self, tables, batch=0, values=(100, 110, 120)):
        for table in tables:
            rows = " ".join(
                f"({self.base + (batch * 30000 + i * 5000) * self.scale},{value if value is not None else 'NULL'},1000)"
                for i, value in enumerate(values)
            )
            tdSql.execute(f"insert into {table} values {rows}")

    # --- impl ---
    def do_types_and_updates(self):
        for precision in ("ms", "us", "ns"):
            self.prepare(precision)
            # Empty supertables must also be accepted; populated tables start with no samples.
            for name, source in (("native", "%%tbname"), ("cached", "%%trows")):
                tdSql.execute(self.stream(
                    name,
                    "tbname as device, unit_text as used_unit, "
                    "DERIVATIVE(v, unit_text, 0) as rate, "
                    "derivative(v, unit_wide, 0) as wide_rate, "
                    "derivative(v, unit_ticks, 0) as tick_rate, "
                    "derivative(v, unit_unsigned, 0) as unsigned_rate, "
                    "derivative(v, 1s, 0) as original_rate",
                    source=source,
                ))
            self.wait_running(2)
            tdSql.error("alter stable src drop tag unit_text")
            self.write(("d1", "d2", "bad", "missing"))
            for name in ("native", "cached"):
                self.wait_rows(f"out_{name}", 8)
                tdSql.query(
                    f"select device, used_unit, rate, wide_rate, tick_rate, unsigned_rate, original_rate "
                    f"from out_{name} order by device, result_ts"
                )
                for i, expected in enumerate((
                    ("bad", "999a", None, 2.0, 2.0, 2.0, 2.0),
                    ("d1", "1s", 2.0, 4.0, 2.0, 2.0, 2.0),
                    ("d2", "2s", 4.0, 120.0, 4.0, 4.0, 2.0),
                    ("missing", None, None, 2.0, 2.0, 2.0, 2.0),
                )):
                    for row in (i * 2, i * 2 + 1):
                        for col, value in enumerate(expected):
                            tdSql.checkData(row, col, value)
            for batch, unit, expected in ((1, "'5s'", 10.0), (2, "'oops'", None), (3, "'2s'", 4.0)):
                tdSql.execute(f"alter table d1 set tag unit_text={unit}")
                # The reader refresh is asynchronous; later batches must observe the new tag.
                time.sleep(2)
                self.write(("d1",), batch)
                for name in ("native", "cached"):
                    self.wait_rows(f"out_{name}", 8 + batch * 2)
                    tdSql.query(
                        f"select used_unit, rate, original_rate from out_{name} "
                        f"where device='d1' and result_ts >= {self.base + batch * 30000 * self.scale} "
                        "order by result_ts"
                    )
                    tdSql.checkRows(2)
                    for row in range(2):
                        tdSql.checkData(row, 0, unit.strip("'"))
                        tdSql.checkData(row, 1, expected)
                        tdSql.checkData(row, 2, 2.0)
            tdSql.execute(f"drop database {self.db}")
        print("stream DERIVATIVE tag types and updates ................ [ passed ]")

    def do_validation_and_original_path(self):
        self.prepare()
        self.write(("d1",), values=(100, None, 110, 100, 120))
        for unit in ("1s", "1000"):
            tdSql.query(f"select derivative(v,{unit},0) from d1")
            tdSql.checkRows(3)
            for row, value in enumerate((1.0, -2.0, 4.0)):
                tdSql.checkData(row, 0, value)
        tdSql.query("select derivative(v,1s,1) from d1")
        tdSql.checkRows(2)
        for expr in ("unit_text", "unit_ticks", "cast(unit_ticks as bigint)", "unit_ticks+0"):
            tdSql.error(f"select derivative(v,{expr},0) from d1")
        for expr in ("unit_col", "unsupported", "cast(unit_ticks as bigint)", "unit_ticks+0"):
            tdSql.error(self.stream("invalid", f"derivative(v,{expr},0) as rate"))
        tdSql.error(self.stream("invalid", "derivative(v,unit_text,unit_ticks) as rate"))
        tdSql.error(self.stream("invalid", "derivative(v,unit_text,0) as rate",
                               trigger="interval(1m)", partition="partition by gid"))
        tdSql.error("select _stream_derivative_factor(unit_text,0) from d1")
        # The complete calculation must remain single-source, even when the
        # DERIVATIVE itself is inside a locally valid child SELECT.
        inner = ("select ts, derivative(v,unit_text,0) as rate from %%tbname "
                 "where ts >= _twstart and ts <= _twend")
        prefix = ("create stream invalid count_window(3) from src partition by tbname "
                  "into out_invalid as ")
        tdSql.error(prefix + f"select a.ts,a.rate,b.unit_text from ({inner}) a join d2 b on a.ts=b.ts",
                    expectErrInfo="requires a single-source calculation", fullMatched=False)
        tdSql.error(prefix + f"select ts,rate from ({inner} union all {inner}) a",
                    expectErrInfo="requires a single-source calculation", fullMatched=False)
        tdSql.execute(prefix.replace("invalid", "wrapped") + f"select ts,rate from ({inner}) a")
        literal_inner = inner.replace("derivative(v,unit_text,0)", "derivative(v,1s,0)")
        tdSql.execute(prefix.replace("invalid", "literal_join") +
                      f"select a.ts,a.rate,b.unit_text from ({literal_inner}) a join d2 b on a.ts=b.ts")
        tdSql.execute(self.stream("fixed", "derivative(v,unit_text,1) as rate",
                                 table="d2", partition="", source="%%trows"))
        tdSql.execute(self.stream("projected", "derivative(v,projected_unit,0) as rate",
                                 source="(select ts,v,unit_text as projected_unit from %%tbname)")
                      + " and projected_unit='2s'")
        tdSql.execute(self.stream("explicit", "derivative(v,unit_text,0) as rate",
                                 partition="partition by tbname,unit_text"))
        tdSql.execute("create stable empty_src(ts timestamp,v double) tags(unit_text varchar(16))")
        tdSql.execute(self.stream("empty", "derivative(v,unit_text,0) as rate",
                                 table="empty_src", source="%%trows"))
        self.wait_running(6)
        tdSql.error("alter table d2 set tag unit_text='5s'")
        tdSql.error("alter stable src drop tag unit_text")
        self.write(("d2",))
        self.wait_rows("out_fixed", 2)
        tdSql.query("select rate from out_fixed order by result_ts")
        tdSql.checkData(0, 0, 4.0)
        tdSql.checkData(1, 0, 4.0)
        self.wait_rows("out_projected", 2)
        tdSql.query("select rate from out_projected order by result_ts")
        tdSql.checkData(0, 0, 4.0)
        tdSql.checkData(1, 0, 4.0)
        self.wait_rows("out_explicit", 2)
        for name, expected in (("wrapped", 4.0), ("literal_join", 2.0)):
            self.wait_rows(f"out_{name}", 2)
            tdSql.query(f"select rate from out_{name} order by ts")
            for row in range(2):
                tdSql.checkData(row, 0, expected)
        tdSql.execute("create table late_point using empty_src tags('2s')")
        tdSql.execute(
            f"insert into late_point values ({self.base},100)"
            f"({self.base+5000},110)({self.base+10000},120)"
        )
        self.wait_rows("out_empty", 2)
        tdSql.query("select rate from out_empty order by result_ts")
        tdSql.checkData(0, 0, 4.0)
        tdSql.checkData(1, 0, 4.0)
        for stream in ("fixed", "projected", "explicit", "wrapped", "literal_join"):
            tdSql.execute(f"drop stream {stream}")
        tdSql.execute("alter stable src drop tag unit_text")
        tdSql.execute(f"drop database {self.db}")
        print("stream DERIVATIVE validation and literal regression .... [ passed ]")

    def do_pending_rows_use_current_tag(self):
        self.prepare()
        for name, source in (("native", "%%tbname"), ("cached", "%%trows")):
            tdSql.execute(self.stream(name, "unit_text as used_unit, derivative(v,unit_text,0) as rate",
                                      source=source))
        self.wait_running(2)
        self.write(("d1",), values=(100, 110))
        tdSql.execute("alter table d1 set tag unit_text='5s'")
        time.sleep(2)
        tdSql.execute(f"insert into d1 values ({self.base+10000},120,1000)")
        for name in ("native", "cached"):
            self.wait_rows(f"out_{name}", 2)
            tdSql.query(f"select used_unit,rate from out_{name} order by result_ts")
            for row in range(2):
                tdSql.checkData(row, 0, "5s")
                tdSql.checkData(row, 1, 10.0)
        tdSql.execute(f"drop database {self.db}")
        print("stream DERIVATIVE pending rows use current unit ........ [ passed ]")

    def do_window_modes_and_history(self):
        self.prepare()
        windows = (
            ("by_interval", "interval(15s)", 2, (2.0, 2.0)),
            ("by_session", "session(ts,6s)", 2, (2.0, 2.0)),
            ("by_state", "state_window(unit_col)", 2, (2.0, 2.0)),
            ("by_event", "event_window(start with v>=100 and gid=1 end with v>=200)", 3, (2.0, 2.0, 8.0)),
        )
        for name, trigger, _, _ in windows:
            tdSql.execute(self.stream(
                name, "derivative(v,unit_text,0) as rate, derivative(v,1s,0) as original_rate",
                trigger=trigger, table="d1", partition="", source="%%trows",
            ))
        self.wait_running(4)
        rows = " ".join(
            f"({self.base + offset},{value},{state})"
            for offset, value, state in ((0, 100, 1000), (5000, 110, 1000),
                                         (10000, 120, 1000), (20000, 200, 2000))
        )
        tdSql.execute(f"insert into d1 values {rows}")
        for name, _, count, expected in windows:
            self.wait_rows(f"out_{name}", count)
            tdSql.query(f"select rate, original_rate from out_{name} order by result_ts")
            for row, value in enumerate(expected):
                tdSql.checkData(row, 0, value)
                tdSql.checkData(row, 1, value)
        tdSql.execute("alter table d1 set tag unit_text='2s'")
        history = self.stream("history", "unit_text as used_unit, derivative(v,unit_text,0) as rate, "
                              "derivative(v,1s,0) as original_rate",
                              trigger="interval(15s)", table="d1", partition="", source="%%trows")
        history = history.replace("event_type(window_close)",
                                  f"event_type(window_close)|fill_history({self.base})")
        tdSql.execute(history)
        self.wait_running(5)
        self.wait_rows("out_history", 2)
        tdSql.query("select used_unit, rate, original_rate from out_history order by result_ts")
        for row in range(2):
            tdSql.checkData(row, 0, "2s")
            tdSql.checkData(row, 1, 4.0)
            tdSql.checkData(row, 2, 2.0)
        tdSql.execute(f"drop database {self.db}")
        print("stream DERIVATIVE window modes and history ............ [ passed ]")

    def do_history_then_realtime_units(self):
        self.prepare()
        # Concurrent snapshot ownership is covered with controlled reader/runner
        # replies in StreamHistoryBatchTest.DerivativeTagSnapshotSurvivesLiveUpdate.
        # SQL checks history/realtime results without racing the server's speed.
        tdSql.execute("create table out_snapshot(result_ts timestamp, used_unit varchar(64), rate double)")
        windows = 4
        rows = " ".join(
            f"({self.base + window * 15000 + point * 5000},{100 + point * 10},1000)"
            for window in range(windows) for point in range(3)
        )
        tdSql.execute(f"insert into d1 values {rows}")
        sql = self.stream(
            "snapshot", "unit_text as used_unit, derivative(v,unit_text,0) as rate",
            source="%%trows", trigger="interval(15s)", table="d1", partition="",
        ).replace("event_type(window_close)",
                  f"event_type(window_close)|low_latency_calc|fill_history({self.base})")
        tdSql.execute(sql)
        expected = windows * 2
        self.wait_rows("out_snapshot", expected)
        tdSql.query("select count(*) from out_snapshot where used_unit='1s' and rate=2.0")
        tdSql.checkData(0, 0, expected)
        # Realtime uses the new value, while all completed history keeps its snapshot.
        tdSql.execute("alter table d1 set tag unit_text='5s'")
        time.sleep(2)
        start = self.base + windows * 15000
        tdSql.execute(
            f"insert into d1 values ({start},100,1000)({start+5000},110,1000)"
            f"({start+10000},120,1000)({start+15000},100,1000)"
        )
        self.wait_rows("out_snapshot", expected + 2)
        tdSql.query(f"select used_unit,rate from out_snapshot where result_ts >= {start} order by result_ts")
        tdSql.checkRows(2)
        for row in range(2):
            tdSql.checkData(row, 0, "5s")
            tdSql.checkData(row, 1, 10.0)
        tdSql.query(f"select count(*) from out_snapshot where result_ts < {start} and used_unit='1s' and rate=2.0")
        tdSql.checkData(0, 0, expected)
        tdSql.execute(f"drop database {self.db}")
        print("stream DERIVATIVE history then realtime tag update ..... [ passed ]")

    def do_subnormal_units(self):
        self.prepare()
        tdSql.execute("alter table d1 set tag unit_text='10s'")
        for name, unit, ignore in (("tag_zero", "unit_text", 0), ("literal_zero", "10s", 0),
                                   ("tag_filter", "unit_text", 1), ("literal_filter", "10s", 1)):
            tdSql.execute(self.stream(name, f"derivative(v,{unit},{ignore}) as rate",
                                      source="%%trows", trigger="count_window(2)", table="d1", partition=""))
        self.wait_running(4)
        # SQL literals reject this subnormal at parsing. Native DOUBLE binding
        # preserves it and exercises the normal storage/stream entry points.
        tiny = math.ulp(0.0)
        stmt = tdSql.cursor._connection.statement(f"insert into {self.db}.d1 (ts,v) values(?,?)")
        try:
            for ts, value in ((self.base, tiny), (self.base + 10000, 0.0),
                              (self.base + 20000, 0.0), (self.base + 30000, tiny)):
                params = new_bind_params(2)
                params[0].timestamp(ts)
                params[1].double(value)
                stmt.bind_param(params)
                stmt.execute()
        finally:
            stmt.close()
        tdSql.query("select v from d1 order by ts")
        tdSql.checkRows(4)
        assert tdSql.getData(0, 0) == tiny
        for name in ("tag_zero", "literal_zero"):
            self.wait_rows(f"out_{name}", 2)
            tdSql.query(f"select rate from out_{name} order by result_ts")
            # Exact equality is required. Approximate float checks accept zero.
            assert tdSql.getData(0, 0) == -tiny
            assert tdSql.getData(1, 0) == tiny
        for name in ("tag_filter", "literal_filter"):
            self.wait_rows(f"out_{name}", 1)
            tdSql.query(f"select rate from out_{name}")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) == tiny
        tdSql.error("select _stream_derivative(v,'10s',0,0) from d1")
        tdSql.execute(f"drop database {self.db}")
        print("stream DERIVATIVE subnormal units and filtering ......... [ passed ]")

    # --- main ---
    def test_stream_derivative_tag(self):
        """Stream DERIVATIVE time interval from native tags

        1. Compare VARCHAR, NCHAR and integer tag units in ms/us/ns databases and both stream inputs.
        2. Check invalid-value isolation, tag refresh, single-child and projected-tag queries.
        3. Reject unsupported inputs and preserve literal DERIVATIVE NULL and negative filtering.
        4. Exercise all five supported windows and historical calculation with the current tag.
        5. Apply an updated unit consistently to samples already pending in the current window.
        6. Preserve completed history and use the updated unit for realtime calculations.
        7. Bind a subnormal DOUBLE and match literal units and negative-row filtering.

        Catalog:
            - Stream

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-11 Codex Add stream DERIVATIVE tag regression.
            - 2026-11-12 Codex Cover subnormal arithmetic and negative filtering.
        """
        self.do_types_and_updates()
        self.do_validation_and_original_path()
        self.do_window_modes_and_history()
        self.do_pending_rows_use_current_tag()
        self.do_history_then_realtime_units()
        self.do_subnormal_units()
