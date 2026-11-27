# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import time
from pathlib import Path
from tempfile import TemporaryDirectory

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestSubqueryTimeAlign:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.ensureSnode(1)

    def setup_method(self):
        self.streams = []

    def teardown_method(self):
        for stream in reversed(self.streams):
            tdSql.execute(f"drop stream if exists {stream}")

    def _prepare(self, db):
        tdSql.prepare(dbname=db, drop=True, vgroups=1, precision="ms")
        tdSql.execute("create table temperatures (ts timestamp, v int)")
        tdSql.execute("create table pulses (ts timestamp, v int)")
        tdSql.execute(
            "create vtable sensor (ts timestamp, temperature int from temperatures.v, "
            "heartbeat int from pulses.v)"
        )
        self.base = (int(time.time()) // 60 - 2) * 60000
        tdSql.execute(f"insert into temperatures values ({self.base + 8000}, 80)")

    def _start(self, stream, source, expr, options="", trigger="sensor"):
        tdSql.execute(
            f"create stream {stream} interval(10s) from {trigger} partition by tbname "
            f"{options} into {stream}_out (ws, n, n_temp, avg_temp) as "
            f"select _twstart, count(heartbeat), count({expr}), avg({expr}) from {source} "
            "where ts >= _twstart and ts <= _twend"
        )
        self.streams.append(stream)
        tdStream.checkStreamStatus(stream)

    def _pulses(self):
        tdSql.execute(
            f"insert into pulses values ({self.base + 12000}, 1) "
            f"({self.base + 16000}, 2) ({self.base + 21000}, 3)"
        )

    def _check(self, stream, avg_temp):
        ws = self.base + 10000
        tdSql.checkResultsByFunc(
            sql=f"select cast(ws as bigint), n, n_temp, avg_temp from {stream}_out where ws = {ws}",
            func=lambda: list(tdSql.queryResult) == [(ws, 2, 0 if avg_temp is None else 2, avg_temp)],
            retry=60,
        )

    def test_window_external_predecessor(self):
        """Read a temperature preceding the calculation window from a named source.

        A temperature of 80 at second 8 supplies the two heartbeat rows in the
        window from second 10 to second 20. A third heartbeat closes that window.

        Catalog:
            - Streams:Subquery:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("align_named_calc")
        self._start("named_calc", "table(time_align(table sensor, true, 'temperature:PREV:_p;heartbeat:PREV:_p'))", "temperature_p")
        self._pulses()
        self._check("named_calc", 80)

    def test_nonleaf_virtual_calculation(self):
        """Calculate aligned temperatures through an inherited virtual super table.

        Start from an ordinary local trigger and point the calculation at the
        parent. Its leaf supplies the temperature preceding the first heartbeat.

        Catalog:
            - Streams:Subquery:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        self._prepare("align_nonleaf_calc")
        tdSql.execute("create stable parent_vst (ts timestamp, temperature int, heartbeat int) tags(site int) virtual 1")
        tdSql.execute("create stable leaf_vst (ts timestamp, auxiliary int) tags(zone int) "
                      "base on align_nonleaf_calc.parent_vst virtual 1")
        tdSql.execute("create vtable leaf_input (temperature from temperatures.v, heartbeat from pulses.v) "
                      "using leaf_vst tags(1, 2)")
        source = "table(time_align(table parent_vst, true, 'temperature:PREV:_p'))"
        self._start("nonleaf_calc", source, "temperature_p")
        tdSql.execute("create stable empty_leaf (ts timestamp, other int) tags(zone int) "
                      "base on align_nonleaf_calc.parent_vst virtual 1")
        self._start("multi_leaf_calc", source, "temperature_p")
        tdSql.execute("create view calc_view as select ts, temperature, heartbeat from sensor")
        tdSql.error("create stream view_calc interval(10s) from sensor into view_out as "
                    "select _twstart as ws, avg(temperature_p) as t "
                    "from table(time_align(table calc_view, true, 'temperature:PREV:_p'))",
                    expectedErrno=0x410E)
        self._pulses()
        self._check("nonleaf_calc", 80)
        self._check("multi_leaf_calc", 80)

    def test_nonleaf_partition_calculation(self):
        """Create an aligned parent placeholder and check child binding on the leaf trigger.

        The parent stream must reach Running. Two children of the leaf receive the
        same heartbeat and produce distinct temperatures through the same calculation.

        Catalog:
            - Streams:Subquery:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-23 OpenAI Created
        """
        self._prepare("align_nonleaf_partition")
        tdSql.execute("create stable parent_vst (ts timestamp, temperature int, heartbeat int) tags(site int) virtual 1")
        tdSql.execute("create table cold_values (ts timestamp, v int)")
        tdSql.execute(f"insert into cold_values values ({self.base + 8000}, 30)")
        tdSql.execute("create stable leaf_vst (ts timestamp, auxiliary int) tags(zone int) "
                      "base on align_nonleaf_partition.parent_vst virtual 1")
        for child, source in (("warm", "temperatures"), ("cold", "cold_values")):
            tdSql.execute(f"create vtable {child} (temperature from {source}.v, heartbeat from pulses.v) "
                          "using leaf_vst tags(1, 2)")
        for trigger in ("parent_vst", "leaf_vst"):
            stream = f"{trigger}_calc"
            tdSql.execute(
                f"create stream {stream} interval(10s) from {trigger} partition by tbname "
                f"into {stream}_out (ws, n, avg_temp) tags(origin_name varchar(64) as tbname) as "
                "select _twstart, count(heartbeat), avg(temperature_p) "
                "from table(time_align(table %%tbname, true, 'temperature:PREV:_p')) "
                "where ts >= _twstart and ts <= _twend"
            )
            self.streams.append(stream)
            tdStream.checkStreamStatus(stream)
        self._pulses()
        tdSql.checkResultsByFunc(
            sql=f"select origin_name, n, avg_temp from leaf_vst_calc_out "
                f"where ws = {self.base + 10000} order by origin_name",
            func=lambda: list(tdSql.queryResult) == [("cold", 2, 30), ("warm", 2, 80)],
            retry=60,
        )

    def test_tbname_does_not_inherit_alignment(self):
        """Compare the raw and explicitly aligned partitions over the same window.

        Both triggers admit the two heartbeats using the aligned temperature.
        The raw calculation sees two missing temperatures. An explicit PREV
        on the same partition sees the preceding 80 at both timestamps.

        Catalog:
            - Streams:Subquery:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("align_partition_calc")
        trigger = "table(time_align(table sensor, true, 'temperature:PREV:_p'))"
        options = "stream_options(pre_filter(temperature_p = 80))"
        self._start("raw_partition", "%%tbname", "temperature", options, trigger)
        self._start(
            "aligned_partition", "table(time_align(table %%tbname, true, 'temperature:PREV:_p'))",
            "temperature_p", options, trigger,
        )
        self._pulses()
        self._check("raw_partition", None)
        self._check("aligned_partition", 80)

    def test_trigger_filter_is_not_calc_where(self):
        """Calculate both input rows when the trigger accepts one row in the window.

        Heartbeats at seconds 12 and 16 have values 1 and 2. The trigger accepts
        only the latter. The explicit calculation counts both and averages 80.

        Catalog:
            - Streams:Subquery:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("align_filtered_calc")
        self._start(
            "filtered_calc",
            "table(time_align(table sensor, true, 'temperature:PREV:_p;heartbeat:PREV:_p'))",
            "temperature_p",
            "stream_options(pre_filter(heartbeat >= 2))",
        )
        self._pulses()
        self._check("filtered_calc", 80)

    def test_runner_snapshot_during_rebind(self):
        """Rebind a virtual column while the Runner evaluates an aligned window.

        1. Pause evaluation when the Runner has read the first window's temperature.
        2. Change the reference and let that window finish on its original source.
        3. Close a second window and verify it reads the replacement temperature.

        Catalog:
            - Streams:Subquery:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._prepare("align_runner_snapshot")
        tdSql.execute("create table replacement (ts timestamp, v int)")
        tdSql.execute(f"insert into replacement values ({self.base + 8000}, 30)")
        with TemporaryDirectory(prefix="align_snapshot_") as directory:
            gate = Path(directory)
            function = gate.name
            script = gate / "gate.py"
            script.write_text(f"""from pathlib import Path
import time

def init():
    pass

def process(block):
    gate = Path({str(gate)!r})
    rows, _ = block.shape()
    if any(block.data(i, 1) == 1 for i in range(rows)):
        (gate / "entered").touch()
        limit = time.monotonic() + 60
        while not (gate / "release").exists():
            if time.monotonic() >= limit:
                raise RuntimeError("timed out waiting for virtual column rebind")
            time.sleep(0.05)
    return [block.data(i, 0) for i in range(rows)]

def destroy():
    pass
""", encoding="utf-8")
            tdSql.execute(f"create function {function} as '{script}' outputtype int language 'python'")
            try:
                self._start("snap_calc", "table(time_align(table %%tbname, true, 'temperature:PREV:_p'))",
                            f"{function}(temperature_p, heartbeat)")
                self._pulses()
                limit = time.monotonic() + 60
                while not (gate / "entered").exists():
                    assert time.monotonic() < limit, "Runner did not reach the aligned window"
                    time.sleep(0.05)
                tdSql.execute("alter vtable sensor alter column temperature set replacement.v")
                (gate / "release").touch()
                self._check("snap_calc", 80)
                tdSql.execute(f"insert into pulses values ({self.base + 26000}, 4) ({self.base + 31000}, 5)")
                ws = self.base + 20000
                tdSql.checkResultsByFunc(
                    sql=f"select cast(ws as bigint), n, n_temp, avg_temp from snap_calc_out where ws={ws}",
                    func=lambda: list(tdSql.queryResult) == [(ws, 2, 2, 30)],
                    retry=60,
                )
            finally:
                (gate / "release").touch()
                tdSql.execute("drop stream if exists snap_calc")
                if "snap_calc" in self.streams:
                    self.streams.remove("snap_calc")
                tdSql.execute(f"drop function {function}")
