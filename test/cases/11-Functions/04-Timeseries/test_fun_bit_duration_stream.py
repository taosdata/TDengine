import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestBitDurationStream:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.ensureSnode()

    @pytest.mark.parametrize(
        "source,history",
        [("normal", False), ("child", False), ("partition", False), ("virtual", False), ("virtual", True)],
    )
    @pytest.mark.parametrize("bounded", [False, True])
    def test_bit_duration_timeline(self, source, history, bounded):
        """Preserve the ascending timeline in stream aggregation.

        1. Aggregate BOOL changes from normal, child and virtual tables.
        2. Verify unbounded queries retain whole-table aggregation semantics.
        3. Verify explicit window bounds retain window-local duration semantics.
        4. Replay virtual-table history with and without window bounds.

        Catalog:
            - Functions/TimeSeries/Stream

        Since: v3.4.3.10

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7162179830

        History:
            - 2026-11-09 Codex Added stream timeline regression
        """
        db = f"bd_timeline_{source}_{int(history)}_{int(bounded)}"
        tdSql.execute(f"create database {db} vgroups 1 buffer 8")
        tdSql.execute(f"use {db}")
        tdSql.execute("create table src (ts timestamp, r bool)")
        tdSql.execute("create stable st (ts timestamp, r bool) tags (id int)")
        tdSql.execute("create table c1 using st tags (1)")
        tdSql.execute("create stable vst (ts timestamp, r bool) tags (id int) virtual 1")
        tdSql.execute(f"create vtable v1 (r from {db}.src.r) using vst tags (1)")
        trigger = {"normal": "src", "child": "c1", "partition": "st", "virtual": "vst"}[source]
        partition = "partition by tbname" if source in ("partition", "virtual") else ""
        calc = "%%tbname" if partition else trigger
        where = "where ts >= _twstart and ts < _twend" if bounded else ""
        physical = "c1" if source in ("child", "partition") else "src"
        insert = (
            f"insert into {physical} values "
            "('2024-01-01 00:00:00.000',true)('2024-01-01 00:00:01.000',false)"
            "('2024-01-01 00:00:03.000',true)('2024-01-01 00:00:04.000',true)"
        )
        options = "max_delay(3s)"
        if history:
            tdSql.execute(insert)
            options += " | fill_history('2024-01-01 00:00:00.000')"
        tdSql.execute(
            f"create stream bd interval(2s) sliding(2s) from {trigger} {partition} "
            f"stream_options({options}) into result as "
            "select _twstart as w, floor(bit_duration(r,1,0)/1000.0) as on_s, "
            "bit_duration(r,1,1) as off_ms, bit_duration(r,1,2) as span_ms, count(r) as n "
            f"from {calc} {where}"
        )
        tdStream.checkStreamStatus()
        if not history:
            tdSql.execute(insert)
        tdSql.checkResultsByFunc(
            "select on_s, off_ms, span_ms, n from result where w = '2024-01-01 00:00:00.000'",
            lambda: tdSql.getRows() == 1,
            retry=30,
        )
        expected = (1, 1000, 1000, 2) if bounded else (2, 2000, 4000, 4)
        for column, value in enumerate(expected):
            tdSql.checkData(0, column, value)
        tdSql.execute("drop stream bd")
        tdSql.execute(f"drop database {db}")
