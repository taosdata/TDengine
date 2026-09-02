import time

from new_test_framework.utils import clusterComCheck, sc, tdLog, tdSql, tdStream


class TestManualRecalcReliability:
    VIEW_SQL = (
        "select recalc_id, `start`, `end`, request_time, progress, status, message "
        "from information_schema.ins_stream_recalculates "
        "where stream_name='s_reliable'"
    )

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def _rows(sql):
        tdSql.query(sql, queryTimes=1)
        return [
            tuple(tdSql.getData(row, col) for col in range(tdSql.queryCols))
            for row in range(tdSql.getRows())
        ]

    @classmethod
    def _wait_row(cls, recalc_id=None, terminal=False, timeout=120):
        deadline = time.monotonic() + timeout
        last = []
        while time.monotonic() < deadline:
            last = cls._rows(cls.VIEW_SQL)
            matched = (
                last if recalc_id is None else [row for row in last if row[0] == recalc_id]
            )
            if len(matched) == 1:
                row = matched[0]
                if terminal is None:
                    return row
                if terminal and row[5] in ("Finished", "Failed"):
                    return row
                if not terminal and row[5] in ("Pending", "Running"):
                    return row
            time.sleep(0.5)
        raise AssertionError(
            f"recalculation row did not reach expected state: {last!r}"
        )

    def test_manual_recalc_survives_redeployment(self):
        """An accepted manual recalculation survives dnode redeployment.

        1. Accept a long-running manual recalculation and capture its identity.
        2. Restart the dnode while the request is Pending or Running.
        3. Verify identity, range, and request time survive and reach Finished.
        4. Verify realtime calculation continues after manual recovery.

        Catalog: Streams:Recalculation

        Since: v3.4.0.0

        Labels: stream,recalc,reliability,restart,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7045278024

        History:
            - 2026-08-21 OpenAI GPT-5.6 Codex Created
        """
        tdStream.createSnode(1)
        tdSql.executes(
            [
                "drop database if exists recalc_reliable",
                "create database recalc_reliable vgroups 1 buffer 8",
                "create stable recalc_reliable.events "
                "(ts timestamp, value int) tags(site int)",
            ]
        )
        for batch_start in range(0, 120000, 5000):
            values = "".join(
                f"({1735689600000 + row * 1000},{row})"
                for row in range(batch_start, batch_start + 5000)
            )
            tdSql.execute(
                "insert into recalc_reliable.site_1 "
                "using recalc_reliable.events tags(1) values" + values
            )
        tdSql.execute(
            "create stream recalc_reliable.s_reliable interval(1m) sliding(1m) "
            "from recalc_reliable.events partition by tbname "
            "into recalc_reliable.result "
            "OUTPUT_SUBTABLE(CONCAT('result_', tbname)) "
            "(wstart, total) tags(source varchar(128) as tbname) "
            "as select _twstart, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.execute(
            "recalculate stream recalc_reliable.s_reliable "
            "from '2025-01-01 00:00:00' to '2025-01-02 09:20:00'"
        )
        before = self._wait_row()
        recalc_id = before[0]
        request_time = before[3]
        if request_time is None:
            raise AssertionError(f"accepted job has NULL request_time: {before!r}")

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdStream.checkStreamStatus()

        after = self._wait_row(recalc_id=recalc_id, terminal=None)
        if after[1:4] != before[1:4]:
            raise AssertionError(
                f"identity/range/request_time changed after restart: "
                f"before={before!r}, after={after!r}"
            )
        terminal = self._wait_row(recalc_id=recalc_id, terminal=True)
        if terminal[5] != "Finished" or terminal[4] != "100%":
            raise AssertionError(f"recalculation did not finish: {terminal!r}")
        if terminal[6] is not None:
            raise AssertionError(
                f"Finished retained an error message: {terminal!r}"
            )

        rows = self._rows("select count(*) from recalc_reliable.result")
        baseline_count = rows[0][0]
        tdSql.execute("insert into recalc_reliable.site_1 values(now,1)(now+2m,2)")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            rows = self._rows("select count(*) from recalc_reliable.result")
            if rows and rows[0][0] > baseline_count:
                return
            time.sleep(0.5)
        raise AssertionError("realtime calculation stopped after manual recovery")
