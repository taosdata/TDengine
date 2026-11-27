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
        tdStream.ensureSnode(1)
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

    def test_manual_recalc_without_end_finishes(self):
        """Open-ended state-window recalculation finishes after writing history.

        1. Create ordinary and virtual-table streams after inserting old data.
        2. Advance realtime windows and confirm history has not been calculated.
        3. Recalculate with and without TO and wait for Finished at 100%.
        4. Verify both request forms produce the same historical aggregates.

        Catalog: Streams:Recalculation

        Since: v3.4.0.0

        Labels: stream,recalc,reliability,virtual-table,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7045278024

        History:
            - 2026-11-05 OpenAI Created
        """
        tdStream.ensureSnode(1)
        db = "recalc_without_end"
        tdSql.executes(
            [
                f"drop database if exists {db}",
                f"create database {db} vgroups 1",
                f"create table {db}.raw (ts timestamp, state_value int, v int)",
                f"create vtable {db}.vt (ts timestamp, "
                f"state_value int from {db}.raw.state_value, v int from {db}.raw.v)",
            ]
        )
        old = int(time.time() * 1000) - 86400000
        values = "".join(
            f"({old + i * 1000},{state},{(i + 1) * 10})"
            for i, state in enumerate((0, 0, 1, 1, 0))
        )
        tdSql.execute(f"insert into {db}.raw values {values}")
        streams = [f"{src}_{kind}" for src in ("raw", "vt") for kind in ("open", "bound")]
        for name in streams:
            src = name.split("_")[0]
            tdSql.execute(
                f"create stream {db}.{name} state_window(state_value) extend(1) "
                f"from {db}.{src} stream_options(ignore_disorder) "
                f"into {db}.out_{name} nodelay_create_subtable "
                f"as select _twstart as ts, count(*) as cnt, sum(v) as total "
                f"from {db}.{src} where ts >= _twstart and ts <= _twend"
            )
        tdStream.checkStreamStatus()
        recent = int(time.time() * 1000) + 1000
        values = "".join(
            f"({recent + i * 1000},{i // 2 % 2},{100 + i})" for i in range(6)
        )
        tdSql.execute(f"insert into {db}.raw values {values}")
        for name in streams:
            tdSql.checkResultsByFunc(
                f"select count(*) from {db}.out_{name}",
                lambda: tdSql.getData(0, 0) == 2,
            )
            assert self._rows(
                f"select count(*) from {db}.out_{name} where ts < {recent}"
            ) == [(0,)]

        for name in streams:
            end = f" to {recent + 4000}" if name.endswith("bound") else ""
            tdSql.execute(f"recalculate stream {db}.{name} from {old}{end}")
        names = ",".join(f"'{name}'" for name in streams)
        view_sql = (
            "select stream_name, progress, status, message "
            "from information_schema.ins_stream_recalculates "
            f"where stream_name in ({names})"
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            rows = self._rows(view_sql)
            assert all(row[2] != "Failed" for row in rows), rows
            if len(rows) == len(streams) and all(row[2] == "Finished" for row in rows):
                break
            time.sleep(0.5)
        else:
            raise AssertionError(f"recalculations did not finish: {rows!r}")
        assert all(row[1] == "100%" and row[3] is None for row in rows), rows
        for name in streams:
            rows = self._rows(
                f"select cnt, total from {db}.out_{name} "
                f"where ts <= {old + 2000} order by ts"
            )
            assert rows == [(2, 30), (2, 70)], (name, rows)
