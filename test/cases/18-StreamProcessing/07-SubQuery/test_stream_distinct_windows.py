# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import time

from taos.tmq import Consumer

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamDistinctWindows:
    BASE = 1735689600000

    @classmethod
    def setup_class(cls):
        tdLog.info("=== setup: stream DISTINCT window databases ===")
        tdSql.execute("drop database if exists sdwin_src")
        tdSql.execute("drop database if exists sdwin_out")
        tdStream.ensureSnode(1)
        tdSql.execute("create database sdwin_src wal_retention_period 3600 keep 36500")
        tdSql.execute("create database sdwin_out keep 36500")
        tdSql.execute("create table sdwin_src.t (ts timestamp, v int, state_key int, start_flag int, end_flag int)")
        tdSql.execute("create table sdwin_src.calc (ts timestamp, v varchar(32), state_key int, start_flag int, end_flag int)")
        tdSql.execute("create table sdwin_src.calc_trigger (ts timestamp, v int)")
        tdSql.execute("create table sdwin_out.subscription_result (ts timestamp, cnt bigint, total bigint)")

    @classmethod
    def teardown_class(cls):
        tdSql.execute("drop topic if exists sdwin_topic")
        tdSql.execute("drop topic if exists sdwin_direct")
        for name in ("ses", "state", "event", "count", "calc_ses", "calc_state", "calc_event"):
            tdSql.execute(f"drop stream if exists sdwin_src.s_{name}")
        tdSql.execute("drop database if exists sdwin_src")
        tdSql.execute("drop database if exists sdwin_out")

    def wait_result(self, name, expected, timeout=120, columns="cnt, total, plain", exact=False):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            tdSql.query(f"select {columns} from sdwin_out.r_{name} order by ts")
            last = tdSql.queryResult
            enough_rows = len(last) == len(expected) if exact else len(last) >= len(expected)
            if enough_rows and last[:len(expected)] == expected:
                return
            time.sleep(1)
        raise AssertionError(f"{name}: expected {expected}, got {last}")

    def do_stream_windows(self):
        windows = {
            "ses": "session(ts, 2s)",
            "state": "state_window(state_key)",
            "event": "event_window(start with start_flag = 1 end with end_flag = 1)",
        }
        for name, window in windows.items():
            tdSql.execute(
                f"create stream sdwin_src.s_{name} {window} from sdwin_src.t "
                f"into sdwin_out.r_{name} as select _twstart as ts, "
                "count(distinct v) as cnt, sum(distinct v) as total, "
                "count(v) as plain, avg(distinct v) as avg_distinct "
                "from %%trows"
            )
            tdStream.checkStreamStatus(f"s_{name}")

        b = self.BASE
        tdSql.execute(
            "insert into sdwin_src.t values "
            f"({b}, 1, 1, 1, 0),({b+1000}, 1, 1, 0, 0),"
            f"({b+1500}, NULL, 1, 0, 0),"
            f"({b+2000}, 2, 1, 0, 1),({b+10000}, 1, 2, 1, 0),"
            f"({b+11000}, 1, 2, 0, 0),({b+12000}, 3, 2, 0, 1),"
            f"({b+20000}, 9, 3, 0, 0)"
        )
        for name in windows:
            self.wait_result(name, [(2, 3, 3), (2, 4, 3)])
            tdSql.query(
                f"select avg_distinct from sdwin_out.r_{name} order by ts limit 2"
            )
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1.5)
            tdSql.checkData(1, 0, 2.0)
        print("stream window DISTINCT ......................... [ passed ]")

    def do_topic_projection(self):
        for window in (
            "session(ts, 2s)",
            "state_window(state_key)",
            "event_window start with start_flag = 1 end with end_flag = 1",
            "count_window(3)",
            "external_window((select ts, ts + 2s from sdwin_src.t) w)",
        ):
            tdSql.query(
                "select count(distinct v) "
                f"from sdwin_src.t {window}"
            )
            tdSql.error(
                "create topic sdwin_bad as select count(distinct v) "
                f"from sdwin_src.t {window}"
            )
        tdSql.execute("create topic sdwin_topic as select ts, cnt, total from sdwin_out.subscription_result")
        consumer = Consumer({
            "group.id": "sdwin_distinct_projection",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "experimental.snapshot.enable": "true",
        })
        try:
            consumer.subscribe(["sdwin_topic"])
            b = self.BASE
            tdSql.execute(
                "insert into sdwin_src.t values "
                f"({b+30000}, 4, 4, 1, 0),({b+31000}, 4, 4, 0, 0),"
                f"({b+32000}, 5, 4, 0, 1),({b+40000}, 9, 5, 0, 0)"
            )
            self.wait_result("ses", [(2, 3, 3), (2, 4, 3), (1, 9, 1), (2, 9, 3)])
            tdSql.execute(
                "insert into sdwin_out.subscription_result "
                "select ts, cnt, total from sdwin_out.r_ses where cnt = 2 and total = 9"
            )
            tdSql.query("select cnt, total from sdwin_out.subscription_result")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(0, 1, 9)
            rows = []
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and not any((r[1], r[2]) == (2, 9) for r in rows):
                msg = consumer.poll(1)
                if msg is None:
                    continue
                assert msg.error() is None, msg.error()
                value = msg.value()
                if value:
                    for block in value:
                        rows.extend(block.fetchall())
            assert any((row[1], row[2]) == (2, 9) for row in rows), rows
        finally:
            consumer.close()
            tdSql.execute("drop topic if exists sdwin_topic")
        print("topic projection of DISTINCT result ........... [ passed ]")

    def do_stream_target_snapshot(self):
        for name in ("ses", "state", "event", "count"):
            tdSql.execute(
                f"create topic sdwin_direct as select ts, cnt, total, plain, avg_distinct "
                f"from sdwin_out.r_{name}"
            )
            consumer = Consumer({
                "group.id": f"sdwin_snapshot_{name}",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "auto.offset.reset": "earliest",
                "experimental.snapshot.enable": "true",
            })
            try:
                consumer.subscribe(["sdwin_direct"])
                values = set()
                deadline = time.monotonic() + 30
                expected = {(2, 3, 3, 1.5), (2, 4, 3, 2.0)}
                while time.monotonic() < deadline and not expected.issubset(values):
                    msg = consumer.poll(1)
                    if msg is None:
                        continue
                    assert msg.error() is None, msg.error()
                    blocks = msg.value()
                    if blocks:
                        for block in blocks:
                            values.update(tuple(row[1:5]) for row in block.fetchall())
                assert expected.issubset(values), (name, values)
            finally:
                consumer.close()
                tdSql.execute("drop topic if exists sdwin_direct")
        print("four stream targets snapshot subscription ... [ passed ]")

    def do_count_window_stream(self):
        tdSql.execute(
            "create stream sdwin_src.s_count count_window(3) from sdwin_src.t "
            "into sdwin_out.r_count as select _twstart as ts, "
            "count(distinct v) as cnt, sum(distinct v) as total, "
            "count(v) as plain, avg(distinct v) as avg_distinct "
            "from %%trows"
        )
        tdStream.checkStreamStatus("s_count")
        b = self.BASE + 100000
        tdSql.execute(
            "insert into sdwin_src.t values "
            f"({b}, 1, 1, 1, 0),({b+1000}, 1, 1, 0, 0),"
            f"({b+2000}, 2, 1, 0, 1),({b+3000}, 1, 2, 1, 0),"
            f"({b+4000}, 1, 2, 0, 0),({b+5000}, 3, 2, 0, 1)"
        )
        self.wait_result("count", [(2, 3, 3), (2, 4, 3)])
        tdSql.query("select avg_distinct from sdwin_out.r_count order by ts limit 2")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1.5)
        tdSql.checkData(1, 0, 2.0)
        print("stream COUNT window DISTINCT .................. [ passed ]")

    def do_reused_calculation_windows(self):
        windows = {
            "calc_ses": "session(ts, 1s)",
            "calc_state": "state_window(state_key)",
            "calc_event": "event_window start with start_flag = 1 end with end_flag = 1",
        }
        for name, window in windows.items():
            tdSql.execute(
                f"create stream sdwin_src.s_{name} interval(30s) sliding(30s) "
                "from sdwin_src.calc_trigger "
                f"into sdwin_out.r_{name} as select _wstart as ts, "
                "count(distinct v) as cnt, count(v) as plain "
                "from sdwin_src.calc where ts >= _twstart and ts < _twend "
                f"{window}"
            )
            tdStream.checkStreamStatus(f"s_{name}")

        base = self.BASE + 300000
        for round_idx in range(2):
            distinct_values = 2100 + round_idx * 100
            for window_idx in range(2):
                window_start = base + round_idx * 30000 + window_idx * 10000
                state_key = round_idx * 2 + window_idx
                rows = []
                for value in range(distinct_values):
                    key = f"v{value:031d}"
                    for copy in range(2):
                        row_idx = value * 2 + copy
                        rows.append(
                            f"({window_start + row_idx}, '{key}', {state_key}, "
                            f"{int(row_idx == 0)}, {int(row_idx == distinct_values * 2 - 1)})"
                        )
                        if len(rows) == 200:
                            tdSql.execute("insert into sdwin_src.calc values " + ",".join(rows))
                            rows.clear()
                if rows:
                    tdSql.execute("insert into sdwin_src.calc values " + ",".join(rows))

            # The next interval boundary closes this trigger and forces reuse of
            # the same calculation operator for the following round.
            tdSql.execute(
                "insert into sdwin_src.calc_trigger values "
                f"({base + round_idx * 30000 + 25000}, {round_idx}),"
                f"({base + (round_idx + 1) * 30000}, {round_idx})"
            )
            expected = [(2100, 4200)] * 2
            if round_idx == 1:
                expected += [(2200, 4400)] * 2
            for name in windows:
                self.wait_result(name, expected, columns="cnt, plain", exact=True)
        print("reused calculation windows DISTINCT .......... [ passed ]")

    def test_stream_distinct_windows_and_topic(self):
        """DISTINCT results survive stream windows and TMQ projection.

        1. Check SESSION, STATE and EVENT stream trigger results.
        2. Check topic restrictions and consume materialized stream results.
        3. Read all four stream targets through topic snapshots.

        Catalog:
            - Stream:Window:Distinct

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-04 Codex Added stream and subscription acceptance.
        """
        self.do_stream_windows()
        self.do_topic_projection()
        self.do_count_window_stream()
        self.do_stream_target_snapshot()

    def test_reused_calculation_windows(self):
        """DISTINCT stays correct after stream calculation window resets.

        1. Run two stream calculations with SESSION, STATE and EVENT windows inside the calculation SQL.
        2. Compare each result after inserting more than 64 KiB of distinct values per window.

        Catalog:
            - Stream:Window:Distinct

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-09 Codex Cover calculation window reuse with large DISTINCT sets.
        """
        self.do_reused_calculation_windows()
