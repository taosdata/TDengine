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
from new_test_framework.utils import (tdLog, tdSql, tdStream, StreamCheckItem,)


class TestStreamVtableRebindRedeploy:
    """Verify runtime vtable column rebind interleaved with stream DROP/CREATE
    does not wedge Period streams in a `Stream vtable calculate need redeploy`
    retry loop (defect 7138880370).

    Production failure (IDMP, 3.4.3.1 enterprise): after rebinding ~29 vtable
    columns at runtime with several rounds of stream DROP/CREATE, Period
    streams on the vst go Failed (`Internal error` / `Retry needed`) and the
    same QID keeps failing in `virtualTableScanCheckNeedRedeploy` forever.
    Restarting taosd and STOP/START do not help; only dropping every stream on
    the vst and waiting ~2 minutes recovers it.

    This case reproduces the production pattern:

    - a Period stream coexists with a CALC_NOTIFY_ONLY stream on the same vst;
    - four rounds of virtual-column rebind across source vgroups, each
      interleaved with DROP/CREATE of the Period stream.

    After each round the stream must stay Running and the latest window must
    aggregate the rebound source table's rows, which proves the redeploy chain
    picked up the new vgroup. If the bug is present the stream leaves Running
    and the check dumps `show streams` diagnostics before failing.
    """

    precision = 'ms'
    period = '10s'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_vtable_rebind_redeploy(self):
        """Stream: vtable column rebind + stream drop/create interleaving

        Covers:
          1. Period stream on a 3-child virtual super table with a coexisting
             CALC_NOTIFY_ONLY stream on the same vst
          2. Four rounds of runtime column rebind across source vgroups,
             interleaved with DROP/CREATE of the Period stream
          3. After each round the stream must stay Running and the latest
             window must read the rebound source table's rows
             (bug symptom: stream leaves Running -> redeploy loop)
          4. Recovery: drop every stream on the vst, wait, recreate -> Running

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.1

        Labels: common

        Jira: 7138880370

        History:
            - 2026-10-19 Created

        """
        tdStream.ensureSnode()
        tdSql.execute("alter all dnodes 'debugflag 131';")
        tdSql.execute("alter all dnodes 'stdebugflag 131';")

        streams = [self.RebindInterleave()]
        tdStream.checkAll(streams)

    # ================================================================== #
    # rebind + drop/create interleaving scenario
    # ================================================================== #
    class RebindInterleave(StreamCheckItem):
        def __init__(self):
            self.db = "sdb_rb"
            self.refdb = ["rb_ref1", "rb_ref2", "rb_ref3"]
            self.reftb = "t"
            self.trig = "trig"
            self.vstb = "vstb"
            self.vcs = ["vc1", "vc2", "vc3"]
            self.period_stream = "s_period"
            self.notify_stream = "s_notify"
            self.outtb = "res_period"
            # Each round: (child table, column, new source db). The rebind moves
            # the column to a source table on a different vgroup so the executor
            # must redeploy a calc reader for the new vgroup.
            self.rebind_plan = [
                ("vc1", "c1", "rb_ref2"),
                ("vc2", "c2", "rb_ref3"),
                ("vc3", "c3", "rb_ref1"),
                ("vc1", "c1", "rb_ref3"),
            ]

        # -------------------------------------------------------------- #
        # helpers
        # -------------------------------------------------------------- #
        def _create_period_stream(self):
            # period trigger has no interval window: _twstart is not allowed,
            # use the local trigger time as the timestamp column instead.
            # A period stream applies no implicit window filter to the query
            # table, so every trigger re-aggregates the whole source table.
            # The checks below therefore assert cumulative row counts per
            # source table, which still reveal which table each column is
            # currently bound to after a rebind.
            tdSql.execute(
                f"create stream {self.db}.{self.period_stream} "
                f"period({TestStreamVtableRebindRedeploy.period}) "
                f"from {self.trig} into {self.outtb} "
                f"as select cast(_tlocaltime / 1000000 as timestamp) ws, "
                f"count(c1) n1, count(c2) n2, count(c3) n3 "
                f"from {self.vstb};"
            )

        def _insert_round_data(self):
            # one trigger row + ref1/ref2/ref3 get 1/2/3 rows in the current window.
            # Distinct timestamps per row: vtable child tables project rows by
            # their ts primary key, so same-ts rows collapse into one.
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"insert into {self.trig} values (now, 1);")
            tdSql.execute(f"insert into {self.refdb[0]}.{self.reftb} values (now, 101);")
            tdSql.execute(f"insert into {self.refdb[1]}.{self.reftb} values (now, 201) (now + 1s, 202);")
            tdSql.execute(
                f"insert into {self.refdb[2]}.{self.reftb} "
                f"values (now, 301) (now + 1s, 302) (now + 2s, 303);"
            )

        def _stream_status(self):
            tdSql.query(
                f"select status from information_schema.ins_streams "
                f"where db_name='{self.db}' and stream_name='{self.period_stream}'"
            )
            if tdSql.getRows() != 1:
                return "None"
            return tdSql.getData(0, 0)

        def _wait_stream_gone(self, timeout=30):
            for _ in range(timeout):
                tdSql.query(
                    f"select count(*) from information_schema.ins_streams "
                    f"where db_name='{self.db}' and stream_name='{self.period_stream}'"
                )
                if tdSql.getData(0, 0) == 0:
                    return
                time.sleep(1)
            tdLog.exit(f"stream:{self.db}.{self.period_stream} still exists {timeout}s after drop")

        def _dump_stream_state(self, why):
            tdLog.info(f"[diagnose] {why}")
            tdSql.query("show streams")
            tdSql.printResult("show streams")

        def _wait_running(self, timeout=30):
            for _ in range(timeout):
                if self._stream_status() == "Running":
                    return
                time.sleep(1)
            self._dump_stream_state(f"stream:{self.db}.{self.period_stream} not Running after recreate")
            tdLog.exit(f"stream:{self.db}.{self.period_stream} not Running within {timeout}s")

        def _rebind_round(self, round_idx):
            vc, col, new_ref = self.rebind_plan[round_idx]
            tdSql.execute(f"use {self.db}")
            tdLog.info(f"round {round_idx}: rebind {self.db}.{vc}.{col} -> {new_ref}.{self.reftb}.cint")
            tdSql.execute(
                f"alter vtable {self.db}.{vc} alter column {col} set {new_ref}.{self.reftb}.cint"
            )
            # interleave stream DROP/CREATE, the production trigger pattern
            tdSql.execute(f"drop stream {self.db}.{self.period_stream}")
            self._wait_stream_gone()
            self._create_period_stream()
            # The fresh stream only processes rows written after it reaches
            # Running (fill_history is off), so wait for Running before writing
            # the next round of data.
            self._wait_running()
            self._insert_round_data()

        def _check_window(self, exp_n1, exp_n2, exp_n3, retry=45):
            # fail fast when the stream lands in a terminal failed state: that is
            # the redeploy loop symptom. Intermediate states (Idle/None/...) keep
            # waiting because a fresh CREATE STREAM takes a moment to go Running.
            for _ in range(retry):
                if self._stream_status() in ("Failed", "Stopped"):
                    self._dump_stream_state(
                        f"stream:{self.db}.{self.period_stream} left Running state"
                    )
                    tdLog.exit(
                        f"stream:{self.db}.{self.period_stream} not Running -> "
                        f"need redeploy loop reproduced"
                    )
                tdSql.query(
                    f"select ws, n1, n2, n3 from {self.db}.{self.outtb} "
                    f"order by ws desc limit 1"
                )
                if (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 1, exp_n1)
                    and tdSql.compareData(0, 2, exp_n2)
                    and tdSql.compareData(0, 3, exp_n3)
                ):
                    return
                time.sleep(1)
            self._dump_stream_state(
                f"window check timeout for {self.db}.{self.period_stream}"
            )
            tdSql.query(
                f"select ws, n1, n2, n3 from {self.db}.{self.outtb} "
                f"order by ws desc limit 1"
            )
            tdSql.printResult("last window")
            tdLog.exit(
                f"window check failed for {self.db}.{self.period_stream}, "
                f"expected n1={exp_n1}, n2={exp_n2}, n3={exp_n3}"
            )

        # -------------------------------------------------------------- #
        # lifecycle
        # -------------------------------------------------------------- #
        def create(self):
            p = TestStreamVtableRebindRedeploy.precision
            # drop leftovers from a previous run so every execution starts clean
            for db in [self.db] + self.refdb:
                tdSql.execute(f"drop database if exists {db}")
                tdSql.execute(
                    f"create database {db} vgroups 1 buffer 8 precision '{p}'"
                )
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.trig} (ts timestamp, cint int)")
            for ref in self.refdb:
                tdSql.execute(
                    f"create table if not exists {ref}.{self.reftb} (ts timestamp, cint int)"
                )
            tdSql.execute(
                f"create stable if not exists {self.vstb} "
                f"(ts timestamp, c1 int, c2 int, c3 int) tags (t1 int) virtual 1"
            )
            # three virtual children, each bound to a source table on its own vgroup
            tdSql.execute(
                f"create vtable if not exists {self.vcs[0]} "
                f"(c1 from {self.refdb[0]}.{self.reftb}.cint) using {self.vstb} tags (1)"
            )
            tdSql.execute(
                f"create vtable if not exists {self.vcs[1]} "
                f"(c2 from {self.refdb[1]}.{self.reftb}.cint) using {self.vstb} tags (2)"
            )
            tdSql.execute(
                f"create vtable if not exists {self.vcs[2]} "
                f"(c3 from {self.refdb[2]}.{self.reftb}.cint) using {self.vstb} tags (3)"
            )
            # notify-only stream coexisting on the same vst (production pattern).
            # The output is never written, but the first select column must
            # still be a timestamp for stream validation.
            tdSql.execute(
                f"create stream {self.notify_stream} "
                f"period({TestStreamVtableRebindRedeploy.period}) "
                f"from {self.trig} stream_options(calc_notify_only) "
                f"as select cast(_tlocaltime / 1000000 as timestamp) ws, "
                f"count(c1) from {self.vstb};"
            )
            # Period stream under test
            self._create_period_stream()

        def insert1(self):
            # baseline: c1->ref1, c2->ref2, c3->ref3.
            # The period stream re-aggregates the whole source table on every
            # trigger, so the expected values are cumulative row counts:
            # ref1=1, ref2=2, ref3=3.
            self._insert_round_data()

        def check1(self):
            self._check_window(1, 2, 3)

        def insert2(self):
            # c1 rebind ref1 -> ref2. Cumulative: ref1=2, ref2=4, ref3=6.
            # n1 must become 4 (reads ref2) instead of 2 (still on ref1).
            self._rebind_round(0)

        def check2(self):
            self._check_window(4, 4, 6)

        def insert3(self):
            # c2 rebind ref2 -> ref3. Cumulative: ref1=3, ref2=6, ref3=9.
            # n2 must become 9 (reads ref3) instead of 6 (still on ref2).
            self._rebind_round(1)

        def check3(self):
            self._check_window(6, 9, 9)

        def insert4(self):
            # c3 rebind ref3 -> ref1. Cumulative: ref1=4, ref2=8, ref3=12.
            # n3 must become 4 (reads ref1) instead of 12 (still on ref3).
            self._rebind_round(2)

        def check4(self):
            self._check_window(8, 12, 4)

        def insert5(self):
            # c1 rebind ref2 -> ref3. Cumulative: ref1=5, ref2=10, ref3=15.
            # n1 must become 15 (reads ref3) instead of 10 (still on ref2).
            self._rebind_round(3)

        def check5(self):
            self._check_window(15, 15, 5)

        def insert6(self):
            # recovery path from the defect: drop EVERY stream on the vst and
            # wait ~2 minutes for the stuck vtable calc task state to clear,
            # then recreate the Period stream
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"drop stream {self.period_stream}")
            tdSql.execute(f"drop stream {self.notify_stream}")
            self._wait_stream_gone(timeout=30)
            tdLog.info("wait 120s for the stuck vtable calc state to clear (production recovery)")
            time.sleep(120)
            self._create_period_stream()
            self._wait_running()
            self._insert_round_data()

        def check6(self):
            # current bindings: c1->ref3, c2->ref3, c3->ref1.
            # Cumulative: ref1=6, ref2=12, ref3=18 -> n1=18, n2=18, n3=6.
            self._check_window(18, 18, 6, retry=60)
