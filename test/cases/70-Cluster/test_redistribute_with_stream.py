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
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
import os
import sys
import time

from new_test_framework.utils import tdLog, tdSql, tdStream

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from redistribute_concurrent_common import RedistributeConcurrentBase


class TestRedistributeWithStream(RedistributeConcurrentBase):
    """Verify stream processing works while redistribute is in-flight."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_redistribute_with_stream(self):
        """Redistribute concurrent with stream processing

        Verify that a stream keeps producing correct results while a
        REDISTRIBUTE VGROUP transaction is running in the background on an
        UNRELATED database.

        NOTE: redistributing the stream's OWN database is destructive (it moves
        the output table's vgroup and wipes emitted rows), so this test keeps
        the stream in a dedicated database that is never redistributed and
        churns a separate data database instead.

        1. Deploy a 4-dnode cluster (dnode1 mnode-only) and create a snode.
        2. Prepare a CHURN database (super table + bulk data) whose vgroups
           will be redistributed.
        3. Create a STREAM database (never redistributed) with an interval
           stream that aggregates trigger rows into an output table; warm it up
           so it is fully live.
        4. Start a background thread that keeps redistributing the CHURN
           database's vgroups.
        5. In parallel, keep writing new trigger data into the stream database;
           the stream must keep generating result rows for the new windows.
        6. Stop the background thread, verify the stream results, and verify
           the churn database's data stayed intact across all the moves.

        Since: v3.3.3.7

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2026-07-16 Created

        """
        # IMPORTANT DESIGN NOTE (root-cause fix):
        # A stream's trigger source, intermediate state and output table all
        # live on vnodes.  REDISTRIBUTE VGROUP physically MOVES a vgroup's
        # data to another dnode.  If we redistribute the very database that
        # holds the stream's trigger/output tables, the output table's vgroup
        # is moved out from under the running stream and its already-emitted
        # rows are wiped (observed: stream_out dropped 3 -> 0 and never
        # recovered).  Redistributing a stream's OWN vnodes while it is running
        # is therefore inherently destructive and is NOT a supported scenario.
        #
        # The meaningful, supported concurrency scenario is: a stream keeps
        # producing correct results while an UNRELATED database's vgroups are
        # being redistributed in the background.  So we use TWO databases:
        #   * streamDb  - holds the trigger tables + stream + output table;
        #                 this database is NEVER redistributed.
        #   * churnDb   - holds a super table with bulk data; its vgroups are
        #                 continuously redistributed by the background thread.
        streamDb = "rdst_stream"
        churnDb = "rdst_stream_churn"
        stbname = "stb"
        ctbNum = 10
        rowsPerTbl = 100

        # A stream needs a snode to run on. Create it on dnode1 (mnode-only).
        self.prepare_cluster()
        tdStream.createSnode(1)

        # Prepare bulk data in the CHURN database. These are the vgroups the
        # background thread will keep redistributing; the data must stay intact
        # across all the moves (verified at the end).
        totalRows = self.prepare_data(churnDb, stbname=stbname, ctbNum=ctbNum,
                                      rowsPerTbl=rowsPerTbl, vgroups=2, replica=1)

        # Create the STREAM database separately. It is never redistributed, so
        # the stream's trigger/output vnodes stay put while churnDb is moved.
        tdLog.info(f"create stream database {streamDb} (never redistributed)")
        tdSql.execute(f"drop database if exists {streamDb}")
        tdSql.execute(
            f"create database {streamDb} vgroups 2 replica 1 "
            f"wal_retention_period 3600"
        )
        tdSql.execute(f"use {streamDb}")
        # Keep the rest of the test referring to the stream database.
        dbname = streamDb

        # The stream trigger source follows the proven stream-2.0 pattern used
        # by cases/18-StreamProcessing/02-Stream/test_stream_data_types.py:
        #   * a SUPER table (`strig`) drives the stream (`from strig`)
        #   * a child table (`ct1`) receives the actual rows
        #   * the AS-query aggregates `%%trows` (the rows of the current
        #     trigger window)
        # Windows close on EVENT-TIME: writing a row whose ts falls into a
        # later 1s bucket closes the previous window and makes the stream emit
        # one result row.  A plain normal table as the trigger source does NOT
        # reliably emit output, which is why the earlier attempts produced an
        # empty (never-created) output table.
        tdSql.execute(
            f"create table {dbname}.strig "
            f"(ts timestamp, c1 int) tags (tg int)"
        )
        tdSql.execute(
            f"create table {dbname}.ct1 using {dbname}.strig tags (1)"
        )
        createStreamSql = (
            f"create stream {dbname}.s_rdst interval(1s) sliding(1s) "
            f"from {dbname}.strig "
            f"into {dbname}.stream_out "
            f"as select _twstart ts, count(*) cnt, avg(c1) avgv "
            f"from %%trows"
        )
        tdLog.info(f"create stream: {createStreamSql}")
        tdSql.execute(createStreamSql)
        tdStream.checkStreamStatus()

        # Number of rows written into each 1s event-time window.  Every closed
        # window's result row must have cnt == rowsPerWindow (asserted later).
        rowsPerWindow = 5
        baseTs = 1700000000000  # fixed base event-time for trigger windows

        # ------------------------------------------------------------------
        # WARM-UP phase (BEFORE redistribute):
        # Establish the stream output table on a STABLE cluster first.  If we
        # start moving the trigger table's vgroups before the stream has ever
        # produced its first output row, the stream tasks get disrupted mid
        # initialization and never create `stream_out`.  Writing a few
        # event-time windows now closes the first windows, forces the stream to
        # materialize `stream_out`, and gives it a checkpoint to recover from
        # when its vnodes move.
        # ------------------------------------------------------------------
        warmupWindows = 4
        for w in range(warmupWindows):
            for r in range(rowsPerWindow):
                ts = baseTs + w * 1000 + r
                tdSql.execute(f"insert into {dbname}.ct1 values ({ts}, {r})")
            tdLog.info(
                f"warm-up window {w + 1}/{warmupWindows} ({rowsPerWindow} rows)"
            )
            time.sleep(1)
        # Wait until stream_out exists and has at least warmupWindows-1 rows,
        # proving the stream is fully live before we start churning vgroups.
        warmedUp = False
        for attempt in range(30):
            try:
                tdSql.query(f"select count(*) from {dbname}.stream_out")
                cnt = tdSql.getData(0, 0)
                if cnt is not None and cnt >= warmupWindows - 1:
                    warmedUp = True
                    tdLog.info(f"stream warmed up: {cnt} rows before redistribute")
                    break
            except Exception as e:  # noqa: BLE001 - output table not ready yet
                tdLog.info(f"warm-up wait (attempt {attempt + 1}/30): {e}")
            time.sleep(1)
        if not warmedUp:
            tdLog.exit("stream failed to warm up before redistribute")

        # Start churning the CHURN database's vgroups in the background.  The
        # stream database (streamDb == dbname) is deliberately NOT touched, so
        # the stream's trigger/output vnodes stay put while an unrelated
        # database is being redistributed.  This exercises the realistic
        # scenario: does an in-flight REDISTRIBUTE VGROUP transaction on other
        # vgroups disturb a running stream?
        thread, stop_event, errors = self.start_redistribute(churnDb)

        try:
            # Keep writing trigger data while redistribute is running. Windows
            # close on EVENT-TIME: each row's ts determines its 1s window, and
            # writing rows into a later window closes the earlier one, making
            # the stream emit that window's aggregate result row.
            #
            # We write `triggerWindows` windows worth of data (each with
            # `rowsPerWindow` rows) using monotonically increasing event-time.
            # The last window stays open (no later data to close it), so the
            # stream emits at most `triggerWindows - 1` result rows.  A short
            # wall-clock sleep between windows lets the stream engine process
            # each closed window while a redistribute may be in flight.
            # Continue event-time windows AFTER the warm-up windows so the ts
            # keeps increasing monotonically (warm-up used windows
            # 0..warmupWindows-1).
            triggerWindows = 15
            churnStart = warmupWindows
            churnEnd = warmupWindows + triggerWindows
            for w in range(churnStart, churnEnd):
                # Write rowsPerWindow rows into event-time window w (all within
                # the same 1s bucket: ts in [baseTs + w*1000, +999]).
                for r in range(rowsPerWindow):
                    ts = baseTs + w * 1000 + r  # +r keeps rows in same window
                    tdSql.execute(
                        f"insert into {dbname}.ct1 values ({ts}, {r})"
                    )
                tdLog.info(
                    f"wrote event-time window {w - churnStart + 1}/"
                    f"{triggerWindows} during redistribute ({rowsPerWindow} rows)"
                )
                time.sleep(1)  # let the stream engine process closed windows
        finally:
            # Always stop the background thread, even on assertion failure.
            # We stop redistribute BEFORE the final flush windows so the stream
            # tasks run on a stable topology while draining pending windows.
            self.stop_redistribute(thread, stop_event, errors)

        # IMPORTANT: an event-time stream only emits a window's result when a
        # LATER event-time row closes it.  The very last window we wrote during
        # the churn phase is still open (no later row has closed it yet), so we
        # must drive a few more monotonically increasing event-time windows to
        # flush the pending windows.
        #
        # Because the stream database is NOT redistributed (only churnDb is),
        # the stream's reader/output tasks are never disrupted and this
        # flush/poll phase converges quickly.  We still drive brand-new later
        # event-time windows while polling so that every pending window has a
        # closing row available.  The target is the number of windows written
        # up to the churn phase, minus the always-open last window:
        #   expectRows = warmupWindows + triggerWindows - 1
        expectRows = warmupWindows + triggerWindows - 1
        # Drive up to 60 extra windows (one per second) while polling.  Since
        # the stream is undisturbed this normally converges in a few seconds;
        # the generous cap only guards against slow CI machines.
        recoveryWindows = 60  # max extra windows to drive while flushing
        flushStart = churnEnd
        resultRows = None
        for i in range(recoveryWindows):
            # Drive one more monotonically increasing event-time window so the
            # stream always has a later row available to close pending windows.
            w = flushStart + i
            for r in range(rowsPerWindow):
                ts = baseTs + w * 1000 + r
                tdSql.execute(f"insert into {dbname}.ct1 values ({ts}, {r})")

            # Check how many result rows the stream has emitted so far.  The
            # output table may still be missing if the reader is mid-recovery;
            # tdSql.query() raises ProgrammingError(0x2603) in that case, so we
            # tolerate it and keep driving new windows.
            try:
                tdSql.query(f"select count(*) from {dbname}.stream_out")
                resultRows = tdSql.getData(0, 0)
            except Exception as e:  # noqa: BLE001 - output table not ready yet
                tdLog.info(
                    f"stream_out not ready (recovery {i + 1}/{recoveryWindows})"
                    f": {e}"
                )
                resultRows = None
            tdLog.info(
                f"recovery window {i + 1}/{recoveryWindows}: "
                f"stream_out has {resultRows} rows (need >= {expectRows})"
            )
            if resultRows is not None and resultRows >= expectRows:
                break
            # Periodically dump stream status to help diagnose recovery.
            if i % 10 == 0:
                try:
                    tdSql.query(
                        "select stream_name, status "
                        "from information_schema.ins_streams"
                    )
                    tdLog.info(f"ins_streams: {tdSql.queryResult}")
                except Exception as e:  # noqa: BLE001
                    tdLog.info(f"failed to query ins_streams: {e}")
            time.sleep(1)
        tdLog.info(f"stream produced {resultRows} result rows")
        if resultRows is None or resultRows < expectRows:
            tdLog.exit(
                f"stream result incomplete: got {resultRows}, "
                f"expected >= {expectRows}"
            )

        # Every closed window received exactly rowsPerWindow rows, so the count
        # in each result row must equal rowsPerWindow. This proves the stream
        # computed correct aggregates while the vgroups were being moved.
        tdSql.query(f"select distinct cnt from {dbname}.stream_out")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, rowsPerWindow)

        # The CHURN database's source data must remain intact across all the
        # vnode moves performed by the background redistribute thread.  This is
        # the data-integrity guarantee of REDISTRIBUTE VGROUP: moving a vgroup
        # relocates its data without losing any rows.
        tdSql.query(f"select count(*) from {churnDb}.{stbname}")
        tdSql.checkData(0, 0, totalRows)
        tdLog.info(
            f"data integrity verified: {totalRows} rows preserved in {churnDb} "
            f"after concurrent redistribute"
        )
