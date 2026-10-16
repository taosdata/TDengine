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
"""Test: run metadata (DDL) operations while ``SPLIT VGROUP`` is in flight.

Scenario
--------
1. generate data on a single-vgroup database
2. start a background worker that repeatedly issues metadata DDL
   (``create stable`` / ``create table``), timestamping every attempt
3. trigger ``split vgroup`` on the main thread, recording the window
   boundaries (``split_start`` / ``split_end``)
4. keep ONLY the attempts that fall completely inside the split window and
   report, per operation type, whether they succeeded or were rejected

Why the window filter matters
-----------------------------
The worker starts before the split is issued and may complete one more round
after the split transaction is gone. Those out-of-window attempts naturally
succeed, so mixing them into the statistics would let the test pass even if
*every* DDL issued during the actual split had failed. Filtering on the
recorded window is what makes the assertions meaningful.

The recorded outcomes feed the final test report ("which operations can /
cannot run during split").
"""

import time
import threading

from new_test_framework.utils import tdLog, tdSql, etool
from split_concurrent_util import new_connection, get_vgroups, split_first_vgroup


class TestSplitWithMeta:
    # database used for the split; kept to a single vgroup so the split target
    # is deterministic.
    dbname = "split_meta"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        # Drop the database first so re-runs are idempotent: taosBenchmark -d
        # does NOT rebuild an existing database (dbinfo.drop defaults to "no"),
        # so a leftover db would keep the multi-vgroup state from a previous
        # split and break the single-vgroup premise, and the bare
        # `create stable {dbname}.stb` below would fail on a leftover table.
        tdSql.execute(f"drop database if exists {cls.dbname}")
        # Use taosBenchmark to create the database with a SINGLE vgroup and a
        # sizeable amount of data (stable "meters", 20 child tables, 5000 rows
        # each) so the split has real work to do and overlaps our worker.
        etool.benchMark(command=f"-d {cls.dbname} -t 20 -n 5000 -v 1 -y")
        # base super table used by the background worker to spawn child tables
        tdSql.execute(
            f"create stable {cls.dbname}.stb "
            f"(ts timestamp, c1 int, c2 float) tags (t1 int)"
        )
        tdLog.info(f"[meta] initial vgroups={get_vgroups(cls.dbname)}")

    # ------------------------------------------------------------------
    # background DDL worker
    # ------------------------------------------------------------------
    def _ddl_worker(self, results, stop_evt):
        """Keep issuing metadata DDL until ``stop_evt`` is set.

        Every attempt is recorded as a tuple
        ``(op, idx, ok, err, t_start, t_end)`` where ``t_start`` / ``t_end``
        are ``time.time()`` values taken immediately before and after the
        statement. The main thread uses those timestamps to keep ONLY the
        attempts that fell entirely inside the split window -- without them,
        DDL issued before the split started or after it finished would be
        mixed into the statistics and mask a real regression.

        Uses its own connection because it runs on a separate thread.

        The connection/cursor lifecycle is wrapped in try/finally: if
        ``new_connection()`` or ``conn.cursor()`` raises (e.g. the cluster is
        unreachable), the thread must not die silently with the connection
        left open. A "worker_setup" marker record is appended instead, and the
        main thread fails with the precise cause.
        """
        conn = None
        cur = None
        idx = 0
        try:
            conn = new_connection()
            cur = conn.cursor()
            while not stop_evt.is_set():
                idx += 1
                sql = (
                    f"create table {self.dbname}.ctb_dyn_{idx} "
                    f"using {self.dbname}.stb tags({idx})"
                )
                t0 = time.time()
                try:
                    cur.execute(sql)
                    results.append(("create_child_table", idx, True, "", t0, time.time()))
                    tdLog.info(f"[meta-worker] OK   #{idx} create child table")
                except Exception as e:  # noqa: BLE001 - we want the raw message
                    results.append(
                        ("create_child_table", idx, False, str(e), t0, time.time())
                    )
                    tdLog.info(f"[meta-worker] FAIL #{idx} create child table: {e}")

                # also try a heavier meta op: create/drop a fresh stable
                sql2 = (
                    f"create stable {self.dbname}.stb_dyn_{idx} "
                    f"(ts timestamp, v int) tags(g int)"
                )
                t0 = time.time()
                try:
                    cur.execute(sql2)
                    results.append(("create_stable", idx, True, "", t0, time.time()))
                    tdLog.info(f"[meta-worker] OK   #{idx} create stable")
                except Exception as e:  # noqa: BLE001
                    results.append(("create_stable", idx, False, str(e), t0, time.time()))
                    tdLog.info(f"[meta-worker] FAIL #{idx} create stable: {e}")

                time.sleep(0.2)
        except Exception as e:  # noqa: BLE001
            # Connection/cursor setup failed (or an unexpected loop error):
            # the thread must not die silently -- otherwise the main thread
            # only sees an empty "results" and fails the generic
            # "no DDL attempt inside the split window" assertion with no clue
            # that the root cause was the connection. The marker record uses
            # t=0 (never inside the split window) so the window statistics
            # below stay meaningful.
            results.append(("worker_setup", idx, False, str(e), 0, 0))
            tdLog.info(f"[meta-worker] worker aborted: {e}")
        finally:
            # close() must run even when the loop is left via an exception,
            # otherwise an already-open connection leaks. close() is guarded
            # so a close failure cannot mask the original error.
            if cur is not None:
                try:
                    cur.close()
                except Exception as e:  # noqa: BLE001
                    tdLog.info(f"[meta-worker] cursor close failed: {e}")
            if conn is not None:
                try:
                    conn.close()
                except Exception as e:  # noqa: BLE001
                    tdLog.info(f"[meta-worker] connection close failed: {e}")
            tdLog.info(f"[meta-worker] stopped after {idx} rounds")

    def test_split_with_meta(self):
        """SPLIT VGROUP 期间执行元数据操作

        验证在 split vgroup 事务进行过程中，创建子表 / 创建超级表等元数据
        DDL 操作是否能够正常执行。

        Since: v3.4.0.0

        Labels: cluster,split,meta,functional

        Jira: None

        History:
            - 2026-06-13 Created
        """
        results = []
        stop_evt = threading.Event()
        worker = threading.Thread(
            target=self._ddl_worker, args=(results, stop_evt), daemon=True
        )

        tdLog.info("[meta] start background DDL worker")
        worker.start()
        # give the worker a moment to warm up so some DDL overlaps the split
        time.sleep(1)

        # ------------------------------------------------------------------
        # main thread performs the split and blocks until it finishes.
        # split_start / split_end bracket the split window; only DDL attempts
        # that fall completely inside this window are meaningful for the
        # "can it run during split?" question.
        # ------------------------------------------------------------------
        try:
            split_start = time.time()
            vgid, elapsed = split_first_vgroup(self.dbname)
            split_end = time.time()
            tdLog.info(
                f"[meta] split of vgroup {vgid} took {elapsed:.1f}s "
                f"(window {split_start:.3f} .. {split_end:.3f})"
            )
        finally:
            # Always stop the worker, even if split_first_vgroup raised (e.g.
            # SystemExit when wait_transactions_clear times out). Otherwise the
            # daemon worker would keep creating ctb_dyn_*/stb_dyn_* tables and
            # hold its connection for the rest of the pytest process. join()
            # with a timeout keeps this from hanging forever; a still-alive
            # daemon thread cannot be killed, only logged.
            stop_evt.set()
            worker.join(timeout=30)
            if worker.is_alive():
                tdLog.info(
                    "[meta] worker thread still alive after join timeout; "
                    "it keeps running until the pytest process exits"
                )

        # If the worker could not even establish its connection, the only
        # record is the "worker_setup" marker (timestamps 0,0 so it never
        # falls inside the split window) and every in-window assertion below
        # would fail with a misleading message. Report the precise cause
        # instead.
        setup_failures = [r for r in results if r[0] == "worker_setup"]
        if setup_failures:
            raise AssertionError(
                "background DDL worker could not run: "
                f"{setup_failures[0][3]}"
            )

        # Keep only the attempts fully contained in the split window. An
        # attempt that started before the split was issued, or finished after
        # the split transaction was already gone, tells us nothing about
        # concurrent behavior.
        in_window = [
            r for r in results if r[4] >= split_start and r[5] <= split_end
        ]
        outside = len(results) - len(in_window)

        total = len(in_window)
        ok = sum(1 for r in in_window if r[2])
        failed = total - ok
        tdLog.info(
            f"[meta] DDL attempts inside split window: total={total}, "
            f"ok={ok}, failed={failed} "
            f"(discarded {outside} attempt(s) outside the window)"
        )
        # dump everything, flagging whether each attempt was inside the window,
        # so the report can be built from precise data
        for r in results:
            scope = "IN " if (r[4] >= split_start and r[5] <= split_end) else "OUT"
            tdLog.info(
                f"[meta] result: scope={scope} op={r[0]} idx={r[1]} "
                f"ok={r[2]} err={r[3]}"
            )

        # split per operation type: child-table creation is a vnode-side op and
        # is expected to mostly succeed, while create-stable is an mnode
        # transaction that conflicts with the split transaction.
        for op in ("create_child_table", "create_stable"):
            sub = [r for r in in_window if r[0] == op]
            sub_ok = sum(1 for r in sub if r[2])
            tdLog.info(
                f"[meta] in-window breakdown: op={op} "
                f"total={len(sub)} ok={sub_ok} failed={len(sub) - sub_ok}"
            )

        # We must have actually exercised DDL *during* the split, otherwise the
        # test proves nothing and should fail loudly instead of passing on
        # attempts made outside the window.
        assert total > 0, (
            "no DDL attempt fell inside the split window; the split likely "
            "finished too fast -- increase the seed data volume"
        )

        # Child-table creation is vnode-side and should keep working during the
        # split (apart from transient 0x0503 while the vnode is migrating).
        child = [r for r in in_window if r[0] == "create_child_table"]
        child_ok = sum(1 for r in child if r[2])
        assert child_ok > 0, (
            f"every create-child-table attempt failed during the split "
            f"({len(child)} attempts), expected vnode-side DDL to keep working"
        )

        # sanity: the split really produced more vgroups
        final_vgroups = get_vgroups(self.dbname)
        assert len(final_vgroups) >= 2, (
            f"split did not increase vgroup count, got {final_vgroups}"
        )
        tdLog.info("[meta] test_split_with_meta PASSED")

    def teardown_class(cls):
        """Best-effort cleanup so a failed run does not poison the next one.

        The background DDL worker creates ctb_dyn_<n> child tables and
        stb_dyn_<n> super tables (the latter are never dropped by the worker)
        every 0.2s, and the split turns the single-vgroup db into a
        multi-vgroup one. Drop the whole database so neither the dynamic
        tables nor the multi-vgroup state leak into the next run / session.
        """
        try:
            tdSql.execute(f"drop database if exists {cls.dbname} force")
        except Exception as e:  # noqa: BLE001 - teardown must not mask failures
            tdLog.info(f"[meta-teardown] drop database {cls.dbname} failed: {e}")
        tdLog.success(f"{__file__} successfully executed")
